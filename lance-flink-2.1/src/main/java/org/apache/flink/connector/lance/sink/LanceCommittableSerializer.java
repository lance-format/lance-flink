/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.FragmentMetadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link LanceCommittable} 的序列化器。
 *
 * <p>编码格式（version=3）：
 * <pre>
 *   [datasetPath:utf]
 *   [writeMode:byte]                                  // 0 = APPEND, 1 = OVERWRITE
 *   [schemaJsonLen:int][schemaJson:bytes]             // Arrow Schema 的 JSON 表示（可能为空）
 *   [fragmentCount:int][fragment...]                  // 显式字段编码，见 {@link FragmentMetadataCodec}
 * </pre>
 *
 * <p>相比 version=2 的变化（issue #88 / M5）：fragment 段从「一次性 Java 原生序列化整个
 * 列表」改为 {@link FragmentMetadataCodec} 的显式字段编码，移除 {@code ObjectOutputStream}
 * 写入路径，消除原生反序列化的安全面与 serialVersionUID 脆弱性。
 *
 * <p>相比 version=1 的变化：删除了 {@code createDataset:bool}（Writer 侧的决策，B1 修复后由
 * Committer 承担），新增 {@code writeMode:byte} 让 Committer 知道作业声明的写入语义。
 *
 * <p><b>向后兼容（R1 + #88）</b>：{@link #deserialize(int, byte[])} 保留 version=1 与 version=2
 * 的读路径。v1 把老格式的 {@code createDataset:bool} 映射为 {@link LanceOptions.WriteMode#OVERWRITE}
 * （true）或 {@link LanceOptions.WriteMode#APPEND}（false）——这是 B1 修复前的隐含语义：
 * createDataset=true 的 committable 走 Overwrite 建表、其它走 Append。v1/v2 的 fragment 段仍是
 * Java 原生序列化 blob，因此读路径保留 {@code ObjectInputStream}（仅用于历史兼容，新数据不再写
 * 该格式）。这样升级的作业可以**无缝读取**早期 alpha 构建里持久化在 checkpoint 中的 v1/v2 committable。
 *
 * <p>Arrow Schema 没有 {@code implements Serializable}，但提供了 {@code toJson()} 与
 * {@code Schema.fromJSON(String)} 的往返接口；使用 JSON 文本既简单又稳定。
 *
 * <p>{@link FragmentMetadata} 及其嵌套类型虽然实现 {@code Serializable}，但这里不走 Java
 * 原生序列化，而用 {@link FragmentMetadataCodec} 的显式字段编码。
 */
public final class LanceCommittableSerializer
        implements SimpleVersionedSerializer<LanceCommittable> {

    public static final LanceCommittableSerializer INSTANCE = new LanceCommittableSerializer();

    private static final int VERSION = 3;

    /** 仍支持读取的历史版本号（写入端只用 {@link #VERSION}）。 */
    private static final int VERSION_V1 = 1;

    /** version=2：fragment 段仍走 Java 原生序列化（M5 修复前的写入格式）。 */
    private static final int VERSION_V2 = 2;

    private static final byte WRITE_MODE_APPEND = 0;
    private static final byte WRITE_MODE_OVERWRITE = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LanceCommittable c) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(c.getDatasetPath());
            out.writeByte(encodeWriteMode(c.getWriteMode()));

            Schema schema = c.getArrowSchema();
            byte[] schemaJson =
                    schema == null
                            ? new byte[0]
                            : schema.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(schemaJson.length);
            out.write(schemaJson);

            FragmentMetadataCodec.writeFragments(out, c.getFragments());
        }
        return bos.toByteArray();
    }

    @Override
    public LanceCommittable deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION:
                return deserializeV3(serialized);
            case VERSION_V2:
                return deserializeV2(serialized);
            case VERSION_V1:
                return deserializeV1(serialized);
            default:
                throw new IOException(
                        "Unsupported LanceCommittable serializer version: "
                                + version
                                + " (supported: "
                                + VERSION_V1
                                + ", "
                                + VERSION_V2
                                + ", "
                                + VERSION
                                + ")");
        }
    }

    private LanceCommittable deserializeV3(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            LanceOptions.WriteMode writeMode = decodeWriteMode(in.readByte());

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema =
                        Schema.fromJSON(
                                new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            List<FragmentMetadata> fragments = FragmentMetadataCodec.readFragments(in);

            return new LanceCommittable(path, schema, writeMode, fragments);
        }
    }

    @SuppressWarnings("unchecked")
    private LanceCommittable deserializeV2(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            LanceOptions.WriteMode writeMode = decodeWriteMode(in.readByte());

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema =
                        Schema.fromJSON(
                                new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            int count = in.readInt();
            int blobLen = in.readInt();
            byte[] blob = new byte[blobLen];
            in.readFully(blob);
            List<FragmentMetadata> fragments =
                    count == 0 ? new ArrayList<>() : (List<FragmentMetadata>) javaDeserialize(blob);

            return new LanceCommittable(path, schema, writeMode, fragments);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FragmentMetadata list", e);
        }
    }

    /**
     * 读取 version=1 的历史格式（B1 修复前写入的 committable）：
     * <pre>
     *   [datasetPath:utf]
     *   [createDataset:bool]                              // v1 特有
     *   [schemaJsonLen:int][schemaJson:bytes]
     *   [fragmentCount:int][fragmentBlobLen:int][fragmentBlob:bytes]
     * </pre>
     * 映射规则：{@code createDataset=true → WriteMode.OVERWRITE}；否则 APPEND。
     * 这符合 v1 的隐含语义：createDataset=true 的 committable 由 Committer 走 Overwrite 建表。
     */
    @SuppressWarnings("unchecked")
    private LanceCommittable deserializeV1(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            boolean createDataset = in.readBoolean();
            LanceOptions.WriteMode writeMode =
                    createDataset
                            ? LanceOptions.WriteMode.OVERWRITE
                            : LanceOptions.WriteMode.APPEND;

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema =
                        Schema.fromJSON(
                                new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            int count = in.readInt();
            int blobLen = in.readInt();
            byte[] blob = new byte[blobLen];
            in.readFully(blob);
            List<FragmentMetadata> fragments =
                    count == 0 ? new ArrayList<>() : (List<FragmentMetadata>) javaDeserialize(blob);

            return new LanceCommittable(path, schema, writeMode, fragments);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FragmentMetadata list (v1)", e);
        }
    }

    private static byte encodeWriteMode(LanceOptions.WriteMode mode) {
        if (mode == LanceOptions.WriteMode.OVERWRITE) {
            return WRITE_MODE_OVERWRITE;
        }
        return WRITE_MODE_APPEND;
    }

    private static LanceOptions.WriteMode decodeWriteMode(byte b) throws IOException {
        switch (b) {
            case WRITE_MODE_APPEND:
                return LanceOptions.WriteMode.APPEND;
            case WRITE_MODE_OVERWRITE:
                return LanceOptions.WriteMode.OVERWRITE;
            default:
                throw new IOException("Unknown write mode byte: " + b);
        }
    }

    private static Object javaDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }
}

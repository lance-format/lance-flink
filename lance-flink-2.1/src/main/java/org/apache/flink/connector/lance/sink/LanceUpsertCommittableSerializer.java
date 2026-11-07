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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link LanceUpsertCommittable} 的序列化器。
 *
 * <p>编码格式（version=1）：
 * <pre>
 *   [datasetPath:utf]
 *   [writeMode:byte]                                  // 0 = APPEND, 1 = OVERWRITE
 *   [primaryKeyCount:int][primaryKey...:utf]
 *   [schemaJsonLen:int][schemaJson:bytes]             // Arrow Schema 的 JSON 表示（可能为空）
 *   [upsertBatchLen:int][upsertBatch:bytes]           // Arrow IPC stream 字节（+I/+U 行）
 *   [deleteBatchLen:int][deleteBatch:bytes]           // Arrow IPC stream 字节（-D 行）
 * </pre>
 *
 * <p>两批行数据是 Arrow IPC stream 字节（见 {@link ArrowIpcCodec}），不是 Java 原生序列化，
 * 延续 #88 移除 {@code ObjectOutputStream} 的安全策略。Arrow Schema 走 JSON 文本往返。
 */
public final class LanceUpsertCommittableSerializer
        implements SimpleVersionedSerializer<LanceUpsertCommittable> {

    public static final LanceUpsertCommittableSerializer INSTANCE =
            new LanceUpsertCommittableSerializer();

    private static final int VERSION = 1;

    private static final byte WRITE_MODE_APPEND = 0;
    private static final byte WRITE_MODE_OVERWRITE = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LanceUpsertCommittable c) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(c.getDatasetPath());
            out.writeByte(encodeWriteMode(c.getWriteMode()));

            out.writeInt(c.getPrimaryKeys().size());
            for (String pk : c.getPrimaryKeys()) {
                out.writeUTF(pk);
            }

            Schema schema = c.getArrowSchema();
            byte[] schemaJson =
                    schema == null
                            ? new byte[0]
                            : schema.toJson().getBytes(StandardCharsets.UTF_8);
            out.writeInt(schemaJson.length);
            out.write(schemaJson);

            writeBytes(out, c.getUpsertBatch());
            writeBytes(out, c.getDeleteBatch());
        }
        return bos.toByteArray();
    }

    @Override
    public LanceUpsertCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported LanceUpsertCommittable serializer version: "
                            + version
                            + " (supported: "
                            + VERSION
                            + ")");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            LanceOptions.WriteMode writeMode = decodeWriteMode(in.readByte());

            int pkCount = in.readInt();
            List<String> primaryKeys = new ArrayList<>(pkCount);
            for (int i = 0; i < pkCount; i++) {
                primaryKeys.add(in.readUTF());
            }

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema = Schema.fromJSON(new String(schemaJson, StandardCharsets.UTF_8));
            }

            byte[] upsertBatch = readBytes(in);
            byte[] deleteBatch = readBytes(in);

            return new LanceUpsertCommittable(
                    path, schema, primaryKeys, writeMode, upsertBatch, deleteBatch);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes == null ? 0 : bytes.length);
        if (bytes != null && bytes.length > 0) {
            out.write(bytes);
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len <= 0) {
            return new byte[0];
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
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
}

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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.FragmentMetadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link LanceCommittable} 的序列化器。
 *
 * <p>编码格式（version=1）：
 * <pre>
 *   [datasetPath:utf]
 *   [createDataset:bool]
 *   [schemaJsonLen:int][schemaJson:bytes]       // Arrow Schema 的 JSON 表示（可能为空）
 *   [fragmentCount:int][fragmentBlobLen:int][fragmentBlob:bytes]  // 一次性 Java 序列化整个 FragmentMetadata 列表
 * </pre>
 *
 * <p>Arrow Schema 没有 {@code implements Serializable}，但提供了 {@code toJson()} 与
 * {@code Schema.fromJSON(String)} 的往返接口；使用 JSON 文本既简单又稳定。
 *
 * <p>{@link FragmentMetadata} 本身实现 {@code Serializable}，因此直接走 Java 原生序列化。
 */
public final class LanceCommittableSerializer
        implements SimpleVersionedSerializer<LanceCommittable> {

    public static final LanceCommittableSerializer INSTANCE = new LanceCommittableSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LanceCommittable c) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(c.getDatasetPath());
            out.writeBoolean(c.isCreateDataset());

            Schema schema = c.getArrowSchema();
            byte[] schemaJson =
                    schema == null ? new byte[0] : schema.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(schemaJson.length);
            out.write(schemaJson);

            List<FragmentMetadata> fragments = c.getFragments();
            out.writeInt(fragments.size());
            byte[] fragmentBlob = javaSerialize(new ArrayList<>(fragments));
            out.writeInt(fragmentBlob.length);
            out.write(fragmentBlob);
        }
        return bos.toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public LanceCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported LanceCommittable serializer version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            boolean createDataset = in.readBoolean();

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema = Schema.fromJSON(new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            int count = in.readInt();
            int blobLen = in.readInt();
            byte[] blob = new byte[blobLen];
            in.readFully(blob);
            List<FragmentMetadata> fragments =
                    count == 0 ? new ArrayList<>() : (List<FragmentMetadata>) javaDeserialize(blob);

            return new LanceCommittable(path, schema, createDataset, fragments);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FragmentMetadata list", e);
        }
    }

    private static byte[] javaSerialize(Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(512);
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
        }
        return bos.toByteArray();
    }

    private static Object javaDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }
}

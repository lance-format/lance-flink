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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

/**
 * Arrow IPC stream 编解码工具：把 {@link VectorSchemaRoot} 序列化为字节数组。
 *
 * <p>upsert 路径与 append 路径的根本差异：{@code Dataset.mergeInsert} 消费的是<b>行数据</b>
 * （{@code ArrowArrayStream}），而非 fragment 元数据。因此 upsert 的 committable 必须跨
 * checkpoint / 网络携带序列化后的行数据，本类用 Arrow IPC stream 格式完成这一序列化，
 * 避免 Java 原生序列化（#88 已移除的安全面）。
 *
 * <p>反序列化（byte[] → VectorSchemaRoot）由调用方内联 {@link org.apache.arrow.vector.ipc
 * .ArrowStreamReader} 完成，因为 reader 返回的 root 生命周期与 reader 绑定——必须在 reader
 * 打开期间同步消费完（{@link ArrowArrayStreams#mergeInsert} 恰好是同步的）。
 */
public final class ArrowIpcCodec {

    private ArrowIpcCodec() {
        // utility class
    }

    /**
     * 把单个 batch 的 {@link VectorSchemaRoot} 序列化为 Arrow IPC stream 字节。
     *
     * @param root 已填充数据的 root（须有正确 rowCount）
     * @return Arrow IPC stream 字节（含 schema 头 + 单个 record batch + EOS）
     */
    public static byte[] serialize(VectorSchemaRoot root) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer =
                new ArrowStreamWriter(root, null, Channels.newChannel(bos))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return bos.toByteArray();
    }
}

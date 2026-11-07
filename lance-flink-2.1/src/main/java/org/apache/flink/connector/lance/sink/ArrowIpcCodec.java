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
 * Arrow IPC stream codec: serializes a {@link VectorSchemaRoot} into a byte array.
 *
 * <p>The fundamental difference between the upsert path and the append path: {@code
 * Dataset.mergeInsert} consumes <b>row data</b> ({@code ArrowArrayStream}) rather than fragment
 * metadata. The upsert committable therefore has to carry serialized row data across checkpoint /
 * network, and this class performs that serialization using the Arrow IPC stream format, avoiding
 * Java native serialization (the attack surface removed by #88).
 *
 * <p>Deserialization (byte[] → VectorSchemaRoot) is inlined by the caller via
 * {@link org.apache.arrow.vector.ipc.ArrowStreamReader}, because the root returned by the reader
 * is bound to the reader's lifetime and must be fully consumed while the reader is open
 * ({@link ArrowArrayStreams#mergeInsert} happens to be synchronous).
 */
public final class ArrowIpcCodec {

    private ArrowIpcCodec() {
        // utility class
    }

    /**
     * Serializes a single batch's {@link VectorSchemaRoot} into Arrow IPC stream bytes.
     *
     * @param root a root populated with data (must have the correct rowCount)
     * @return Arrow IPC stream bytes (schema header + single record batch + EOS)
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

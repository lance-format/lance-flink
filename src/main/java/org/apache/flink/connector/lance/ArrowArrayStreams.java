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

package org.apache.flink.connector.lance;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import org.lance.Dataset;
import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;

import java.io.IOException;

/**
 * Bridges a single {@link VectorSchemaRoot} to the Arrow C Data Interface
 * ({@link ArrowArrayStream}) required by {@link Dataset#mergeInsert}.
 *
 * <p>The Arrow Java SDK does not expose a public {@code VectorSchemaRoot -> ArrowArrayStream}
 * adapter, so this class implements the minimal {@link ArrowReader} that yields exactly one
 * record batch and exports it via {@link Data#exportArrayStream}. The exported stream lazily
 * consumes the reader (whose lifecycle is owned by the stream's release callback); the record
 * batch itself is closed by the caller here once the merge completes.
 */
public final class ArrowArrayStreams {

    private ArrowArrayStreams() {
        // utility class
    }

    /**
     * Execute a native merge-insert (upsert) of a single batch.
     *
     * @param dataset target Lance dataset
     * @param params  merge-insert semantics (on keys, matched/unmatched behavior)
     * @param allocator Arrow allocator (must stay open for the duration of the call)
     * @param root    populated data batch
     * @return the merge-insert result
     */
    public static MergeInsertResult mergeInsert(
            Dataset dataset,
            MergeInsertParams params,
            BufferAllocator allocator,
            VectorSchemaRoot root) throws IOException {
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        ArrowReader reader = new SingleBatchReader(allocator, root.getSchema(), batch);
        ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
        try {
            Data.exportArrayStream(allocator, reader, stream);
            return dataset.mergeInsert(params, stream);
        } finally {
            // Closing the stream triggers its release callback, which closes the reader
            // (and therefore the reader-owned VectorSchemaRoot).
            stream.close();
            batch.close();
        }
    }

    /**
     * An {@link ArrowReader} that yields exactly one record batch and then reports end-of-stream.
     */
    private static final class SingleBatchReader extends ArrowReader {

        private final Schema schema;
        private final ArrowRecordBatch batch;
        private boolean consumed;

        SingleBatchReader(BufferAllocator allocator, Schema schema, ArrowRecordBatch batch) {
            super(allocator);
            this.schema = schema;
            this.batch = batch;
            this.consumed = false;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            if (consumed) {
                return false;
            }
            consumed = true;
            loadRecordBatch(batch);
            return true;
        }

        @Override
        public long bytesRead() {
            return 0L;
        }

        @Override
        protected void closeReadSource() throws IOException {
            // nothing to release here; the record batch is owned by the enclosing caller
        }

        @Override
        protected Schema readSchema() throws IOException {
            return schema;
        }
    }
}

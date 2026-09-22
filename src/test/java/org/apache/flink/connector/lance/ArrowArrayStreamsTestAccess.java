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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.lance.Dataset;
import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;

import java.io.IOException;

/**
 * Test-only bridge to the package-private {@link ArrowArrayStreams}.
 *
 * <p>{@link ArrowArrayStreams} is intentionally package-private production code. This class lives
 * in the same package under {@code src/test} so spikes and IT cases can reuse the exact
 * {@code VectorSchemaRoot -> ArrowArrayStream} bridge the sink uses, rather than re-implementing
 * it (which would make a spike prove something about the test harness instead of about the SDK).
 */
final class ArrowArrayStreamsTestAccess {

    private ArrowArrayStreamsTestAccess() {
        // utility class
    }

    static MergeInsertResult mergeInsert(
            Dataset dataset,
            MergeInsertParams params,
            BufferAllocator allocator,
            VectorSchemaRoot root) throws IOException {
        return ArrowArrayStreams.mergeInsert(dataset, params, allocator, root);
    }
}

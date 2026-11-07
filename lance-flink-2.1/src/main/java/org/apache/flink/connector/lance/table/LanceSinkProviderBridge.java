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

package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.LanceSink;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;

/**
 * Flink version bridge: wraps {@link LanceSink} as a
 * {@link DynamicTableSink.SinkRuntimeProvider}.
 *
 * <p>This file is the "Flink 2.x version": {@code LanceSink} implements Flink 2.1's 2PC composite
 * interface {@code Sink<RowData> + SupportsCommitter<LanceCommittable>} (after FLIP-372, the old
 * {@code TwoPhaseCommittingSink} composite interface was split into two more orthogonal mixins),
 * wrapped via {@link SinkV2Provider#of(org.apache.flink.api.connector.sink2.Sink)}.
 *
 * <p>This overrides the same-named class in the root src (the 1.x version); the root-src version
 * is excluded via maven-compiler-plugin {@code <excludes>} in the 2.1 module pom.
 */
final class LanceSinkProviderBridge {

    private LanceSinkProviderBridge() {}

    static DynamicTableSink.SinkRuntimeProvider toProvider(LanceSink sink) {
        return SinkV2Provider.of(sink);
    }
}

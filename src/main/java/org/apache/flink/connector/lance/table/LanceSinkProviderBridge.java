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
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

/**
 * Flink version bridge: wraps {@link LanceSink} into a
 * {@link DynamicTableSink.SinkRuntimeProvider}.
 *
 * <p>This file is the "Flink 1.x version": {@code LanceSink} implements {@code SinkFunction},
 * wrapped via
 * {@link SinkFunctionProvider#of(org.apache.flink.streaming.api.functions.sink.SinkFunction)}.
 *
 * <p>The {@code lance-flink-2.1} submodule provides another implementation of the same class in the
 * same package (using {@code SinkV2Provider.of(Sink)}), and excludes this file from the 2.1
 * compilation via the maven-compiler-plugin {@code <excludes>}.
 */
final class LanceSinkProviderBridge {

    private LanceSinkProviderBridge() {}

    static DynamicTableSink.SinkRuntimeProvider toProvider(LanceSink sink) {
        return SinkFunctionProvider.of(sink);
    }
}

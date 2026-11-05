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
 * Flink 版本桥接：把 {@link LanceSink} 包装为 {@link DynamicTableSink.SinkRuntimeProvider}。
 *
 * <p>本文件是「Flink 1.x 版本」：{@code LanceSink} 实现 {@code SinkFunction}，
 * 用 {@link SinkFunctionProvider#of(org.apache.flink.streaming.api.functions.sink.SinkFunction)} 包装。
 *
 * <p>{@code lance-flink-2.1} 子模块在同包下提供同名类的另一个实现
 * （用 {@code SinkV2Provider.of(Sink)}），并通过 maven-compiler-plugin 的
 * {@code <excludes>} 把本文件从 2.1 的编译中排除。
 */
final class LanceSinkProviderBridge {

    private LanceSinkProviderBridge() {}

    static DynamicTableSink.SinkRuntimeProvider toProvider(LanceSink sink) {
        return SinkFunctionProvider.of(sink);
    }
}

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
 * Flink 版本桥接：把 {@link LanceSink} 包装为 {@link DynamicTableSink.SinkRuntimeProvider}。
 *
 * <p>本文件是「Flink 2.x 版本」：{@code LanceSink} 实现 Flink 2.1 的 2PC 组合接口
 * {@code Sink<RowData> + SupportsCommitter<LanceCommittable>}（FLIP-372 之后，
 * 旧的 {@code TwoPhaseCommittingSink} 组合接口已被拆分为更正交的两个 mixin），
 * 用 {@link SinkV2Provider#of(org.apache.flink.api.connector.sink2.Sink)} 包装。
 *
 * <p>这是对根 src 中同名类（1.x 版本）的覆盖；根 src 版本已通过 2.1 模块 pom 的
 * maven-compiler-plugin {@code <excludes>} 排除。
 */
final class LanceSinkProviderBridge {

    private LanceSinkProviderBridge() {}

    static DynamicTableSink.SinkRuntimeProvider toProvider(LanceSink sink) {
        return SinkV2Provider.of(sink);
    }
}

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

import org.apache.flink.connector.lance.LanceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

/**
 * Flink 版本桥接：把 {@link LanceSource} 挂载为 {@link DataStream}。
 *
 * <p>本文件是「Flink 1.x 版本」：{@code LanceSource} 实现 {@code SourceFunction}，
 * 走 {@link StreamExecutionEnvironment#addSource}。
 *
 * <p>{@code lance-flink-2.1} 子模块在同包下提供同名类的另一个实现（走 FLIP-27 的
 * {@code env.fromSource(Source, WatermarkStrategy.noWatermarks(), name)}），并通过
 * maven-compiler-plugin 的 {@code <excludes>} 把本文件从 2.1 的编译中排除，
 * 实现对上层 {@code LanceDynamicTableSource} 透明的版本切换。
 */
final class LanceSourceStreamBridge {

    private LanceSourceStreamBridge() {}

    static DataStream<RowData> toStream(
            StreamExecutionEnvironment env, LanceSource source, String name) {
        return env.addSource(source, name);
    }
}

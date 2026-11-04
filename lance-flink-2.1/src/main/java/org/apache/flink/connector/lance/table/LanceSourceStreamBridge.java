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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.lance.LanceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

/**
 * Flink 版本桥接：把 {@link LanceSource} 挂载为 {@link DataStream}。
 *
 * <p>本文件是「Flink 2.x 版本」：{@code LanceSource} 实现 FLIP-27 的
 * {@code Source<RowData, LanceSplit, LanceEnumState>}，走
 * {@link StreamExecutionEnvironment#fromSource}。Lance connector 当前仅批读，
 * 使用 {@link WatermarkStrategy#noWatermarks()} 即可。
 *
 * <p>这是对根 src 中同名类（1.x 版本）的覆盖；根 src 版本已通过 2.1 模块 pom 的
 * maven-compiler-plugin {@code <excludes>} 排除。
 */
final class LanceSourceStreamBridge {

    private LanceSourceStreamBridge() {}

    static DataStream<RowData> toStream(
            StreamExecutionEnvironment env, LanceSource source, String name) {
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), name);
    }
}

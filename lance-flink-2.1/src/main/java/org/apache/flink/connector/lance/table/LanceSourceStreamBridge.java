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
 * Flink version bridge: mounts {@link LanceSource} as a {@link DataStream}.
 *
 * <p>This file is the "Flink 2.x version": {@code LanceSource} implements the FLIP-27
 * {@code Source<RowData, LanceSplit, LanceEnumState>}, via
 * {@link StreamExecutionEnvironment#fromSource}. The Lance connector currently only supports
 * batch reads, so {@link WatermarkStrategy#noWatermarks()} suffices.
 *
 * <p>This overrides the same-named class in the root src (the 1.x version); the root-src version
 * is excluded via maven-compiler-plugin {@code <excludes>} in the 2.1 module pom.
 */
final class LanceSourceStreamBridge {

    private LanceSourceStreamBridge() {}

    static DataStream<RowData> toStream(
            StreamExecutionEnvironment env, LanceSource source, String name) {
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), name);
    }
}

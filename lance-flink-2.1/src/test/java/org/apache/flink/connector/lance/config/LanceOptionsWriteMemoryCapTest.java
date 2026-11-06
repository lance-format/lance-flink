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

package org.apache.flink.connector.lance.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #87 regression tests for {@code write.memory-cap-bytes}.
 *
 * <p>The option gates the writer's OOM safety valve: -1 means unbounded (write() never
 * materializes, eliminating the orphan-file window); a non-negative value bounds the
 * in-memory row buffer, materializing early when the estimate is exceeded.
 */
class LanceOptionsWriteMemoryCapTest {

    @Test
    @DisplayName("write.memory-cap-bytes defaults to -1 (unbounded, write() never materializes)")
    void defaultsToUnbounded() {
        LanceOptions options =
                LanceOptions.builder()
                        .path("/tmp/x")
                        .build();

        assertThat(options.getWriteMemoryCapBytes()).isEqualTo(-1L);
    }

    @Test
    @DisplayName("builder writeMemoryCapBytes round-trips")
    void builderRoundTrip() {
        LanceOptions options =
                LanceOptions.builder()
                        .path("/tmp/x")
                        .writeMemoryCapBytes(64 * 1024 * 1024L)
                        .build();

        assertThat(options.getWriteMemoryCapBytes()).isEqualTo(64 * 1024 * 1024L);
    }

    @Test
    @DisplayName("fromConfiguration reads write.memory-cap-bytes")
    void fromConfigurationRoundTrip() {
        Configuration config = new Configuration();
        config.set(LanceOptions.WRITE_MEMORY_CAP_BYTES, 128 * 1024 * 1024L);

        LanceOptions options = LanceOptions.fromConfiguration(config);

        assertThat(options.getWriteMemoryCapBytes()).isEqualTo(128 * 1024 * 1024L);
    }

    @Test
    @DisplayName("write.memory-cap-bytes below -1 is rejected")
    void rejectsBelowMinusOne() {
        LanceOptions.Builder builder =
                LanceOptions.builder().path("/tmp/x").writeMemoryCapBytes(-2L);

        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("write.memory-cap-bytes");
    }

    @Test
    @DisplayName("write.memory-cap-bytes = 0 is a valid bounded cap")
    void zeroIsValidBoundedCap() {
        LanceOptions options =
                LanceOptions.builder()
                        .path("/tmp/x")
                        .writeMemoryCapBytes(0L)
                        .build();

        assertThat(options.getWriteMemoryCapBytes()).isEqualTo(0L);
    }
}

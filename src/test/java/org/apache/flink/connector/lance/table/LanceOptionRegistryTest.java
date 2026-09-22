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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.lance.PrimaryKeyPersistence;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.util.LanceAllocators;

import org.apache.arrow.memory.BufferAllocator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the A7 option-classification registry and the B3 allocator cap.
 *
 * <p>The A7 cases are deliberately written against the factories' live option sets rather than a
 * restated list: the defect being guarded is precisely that a hand-maintained copy silently drifts
 * from the declarations. A test that restated the keys would drift with it and keep passing.
 */
class LanceOptionRegistryTest {

    // ------------------------------------------------------------------
    // A7: classification derives from the factory declarations
    // ------------------------------------------------------------------

    /**
     * Every option either factory declares must classify as connector configuration. This is the
     * acceptance criterion "adding a new ConfigOption does not require a change to LanceCatalog":
     * a newly declared option is picked up here with no edit, and one that is not would fail.
     */
    @Test
    @DisplayName("every declared factory option classifies as connector configuration")
    void allFactoryOptionsAreConnectorOptions() {
        LanceDynamicTableFactory tableFactory = new LanceDynamicTableFactory();
        LanceCatalogFactory catalogFactory = new LanceCatalogFactory();

        for (ConfigOption<?> option : tableFactory.requiredOptions()) {
            assertThat(LanceOptionRegistry.isConnectorOption(option.key()))
                    .as("table factory required option '%s' must not be a TBLPROPERTY", option.key())
                    .isTrue();
        }
        for (ConfigOption<?> option : tableFactory.optionalOptions()) {
            assertThat(LanceOptionRegistry.isConnectorOption(option.key()))
                    .as("table factory optional option '%s' must not be a TBLPROPERTY", option.key())
                    .isTrue();
        }
        for (ConfigOption<?> option : catalogFactory.requiredOptions()) {
            assertThat(LanceOptionRegistry.isConnectorOption(option.key()))
                    .as("catalog factory required option '%s' must not be a TBLPROPERTY", option.key())
                    .isTrue();
        }
        for (ConfigOption<?> option : catalogFactory.optionalOptions()) {
            assertThat(LanceOptionRegistry.isConnectorOption(option.key()))
                    .as("catalog factory optional option '%s' must not be a TBLPROPERTY", option.key())
                    .isTrue();
        }
    }

    /**
     * These two were declared by {@code LanceCatalogFactory} but absent from the old hard-coded
     * set, so they leaked into the Lance dataset config as user properties. Pinned explicitly
     * because they are the concrete evidence that the dual-truth defect was real.
     */
    @Test
    @DisplayName("S3 options missing from the old hard-coded set are now classified correctly")
    void previouslyLeakingS3OptionsAreReserved() {
        assertThat(LanceOptionRegistry.isTblProperty("s3-virtual-hosted-style")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("s3-allow-http")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("s3-access-key")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("s3-secret-key")).isFalse();
    }

    @Test
    @DisplayName("reserved prefixes are connector configuration")
    void reservedPrefixesAreConnectorOptions() {
        assertThat(LanceOptionRegistry.isTblProperty("read.batch-size")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("write.mode")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("index.type")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("vector.column")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("hadoop.tbdsfs.meta")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("connector")).isFalse();
        assertThat(LanceOptionRegistry.isTblProperty("path")).isFalse();
    }

    @Test
    @DisplayName("user properties are TBLPROPERTIES")
    void userPropertiesAreTblProperties() {
        assertThat(LanceOptionRegistry.isTblProperty("owner")).isTrue();
        assertThat(LanceOptionRegistry.isTblProperty("retention-days")).isTrue();
        assertThat(LanceOptionRegistry.isTblProperty("my.custom.prop")).isTrue();
    }

    // ------------------------------------------------------------------
    // A7: dataset config ownership
    // ------------------------------------------------------------------

    /**
     * The cross-engine guarantee: a foreign engine's namespaced key is never Flink-owned, so the
     * RESET path can never select it for deletion.
     */
    @Test
    @DisplayName("foreign engine config keys are not Flink-owned")
    void foreignEngineKeysAreNotOwned() {
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey("spark.stats.approx_count")).isFalse();
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey("trino.metadata.version")).isFalse();
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey("ray.checkpoint.id")).isFalse();
    }

    /**
     * The other half of the ownership rule. The previous heuristic treated every dotted key as
     * foreign, which was safe but made RESET impossible for dotted properties Flink itself wrote.
     */
    @Test
    @DisplayName("Flink-namespaced and unnamespaced keys are Flink-owned")
    void flinkKeysAreOwned() {
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey("flink.primary-key")).isTrue();
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey(
                PrimaryKeyPersistence.PK_CONFIG_KEY)).isTrue();
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey("owner")).isTrue();
        assertThat(LanceOptionRegistry.isFlinkOwnedConfigKey("retention-days")).isTrue();
    }

    // ------------------------------------------------------------------
    // B3: allocator cap
    // ------------------------------------------------------------------

    @Test
    @DisplayName("allocator limit defaults to unlimited when unset")
    void allocatorDefaultsToUnlimited() {
        assertThat(LanceAllocators.resolveMaxBytes(null))
                .isEqualTo(LanceAllocators.UNLIMITED);
        assertThat(LanceAllocators.resolveMaxBytes(new HashMap<>()))
                .isEqualTo(LanceAllocators.UNLIMITED);
    }

    @Test
    @DisplayName("allocator limit is read from options")
    void allocatorLimitIsRead() {
        Map<String, String> options = new HashMap<>();
        options.put(LanceAllocators.ALLOCATOR_MAX_BYTES_KEY, "1048576");
        assertThat(LanceAllocators.resolveMaxBytes(options)).isEqualTo(1_048_576L);
    }

    /**
     * A malformed memory hint must not prevent a job from starting; the safe fallback is the
     * pre-existing unbounded behaviour.
     */
    @Test
    @DisplayName("malformed or non-positive allocator limits fall back to unlimited")
    void malformedAllocatorLimitsFallBack() {
        Map<String, String> garbage = new HashMap<>();
        garbage.put(LanceAllocators.ALLOCATOR_MAX_BYTES_KEY, "not-a-number");
        assertThat(LanceAllocators.resolveMaxBytes(garbage))
                .isEqualTo(LanceAllocators.UNLIMITED);

        Map<String, String> negative = new HashMap<>();
        negative.put(LanceAllocators.ALLOCATOR_MAX_BYTES_KEY, "-1");
        assertThat(LanceAllocators.resolveMaxBytes(negative))
                .isEqualTo(LanceAllocators.UNLIMITED);

        Map<String, String> zero = new HashMap<>();
        zero.put(LanceAllocators.ALLOCATOR_MAX_BYTES_KEY, "0");
        assertThat(LanceAllocators.resolveMaxBytes(zero))
                .isEqualTo(LanceAllocators.UNLIMITED);
    }

    /**
     * The cap must actually bind, not merely be recorded. Allocating beyond the limit has to fail
     * rather than succeed silently -- otherwise the option would be decorative.
     */
    @Test
    @DisplayName("a bounded allocator actually refuses oversized allocations")
    void boundedAllocatorEnforcesLimit() {
        try (BufferAllocator allocator = LanceAllocators.create("test-bounded", 1024)) {
            assertThat(allocator.getLimit()).isEqualTo(1024L);
            // Released explicitly: Arrow treats an outstanding buffer at close() as a leak and
            // throws, which would mask the assertion this test actually makes.
            try (org.apache.arrow.memory.ArrowBuf buf = allocator.buffer(512)) {
                assertThat(buf).isNotNull();
            }
            try {
                allocator.buffer(1_048_576);
                org.assertj.core.api.Assertions.fail(
                        "allocation beyond the configured limit should have been refused");
            } catch (OutOfMemoryError | RuntimeException expected) {
                // Arrow signals limit breaches with OutOfMemoryException, a RuntimeException.
                assertThat(expected).isNotNull();
            }
        }
    }

    @Test
    @DisplayName("an unlimited allocator reports Long.MAX_VALUE")
    void unlimitedAllocatorIsUnbounded() {
        try (BufferAllocator allocator = LanceAllocators.create("test-unlimited")) {
            assertThat(allocator.getLimit()).isEqualTo(Long.MAX_VALUE);
        }
    }

    @Test
    @DisplayName("LanceOptions exposes the allocator cap and defaults to unlimited")
    void lanceOptionsCarriesAllocatorCap() {
        LanceOptions defaults = LanceOptions.builder().path("/tmp/x").build();
        assertThat(defaults.getArrowAllocatorMaxBytes()).isEqualTo(Long.MAX_VALUE);

        LanceOptions bounded = LanceOptions.builder()
                .path("/tmp/x")
                .arrowAllocatorMaxBytes(4096)
                .build();
        assertThat(bounded.getArrowAllocatorMaxBytes()).isEqualTo(4096L);

        LanceOptions normalised = LanceOptions.builder()
                .path("/tmp/x")
                .arrowAllocatorMaxBytes(-5)
                .build();
        assertThat(normalised.getArrowAllocatorMaxBytes()).isEqualTo(Long.MAX_VALUE);
    }
}

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
import org.apache.flink.connector.lance.table.LanceDynamicTableFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the {@code write.data-storage-version} option.
 *
 * <p>The option exists because some Arrow types are gated on the Lance file format version: a MAP
 * column needs 2.2 or newer, and on the SDK default of 2.1 the schema is accepted at CREATE TABLE
 * while the first write fails inside the Rust encoder. See
 * {@code .gh-comments/issue-map-type-blocked-by-storage-version.md}.
 */
class WriteDataStorageVersionOptionTest {

    @Test
    @DisplayName("Unset leaves the version null so the SDK keeps its own default")
    void unsetLeavesVersionNull() {
        // Deliberately not defaulted to "2.1": hardcoding today's SDK default would pin the
        // connector to it once the SDK moves forward.
        LanceOptions options =
                LanceOptions.fromConfiguration(
                        Configuration.fromMap(
                                java.util.Collections.singletonMap("path", "/tmp/x.lance")));

        assertThat(options.getWriteDataStorageVersion()).isNull();
        assertThat(LanceOptions.WRITE_DATA_STORAGE_VERSION.hasDefaultValue()).isFalse();
    }

    @Test
    @DisplayName("An explicit version reaches LanceOptions")
    void explicitVersionIsCarried() {
        java.util.Map<String, String> map = new java.util.HashMap<>();
        map.put("path", "/tmp/x.lance");
        map.put("write.data-storage-version", "2.2");

        LanceOptions options = LanceOptions.fromConfiguration(Configuration.fromMap(map));

        assertThat(options.getWriteDataStorageVersion()).isEqualTo("2.2");
    }

    @Test
    @DisplayName("The factory declares the option, so it is a reserved key rather than a user property")
    void factoryDeclaresOption() {
        // LanceOptionRegistry derives the reserved-key set from the factories' live declarations.
        // An undeclared option would be written into the dataset config as a user TBLPROPERTY.
        assertThat(new LanceDynamicTableFactory().optionalOptions())
                .extracting(o -> o.key())
                .contains("write.data-storage-version");
    }

    @Test
    @DisplayName("The option participates in equals, hashCode and toString")
    void optionParticipatesInValueSemantics() {
        // A field missing from equals makes two differently-configured option objects compare
        // equal, which would silently reuse a cached sink or catalog built for another version.
        LanceOptions base = LanceOptions.builder().path("/tmp/x.lance").build();
        LanceOptions withVersion =
                LanceOptions.builder().path("/tmp/x.lance").writeDataStorageVersion("2.2").build();

        assertThat(withVersion).isNotEqualTo(base);
        assertThat(withVersion.hashCode()).isNotEqualTo(base.hashCode());
        assertThat(withVersion.toString()).contains("2.2");
    }
}

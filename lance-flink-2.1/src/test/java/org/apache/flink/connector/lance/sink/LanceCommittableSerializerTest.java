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

package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceCommittableSerializer round-trip tests.
 *
 * <p>B2 (maintainer review PR#86): committable serialization is on the Flink 2PC
 * checkpoint path; a format regression silently breaks Exactly-Once on recovery.
 *
 * <p>These tests avoid constructing real {@code FragmentMetadata} (which requires a
 * live Lance dataset JNI handle) and focus on the metadata we control: schema,
 * writeMode, path, and the empty-fragments boundary. End-to-end committable paths
 * with non-empty fragments are already covered by {@code FlinkSqlDemo}.
 */
class LanceCommittableSerializerTest {

    private final LanceCommittableSerializer serializer = LanceCommittableSerializer.INSTANCE;

    @Test
    @DisplayName("Empty-fragment APPEND committable round-trips")
    void appendEmptyCommittableRoundTrip() throws IOException {
        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        /* arrowSchema */ null,
                        LanceOptions.WriteMode.APPEND,
                        Collections.emptyList());

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getDatasetPath()).isEqualTo("/tmp/x");
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(out.getArrowSchema()).isNull();
        assertThat(out.getFragments()).isEmpty();
    }

    @Test
    @DisplayName("Empty-fragment OVERWRITE committable round-trips preserving writeMode")
    void overwriteEmptyCommittableRoundTrip() throws IOException {
        LanceCommittable in =
                new LanceCommittable(
                        "s3://bucket/foo",
                        /* arrowSchema */ null,
                        LanceOptions.WriteMode.OVERWRITE,
                        Collections.emptyList());

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getDatasetPath()).isEqualTo("s3://bucket/foo");
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
    }

    @Test
    @DisplayName("Committable with Arrow Schema round-trips via JSON")
    void committableWithSchemaRoundTrip() throws IOException {
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        schema,
                        LanceOptions.WriteMode.APPEND,
                        Collections.emptyList());

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getArrowSchema()).isNotNull();
        assertThat(out.getArrowSchema().getFields()).hasSize(2);
        assertThat(out.getArrowSchema().getFields().get(0).getName()).isEqualTo("id");
        assertThat(out.getArrowSchema().getFields().get(1).getName()).isEqualTo("name");
    }

    @Test
    @DisplayName("Null writeMode in constructor is normalized to APPEND and round-trips")
    void nullWriteModeNormalizedToAppend() throws IOException {
        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        /* schema */ null,
                        /* writeMode */ null,
                        Collections.emptyList());

        assertThat(in.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
    }

    @Test
    @DisplayName("deserialize rejects unknown serializer version")
    void rejectsUnknownVersion() {
        byte[] bytes = new byte[16]; // garbage
        assertThatThrownBy(() -> serializer.deserialize(999, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    @DisplayName("getVersion is stable positive")
    void getVersionReturnsPositive() {
        assertThat(serializer.getVersion()).isGreaterThan(0);
    }
}

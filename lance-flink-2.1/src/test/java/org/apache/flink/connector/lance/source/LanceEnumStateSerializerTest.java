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

package org.apache.flink.connector.lance.source;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceEnumStateSerializer round-trip tests.
 *
 * <p>B2 (maintainer review PR#86): the enumerator state is persisted to Flink's
 * checkpoint, so any binary format regression silently breaks failover.
 */
class LanceEnumStateSerializerTest {

    private final LanceEnumStateSerializer serializer = LanceEnumStateSerializer.INSTANCE;

    @Test
    @DisplayName("Empty state round-trips")
    void emptyStateRoundTrip() throws IOException {
        LanceEnumState in = new LanceEnumState(Collections.emptyList());
        byte[] bytes = serializer.serialize(in);
        LanceEnumState out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getRemainingSplits()).isEmpty();
    }

    @Test
    @DisplayName("State with multiple fragment splits round-trips preserving order")
    void multipleFragmentSplitsRoundTripInOrder() throws IOException {
        LanceEnumState in =
                new LanceEnumState(
                        Arrays.asList(
                                LanceSplit.fragment(0L),
                                LanceSplit.fragment(1L),
                                LanceSplit.fragment(42L)));
        byte[] bytes = serializer.serialize(in);
        LanceEnumState out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getRemainingSplits()).hasSize(3);
        assertThat(out.getRemainingSplits().get(0).getFragmentId()).isEqualTo(0L);
        assertThat(out.getRemainingSplits().get(1).getFragmentId()).isEqualTo(1L);
        assertThat(out.getRemainingSplits().get(2).getFragmentId()).isEqualTo(42L);
    }

    @Test
    @DisplayName("Mixed kinds (WHOLE_DATASET + FRAGMENT) round-trip")
    void mixedKindsRoundTrip() throws IOException {
        LanceEnumState in =
                new LanceEnumState(
                        Arrays.asList(LanceSplit.wholeDataset(), LanceSplit.fragment(7L)));
        byte[] bytes = serializer.serialize(in);
        LanceEnumState out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getRemainingSplits()).hasSize(2);
        assertThat(out.getRemainingSplits().get(0).getKind())
                .isEqualTo(LanceSplit.Kind.WHOLE_DATASET);
        assertThat(out.getRemainingSplits().get(1).getKind())
                .isEqualTo(LanceSplit.Kind.FRAGMENT);
        assertThat(out.getRemainingSplits().get(1).getFragmentId()).isEqualTo(7L);
    }

    @Test
    @DisplayName("deserialize rejects unknown serializer version")
    void rejectsUnknownVersion() {
        byte[] bytes = new byte[] {0, 0, 0, 0}; // garbage
        assertThatThrownBy(() -> serializer.deserialize(999, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }
}

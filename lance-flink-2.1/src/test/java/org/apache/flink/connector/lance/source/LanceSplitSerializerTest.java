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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceSplitSerializer round-trip tests.
 *
 * <p>B2 (maintainer review PR#86): serializer coverage was 0 before this PR; split
 * serialization is on the hot Flink checkpoint path, so a round-trip and a bad-kind
 * byte test are the minimum to catch binary format regressions.
 */
class LanceSplitSerializerTest {

    private final LanceSplitSerializer serializer = LanceSplitSerializer.INSTANCE;

    @Test
    @DisplayName("Fragment split round-trips preserving splitId, kind, fragmentId")
    void fragmentSplitRoundTrip() throws IOException {
        LanceSplit in = LanceSplit.fragment(42L);
        byte[] bytes = serializer.serialize(in);
        LanceSplit out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.splitId()).isEqualTo("fragment-42");
        assertThat(out.getKind()).isEqualTo(LanceSplit.Kind.FRAGMENT);
        assertThat(out.getFragmentId()).isEqualTo(42L);
        assertThat(out).isEqualTo(in);
    }

    @Test
    @DisplayName("WHOLE_DATASET split round-trips with fragmentId=-1")
    void wholeDatasetSplitRoundTrip() throws IOException {
        LanceSplit in = LanceSplit.wholeDataset();
        byte[] bytes = serializer.serialize(in);
        LanceSplit out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.splitId()).isEqualTo("whole-dataset");
        assertThat(out.getKind()).isEqualTo(LanceSplit.Kind.WHOLE_DATASET);
        assertThat(out.getFragmentId()).isEqualTo(-1L);
        assertThat(out).isEqualTo(in);
    }

    @Test
    @DisplayName("deserialize rejects unknown serializer version")
    void rejectsUnknownVersion() {
        byte[] bytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}; // garbage, but version != 1
        assertThatThrownBy(() -> serializer.deserialize(999, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    @DisplayName("deserialize rejects unknown kind byte within supported version")
    void rejectsUnknownKindByte() throws IOException {
        // Hand-craft a payload matching version=1 layout with an invalid kind byte (0x7F).
        // Layout: [kind:byte][fragmentId:long][splitId:utf-16-ish via writeUTF]
        // We only need the first byte to be bogus; the deserializer should fail fast.
        byte[] bytes = new byte[] {
            0x7F, // bad kind
            0, 0, 0, 0, 0, 0, 0, 0, // fragmentId=0
            0, 1, 'x' // writeUTF: length=1, char='x'
        };
        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion(), bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kind");
    }

    @Test
    @DisplayName("getVersion returns a stable positive integer")
    void getVersionReturnsPositive() {
        assertThat(serializer.getVersion()).isGreaterThan(0);
    }
}

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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B1 regression tests for {@link LanceCommittable} + {@link LanceSinkWriter}'s stateless
 * contract.
 *
 * <p>Before B1 fix: {@code LanceCommittable} carried a {@code createDataset:boolean} flag
 * decided inside {@link LanceSinkWriter} based on {@code Files.exists(path)} at construct
 * time and a transient {@code subtaskHasWritten} flag. That design was racy against
 * {@code Fragment.write()}'s side effect of materialising {@code <path>/data/} directories
 * before any commit happens, and lost its decision after writer re-create on failover.
 *
 * <p>After B1 fix: Writer no longer makes that decision. Each committable carries only the
 * declared {@code writeMode}; the {@link LanceCommitter} decides Overwrite-vs-Append at
 * commit time via {@code Dataset.open(path)} probing. These tests pin that contract so a
 * future refactor doesn't reintroduce the leak.
 */
class LanceSinkWriterCreateDatasetFlagTest {

    @Test
    @DisplayName("LanceCommittable has no createDataset field — only writeMode")
    void committableHasNoCreateDatasetField() {
        LanceCommittable c =
                new LanceCommittable(
                        "/tmp/x",
                        /* schema */ null,
                        LanceOptions.WriteMode.APPEND,
                        Collections.emptyList());

        // Writer no longer owns Overwrite-vs-Append decision; only the declared mode
        // flows through to the Committer.
        assertThat(c.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);

        // Explicitly assert that LanceCommittable's public surface does NOT expose any
        // "isCreateDataset" / "getCreateDataset" accessor — the moment such an accessor
        // reappears, this test fails to signal regression of the B1 fix.
        boolean hasCreateDatasetAccessor =
                java.util.Arrays.stream(LanceCommittable.class.getMethods())
                        .map(java.lang.reflect.Method::getName)
                        .anyMatch(
                                n ->
                                        n.toLowerCase(java.util.Locale.ROOT)
                                                        .contains("createdataset"));
        assertThat(hasCreateDatasetAccessor)
                .withFailMessage(
                        "LanceCommittable must not re-expose a 'createDataset' accessor "
                                + "— B1 moved that decision to LanceCommitter; the stateless "
                                + "Writer contract is being broken.")
                .isFalse();
    }

    @Test
    @DisplayName("Committable preserves OVERWRITE writeMode so Committer sees intent")
    void overwriteModeFlowsThroughCommittable() {
        LanceCommittable c =
                new LanceCommittable(
                        "/tmp/x",
                        /* schema */ null,
                        LanceOptions.WriteMode.OVERWRITE,
                        Collections.emptyList());

        assertThat(c.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
    }

    @Test
    @DisplayName("Null writeMode is defensively normalised to APPEND")
    void nullWriteModeNormalisedToAppend() {
        LanceCommittable c =
                new LanceCommittable(
                        "/tmp/x",
                        /* schema */ null,
                        /* writeMode */ null,
                        Collections.emptyList());

        // A null writeMode could happen if a hand-constructed committable gets
        // deserialised from an unknown older format; defaulting to APPEND is the
        // safe choice (never truncates existing data).
        assertThat(c.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
    }

    @Test
    @DisplayName("Fragment list defensive copy — mutating the input doesn't leak")
    void committableTakesDefensiveFragmentCopy() {
        java.util.List<org.lance.FragmentMetadata> input = new java.util.ArrayList<>();
        LanceCommittable c =
                new LanceCommittable(
                        "/tmp/x",
                        null,
                        LanceOptions.WriteMode.APPEND,
                        input);

        // Should not throw and should not reflect the mutation inside the committable.
        // We can't construct a real FragmentMetadata without a native handle, so this
        // only checks the empty-list path (which is the one checkpoint recovery hits
        // most often: empty-batch committables from idle subtasks).
        assertThat(c.getFragments()).isEmpty();
        assertThat(c.isEmpty()).isTrue();
    }
}

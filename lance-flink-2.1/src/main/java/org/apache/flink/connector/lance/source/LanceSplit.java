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

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/**
 * Split unit of the Lance source.
 *
 * <p>Two semantics:
 * <ul>
 *   <li>{@link Kind#FRAGMENT}: corresponds to one Fragment of the Lance Dataset (parallel scan by
 *       Fragment when there is no filter / limit);
 *   <li>{@link Kind#WHOLE_DATASET}: when a filter or limit is present, the whole table produces
 *       only 1 such split, handled by a single reader — equivalent to the old "only subtask 0 runs"
 *       fallback strategy.
 * </ul>
 *
 * <p>This class is immutable and can be directly used as the payload dispatched by
 * {@link org.apache.flink.api.connector.source.SplitEnumerator} to a
 * {@link org.apache.flink.api.connector.source.SourceReader}.
 */
public final class LanceSplit implements SourceSplit {

    /** Split semantics. */
    public enum Kind {
        /** A single Fragment slice. */
        FRAGMENT,
        /** Whole-table scan (filter or limit scenario). */
        WHOLE_DATASET
    }

    private final String splitId;
    private final Kind kind;

    /** Used only by the FRAGMENT kind; -1 for WHOLE_DATASET. */
    private final long fragmentId;

    public LanceSplit(String splitId, Kind kind, long fragmentId) {
        this.splitId = Objects.requireNonNull(splitId, "splitId");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.fragmentId = fragmentId;
    }

    public static LanceSplit fragment(long fragmentId) {
        return new LanceSplit("fragment-" + fragmentId, Kind.FRAGMENT, fragmentId);
    }

    public static LanceSplit wholeDataset() {
        return new LanceSplit("whole-dataset", Kind.WHOLE_DATASET, -1L);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public Kind getKind() {
        return kind;
    }

    public long getFragmentId() {
        return fragmentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LanceSplit)) return false;
        LanceSplit that = (LanceSplit) o;
        return fragmentId == that.fragmentId
                && kind == that.kind
                && splitId.equals(that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, kind, fragmentId);
    }

    @Override
    public String toString() {
        return "LanceSplit{" + splitId + ", kind=" + kind + ", fragmentId=" + fragmentId + '}';
    }
}

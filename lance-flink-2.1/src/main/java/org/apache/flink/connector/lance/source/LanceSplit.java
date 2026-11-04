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
 * Lance 数据源的 split 单元。
 *
 * <p>两种语义：
 * <ul>
 *   <li>{@link Kind#FRAGMENT}：对应 Lance Dataset 的一个 Fragment（无 filter / limit 时按 Fragment 并行扫描）；
 *   <li>{@link Kind#WHOLE_DATASET}：当存在 filter 或 limit 时，整表只产出 1 个此类 split，
 *       由单个 reader 处理，等价于旧实现中「仅 subtask 0 执行」的降级策略。
 * </ul>
 *
 * <p>此类不可变，可直接作为 {@link org.apache.flink.api.connector.source.SplitEnumerator}
 * 分发给 {@link org.apache.flink.api.connector.source.SourceReader} 的载荷。
 */
public final class LanceSplit implements SourceSplit {

    /** split 语义。 */
    public enum Kind {
        /** 单个 Fragment 切片。 */
        FRAGMENT,
        /** 整表 scan（filter 或 limit 场景）。 */
        WHOLE_DATASET
    }

    private final String splitId;
    private final Kind kind;

    /** 仅 FRAGMENT 类型使用；WHOLE_DATASET 时为 -1。 */
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

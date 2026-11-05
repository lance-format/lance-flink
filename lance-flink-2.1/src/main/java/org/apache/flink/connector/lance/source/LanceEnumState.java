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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link LanceSplitEnumerator} 的快照状态：剩余未分配的 splits。
 *
 * <p>当发生 failover 时，Flink runtime 会用此状态重建 enumerator，
 * 已分配给 reader 但未完成的 split 由 reader 侧 report 回来再次纳入未分配集合。
 */
public final class LanceEnumState {

    private final List<LanceSplit> remainingSplits;

    public LanceEnumState(Collection<LanceSplit> remainingSplits) {
        this.remainingSplits = new ArrayList<>(remainingSplits);
    }

    public List<LanceSplit> getRemainingSplits() {
        return Collections.unmodifiableList(remainingSplits);
    }

    @Override
    public String toString() {
        return "LanceEnumState{remaining=" + remainingSplits.size() + '}';
    }
}

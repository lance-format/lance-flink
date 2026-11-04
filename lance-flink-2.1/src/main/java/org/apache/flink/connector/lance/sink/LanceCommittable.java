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

import org.lance.FragmentMetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link LanceSinkWriter#prepareCommit()} 的产物，由 {@link LanceCommitter#commit} 消费。
 * 携带本次 checkpoint 窗口内 writer 已经写出的所有 Lance Fragment 元数据，
 * 以及决定提交方式所需的最小上下文：
 * <ul>
 *   <li>{@link #datasetPath} —— 目标 dataset 路径
 *   <li>{@link #arrowSchema} —— 建表用的 Arrow Schema（仅 {@code createDataset=true} 时使用）
 *   <li>{@link #createDataset} —— 是否首次写入（走 Overwrite 建表 vs Append 追加）
 *   <li>{@link #fragments} —— Lance 已写出的 FragmentMetadata 列表
 * </ul>
 *
 * <p>所有字段均可序列化：{@link FragmentMetadata} 本身 {@code implements Serializable}，
 * Arrow {@link org.apache.arrow.vector.types.pojo.Schema} 的序列化 / 反序列化由
 * {@link LanceCommittableSerializer} 负责（Schema 不实现 Serializable，通过 Arrow IPC byte 序列化）。
 */
public final class LanceCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String datasetPath;
    private final transient org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final boolean createDataset;
    private final List<FragmentMetadata> fragments;

    public LanceCommittable(
            String datasetPath,
            org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            boolean createDataset,
            List<FragmentMetadata> fragments) {
        this.datasetPath = Objects.requireNonNull(datasetPath, "datasetPath");
        this.arrowSchema = arrowSchema;
        this.createDataset = createDataset;
        this.fragments =
                fragments == null ? Collections.emptyList() : new ArrayList<>(fragments);
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public org.apache.arrow.vector.types.pojo.Schema getArrowSchema() {
        return arrowSchema;
    }

    public boolean isCreateDataset() {
        return createDataset;
    }

    public List<FragmentMetadata> getFragments() {
        return Collections.unmodifiableList(fragments);
    }

    public boolean isEmpty() {
        return fragments.isEmpty();
    }

    @Override
    public String toString() {
        return "LanceCommittable{path="
                + datasetPath
                + ", createDataset="
                + createDataset
                + ", fragments="
                + fragments.size()
                + '}';
    }
}

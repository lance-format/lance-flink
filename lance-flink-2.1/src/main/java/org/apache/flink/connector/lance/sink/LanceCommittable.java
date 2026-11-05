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

import org.lance.FragmentMetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link LanceSinkWriter#prepareCommit()} 的产物，由 {@link LanceCommitter#commit} 消费。
 *
 * <p>Writer 侧不再承担任何「是否首次写入 / 是否建表 / 是否 Overwrite」的判定——Writer
 * 完全无状态，只负责把一批 {@link FragmentMetadata} 和建表所需的 Arrow Schema 打包；
 * 全部决策（dataset 是否已存在、首批走 Overwrite 还是 Append、Overwrite 模式下的清理）
 * 由 {@link LanceCommitter} 完成，因为 Committer 天然串行、无 subtask 并发冲突。
 *
 * <p>字段：
 * <ul>
 *   <li>{@link #datasetPath} —— 目标 dataset 路径
 *   <li>{@link #arrowSchema} —— 建表用的 Arrow Schema（Overwrite / 首批 commit 时使用）
 *   <li>{@link #writeMode} —— 作业声明的写入模式（APPEND / OVERWRITE）
 *   <li>{@link #fragments} —— Writer 已写出的 Fragment 元数据列表
 * </ul>
 *
 * <p>{@link FragmentMetadata} 实现 {@code Serializable}；Arrow {@link
 * org.apache.arrow.vector.types.pojo.Schema} 不实现 {@code Serializable}，由
 * {@link LanceCommittableSerializer} 走 Arrow JSON 文本序列化。
 */
public final class LanceCommittable implements Serializable {

    private static final long serialVersionUID = 2L;

    private final String datasetPath;
    private final transient org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private final LanceOptions.WriteMode writeMode;
    private final List<FragmentMetadata> fragments;

    public LanceCommittable(
            String datasetPath,
            org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            LanceOptions.WriteMode writeMode,
            List<FragmentMetadata> fragments) {
        this.datasetPath = Objects.requireNonNull(datasetPath, "datasetPath");
        this.arrowSchema = arrowSchema;
        this.writeMode = writeMode == null ? LanceOptions.WriteMode.APPEND : writeMode;
        this.fragments =
                fragments == null ? Collections.emptyList() : new ArrayList<>(fragments);
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public org.apache.arrow.vector.types.pojo.Schema getArrowSchema() {
        return arrowSchema;
    }

    public LanceOptions.WriteMode getWriteMode() {
        return writeMode;
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
                + ", writeMode="
                + writeMode
                + ", fragments="
                + fragments.size()
                + '}';
    }
}

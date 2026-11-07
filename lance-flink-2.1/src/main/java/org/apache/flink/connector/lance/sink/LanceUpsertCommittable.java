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

import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link LanceUpsertWriter#prepareCommit()} 的产物，由 {@link LanceUpsertCommitter#commit} 消费。
 *
 * <p>与 append 路径的 {@link LanceCommittable} 不同：{@code Dataset.mergeInsert} 消费的是
 * <b>行数据</b>（{@code ArrowArrayStream}）而非 fragment 元数据，因此本类携带的是 Arrow IPC
 * stream 序列化后的两批行数据：
 *
 * <ul>
 *   <li>{@link #upsertBatch} —— {@code +I/+U} 行（Arrow IPC bytes），committer 走
 *       {@code mergeInsert(UpdateAll + InsertAll)}；</li>
 *   <li>{@link #deleteBatch} —— {@code -D} 行（Arrow IPC bytes），committer 走
 *       {@code mergeInsert(Delete + DoNothing)}。</li>
 * </ul>
 *
 * <p>两批必须分开序列化，因为单个 {@code mergeInsert} 只能设一种 {@code WhenMatched}
 * 行为（PoC 已实证：{@code WhenMatched} 是单一枚举，无法在一次调用内混合 update 与 delete）。
 *
 * <p>{@code UPDATE_BEFORE} 在 writer 侧即被丢弃（upsert 不需要旧值），不会进入任何一批。
 */
public final class LanceUpsertCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String datasetPath;
    private final transient Schema arrowSchema;
    private final List<String> primaryKeys;
    private final LanceOptions.WriteMode writeMode;
    private final byte[] upsertBatch;
    private final byte[] deleteBatch;

    public LanceUpsertCommittable(
            String datasetPath,
            Schema arrowSchema,
            List<String> primaryKeys,
            LanceOptions.WriteMode writeMode,
            byte[] upsertBatch,
            byte[] deleteBatch) {
        this.datasetPath = Objects.requireNonNull(datasetPath, "datasetPath");
        this.arrowSchema = arrowSchema;
        this.primaryKeys =
                primaryKeys == null ? Collections.emptyList() : new ArrayList<>(primaryKeys);
        this.writeMode = writeMode == null ? LanceOptions.WriteMode.APPEND : writeMode;
        this.upsertBatch = upsertBatch == null ? new byte[0] : upsertBatch;
        this.deleteBatch = deleteBatch == null ? new byte[0] : deleteBatch;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public Schema getArrowSchema() {
        return arrowSchema;
    }

    public List<String> getPrimaryKeys() {
        return Collections.unmodifiableList(primaryKeys);
    }

    public LanceOptions.WriteMode getWriteMode() {
        return writeMode;
    }

    public byte[] getUpsertBatch() {
        return upsertBatch;
    }

    public byte[] getDeleteBatch() {
        return deleteBatch;
    }

    public boolean isEmpty() {
        return upsertBatch.length == 0 && deleteBatch.length == 0;
    }

    @Override
    public String toString() {
        return "LanceUpsertCommittable{path="
                + datasetPath
                + ", writeMode="
                + writeMode
                + ", primaryKeys="
                + primaryKeys
                + ", upsertBatchBytes="
                + upsertBatch.length
                + ", deleteBatchBytes="
                + deleteBatch.length
                + '}';
    }
}

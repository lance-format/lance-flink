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
 * The product of {@link LanceSinkWriter#prepareCommit()}, consumed by {@link LanceCommitter#commit}.
 *
 * <p>The writer no longer makes any "is this the first write / create the table / overwrite"
 * decision — the writer is completely stateless and only packages a batch of
 * {@link FragmentMetadata} together with the Arrow Schema needed to create the table; all
 * decisions (whether the dataset already exists, whether the first batch goes through Overwrite or
 * Append, and cleanup under Overwrite mode) are made by {@link LanceCommitter}, because the
 * committer is naturally serial and free of subtask concurrency conflicts.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #datasetPath} — target dataset path
 *   <li>{@link #arrowSchema} — Arrow Schema used to create the table (for Overwrite / first commit)
 *   <li>{@link #writeMode} — the write mode declared by the job (APPEND / OVERWRITE)
 *   <li>{@link #fragments} — the list of Fragment metadata already written by the writer
 * </ul>
 *
 * <p>{@link FragmentMetadata} implements {@code Serializable}; Arrow {@link
 * org.apache.arrow.vector.types.pojo.Schema} does not implement {@code Serializable} and is
 * serialized by {@link LanceCommittableSerializer} via Arrow JSON text.
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

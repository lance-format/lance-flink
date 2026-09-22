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

package org.apache.flink.connector.lance;

import org.lance.Dataset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Persists the Flink primary-key column names into the Lance dataset config so that the
 * {@code PRIMARY KEY ... NOT ENFORCED} constraint survives a catalog round-trip.
 *
 * <p>Lance has no native primary-key constraint; the connector stores the ordered key column
 * names under a reserved config key via {@link Dataset#updateConfig(Map)} and restores them via
 * {@link Dataset#getConfig()}.
 */
public final class PrimaryKeyPersistence {

    /** Reserved Lance dataset config key holding the comma-separated primary-key columns. */
    public static final String PK_CONFIG_KEY = "flink.primary-key";

    private PrimaryKeyPersistence() {
        // utility class
    }

    /**
     * Persist the primary-key column names into the dataset config.
     *
     * @param dataset     the Lance dataset (must already be materialized)
     * @param primaryKeys ordered primary-key column names; empty/null writes nothing
     */
    public static void persist(Dataset dataset, List<String> primaryKeys) {
        if (dataset == null || primaryKeys == null || primaryKeys.isEmpty()) {
            return;
        }
        dataset.updateConfig(Collections.singletonMap(PK_CONFIG_KEY, String.join(",", primaryKeys)));
    }

    /**
     * Load the primary-key column names from the dataset config.
     *
     * @param dataset the Lance dataset
     * @return ordered primary-key column names, or an empty list if not configured
     */
    public static List<String> load(Dataset dataset) {
        if (dataset == null) {
            return Collections.emptyList();
        }
        Map<String, String> config = dataset.getConfig();
        if (config == null) {
            return Collections.emptyList();
        }
        String raw = config.get(PK_CONFIG_KEY);
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String column : raw.split(",")) {
            String trimmed = column.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}

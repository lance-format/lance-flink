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

package org.apache.flink.connector.lance.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.lance.PrimaryKeyPersistence;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Single source of truth for classifying a table option key as connector/runtime configuration
 * versus a user-defined TBLPROPERTY, and for deciding which Lance dataset config keys Flink owns.
 *
 * <h3>Why this exists</h3>
 * <p>Classification used to be a hard-coded key set inside {@code LanceCatalog}, maintained in
 * parallel with the {@link ConfigOption} constants declared by the factories. Nothing enforced
 * agreement between the two, and they had already drifted: {@code s3-virtual-hosted-style} and
 * {@code s3-allow-http} are declared by {@link LanceCatalogFactory} but were missing from the
 * hard-coded set, so they were classified as user TBLPROPERTIES and written into the Lance dataset
 * config. Deriving the set from the {@code ConfigOption} declarations removes that failure mode:
 * adding an option to a factory now classifies it correctly with no second edit.
 *
 * <h3>Ownership model for dataset config</h3>
 * <p>Flink shares the Lance dataset config namespace with sibling engines (Spark, Trino, Ray).
 * {@code ALTER TABLE ... RESET} must therefore never delete a key it does not own. Rather than
 * guessing which keys belong to others, this class defines what Flink owns and treats everything
 * else as foreign:
 *
 * <ul>
 *   <li>Keys written by this connector live under {@value #FLINK_NAMESPACE_PREFIX}.</li>
 *   <li>A key with no dot is an unnamespaced user TBLPROPERTY set through Flink DDL, which Flink
 *       also owns.</li>
 *   <li>Any other dotted key (for example {@code spark.stats.approx_count}) is foreign and is
 *       never removed.</li>
 * </ul>
 *
 * <p>The previous heuristic treated <em>every</em> dotted key as foreign. That was safe against
 * cross-engine deletion but also made it impossible to ever RESET a dotted property set through
 * Flink itself, since those are indistinguishable from foreign keys under that rule. Scoping
 * Flink-owned dotted keys to an explicit prefix restores RESET for the keys Flink actually wrote
 * while keeping the cross-engine guarantee.
 */
public final class LanceOptionRegistry {

    /** Prefix under which this connector stores its own dataset config keys. */
    public static final String FLINK_NAMESPACE_PREFIX = "flink.";

    /**
     * Prefixes reserved for connector configuration. Options under these prefixes are consumed by
     * the connector at runtime and are never persisted as user TBLPROPERTIES.
     */
    private static final List<String> RESERVED_PREFIXES =
            Collections.unmodifiableList(Arrays.asList(
                    "read.", "write.", "index.", "vector.", "hadoop."));

    /**
     * Exact connector option keys, derived from the {@link ConfigOption} constants declared by the
     * connector's factories rather than restated by hand.
     *
     * <p>{@code LanceDynamicTableFactory} contributes the table-level options and
     * {@code LanceCatalogFactory} the catalog-level ones (notably the storage credentials, which
     * may also appear in a table's option map). Both are enumerated so a new option added to
     * either factory is classified correctly without touching this class.
     */
    private static final Set<String> RESERVED_KEYS;

    static {
        Set<String> keys = new LinkedHashSet<>();
        // "connector" is supplied by the Flink planner itself, not by any factory's option set.
        keys.add("connector");
        // The factories are stateless, so instantiating them here is cheap and, more importantly,
        // reads the live option sets. Adding an option to either factory is picked up with no
        // corresponding edit in this class -- which is the entire point of the registry.
        LanceDynamicTableFactory tableFactory = new LanceDynamicTableFactory();
        collectKeys(keys, tableFactory.requiredOptions());
        collectKeys(keys, tableFactory.optionalOptions());
        LanceCatalogFactory catalogFactory = new LanceCatalogFactory();
        collectKeys(keys, catalogFactory.requiredOptions());
        collectKeys(keys, catalogFactory.optionalOptions());
        RESERVED_KEYS = Collections.unmodifiableSet(new HashSet<>(keys));
    }

    private LanceOptionRegistry() {
        // utility class
    }

    private static void collectKeys(Set<String> target, Set<ConfigOption<?>> options) {
        for (ConfigOption<?> option : options) {
            target.add(option.key());
        }
    }

    /**
     * Whether the key is connector/runtime configuration rather than a user TBLPROPERTY.
     */
    public static boolean isConnectorOption(String key) {
        if (key == null) {
            return false;
        }
        if (RESERVED_KEYS.contains(key)) {
            return true;
        }
        for (String prefix : RESERVED_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the key should be persisted to the Lance dataset config as a user TBLPROPERTY.
     */
    public static boolean isTblProperty(String key) {
        return key != null && !isConnectorOption(key);
    }

    /**
     * Whether Flink owns this Lance dataset config key and may therefore remove it on
     * {@code ALTER TABLE ... RESET}.
     *
     * <p>Internal connector metadata such as {@link PrimaryKeyPersistence#PK_CONFIG_KEY} lives
     * under the Flink namespace and is owned, but it is filtered out separately by the caller
     * because it is managed by the connector rather than by user DDL.
     */
    public static boolean isFlinkOwnedConfigKey(String key) {
        if (key == null) {
            return false;
        }
        if (key.startsWith(FLINK_NAMESPACE_PREFIX)) {
            return true;
        }
        // Unnamespaced keys can only have come from Flink DDL: other engines namespace their
        // metadata. Treating them as owned keeps RESET working for plain TBLPROPERTIES.
        return key.indexOf('.') < 0;
    }

    /**
     * Exposed for assertions in tests that verify the registry stays in sync with the factories.
     */
    static Set<String> reservedKeys() {
        return RESERVED_KEYS;
    }
}

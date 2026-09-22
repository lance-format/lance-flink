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

package org.apache.flink.connector.lance.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Central factory for the Arrow {@link RootAllocator}s used across the connector.
 *
 * <h3>Why this is centralized</h3>
 * <p>Every sink, source, catalog and index builder previously constructed
 * {@code new RootAllocator(Long.MAX_VALUE)} inline. In a TaskManager running several slots, each
 * with its own sinks and sources, that yields many independently unbounded off-heap arenas that
 * compete with Flink's own memory manager. There is no ceiling and no attribution: a single
 * oversized Arrow batch can exhaust off-heap memory and take neighbouring slots down with it, and
 * the resulting OOM names no owner.
 *
 * <p>Routing every allocation site through this factory makes the limit configurable in one place
 * and gives each allocator a name, so an {@code OutOfMemoryException} identifies the component
 * that overran.
 *
 * <h3>Compatibility</h3>
 * <p>The cap is opt-in. When {@code arrow.allocator-max-bytes} is unset the limit stays
 * {@link Long#MAX_VALUE}, preserving the previous behaviour exactly.
 */
public final class LanceAllocators {

    private static final Logger LOG = LoggerFactory.getLogger(LanceAllocators.class);

    /**
     * Option key bounding each Arrow allocator, in bytes. Unset means unlimited.
     *
     * <p>The bound is per allocator instance, not global: it caps the blast radius of one
     * component rather than the connector's total footprint.
     */
    public static final String ALLOCATOR_MAX_BYTES_KEY = "arrow.allocator-max-bytes";

    /** Default limit, matching the historical unbounded behaviour. */
    public static final long UNLIMITED = Long.MAX_VALUE;

    private LanceAllocators() {
        // utility class
    }

    /**
     * Create a named root allocator with no limit.
     *
     * <p>For call sites that have no access to user configuration. The name still improves
     * diagnosability of allocator-related failures.
     */
    public static BufferAllocator create(String name) {
        return create(name, UNLIMITED);
    }

    /**
     * Create a named root allocator bounded by {@code maxBytes}.
     *
     * <p>{@code RootAllocator(long)} takes the limit directly; {@link Long#MAX_VALUE} reproduces
     * the previous unbounded allocator exactly.
     */
    public static BufferAllocator create(String name, long maxBytes) {
        long limit = maxBytes > 0 ? maxBytes : UNLIMITED;
        if (limit != UNLIMITED) {
            LOG.debug("Creating Arrow allocator '{}' limited to {} bytes", name, limit);
        }
        return new RootAllocator(limit);
    }

    /**
     * Create a named root allocator, reading the limit from a connector option map.
     *
     * @param options table or catalog options; may be {@code null}
     */
    public static BufferAllocator create(String name, Map<String, String> options) {
        return create(name, resolveMaxBytes(options));
    }

    /**
     * Resolve the configured allocator limit, falling back to unlimited.
     *
     * <p>An unparseable or non-positive value is reported and ignored rather than failing table
     * creation: a malformed memory hint should not prevent a job from starting, and the safe
     * fallback is the pre-existing behaviour.
     */
    public static long resolveMaxBytes(Map<String, String> options) {
        if (options == null) {
            return UNLIMITED;
        }
        String raw = options.get(ALLOCATOR_MAX_BYTES_KEY);
        if (raw == null || raw.trim().isEmpty()) {
            return UNLIMITED;
        }
        try {
            long parsed = Long.parseLong(raw.trim());
            if (parsed <= 0) {
                LOG.warn("Ignoring non-positive {}={}; using unlimited allocator",
                        ALLOCATOR_MAX_BYTES_KEY, raw);
                return UNLIMITED;
            }
            return parsed;
        } catch (NumberFormatException e) {
            LOG.warn("Ignoring unparseable {}={}; using unlimited allocator",
                    ALLOCATOR_MAX_BYTES_KEY, raw);
            return UNLIMITED;
        }
    }
}

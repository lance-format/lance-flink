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

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.arrow.memory.BufferAllocator;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Helper that opens a {@link Dataset} honoring the {@code read.version} / {@code read.as-of-timestamp}
 * time-travel options declared in {@link LanceOptions} (issue #5).
 *
 * <p>Resolution order:
 * <ol>
 *   <li>If {@link LanceOptions#getReadVersion()} is set → open at that version.</li>
 *   <li>Else if {@link LanceOptions#getReadAsOfTimestamp()} is set → list all versions, pick the newest
 *       whose {@code getDataTime()} is &le; the parsed timestamp, open at that version.</li>
 *   <li>Otherwise → open latest (backward compatible with pre-time-travel behavior).</li>
 * </ol>
 *
 * <p>The timestamp accepts any string parsable by {@link Instant#parse(CharSequence)},
 * {@link OffsetDateTime#parse(CharSequence)}, or {@link ZonedDateTime#parse(CharSequence)}.
 * If the input is a bare {@code yyyy-MM-ddTHH:mm:ss}, it is assumed to be UTC.
 */
public final class LanceOpener {

    private static final Logger LOG = LoggerFactory.getLogger(LanceOpener.class);

    private LanceOpener() {}

    /** Open a dataset honoring the time-travel options declared in {@code options}. */
    public static Dataset open(String datasetPath, BufferAllocator allocator, LanceOptions options) {
        return open(datasetPath, allocator, options, null);
    }

    /**
     * Open a dataset honoring the time-travel options declared in {@code options}.
     *
     * @param flinkConf Flink runtime configuration used to inject {@code flink.hadoop.*} (such as
     *                  {@code flink.hadoop.tbdsfs.meta}) into the Hadoop {@code Configuration},
     *                  so that Hadoop-compatible filesystems like tbdsfs/hdfs can initialize
     *                  correctly. May be {@code null}.
     */
    public static Dataset open(String datasetPath, BufferAllocator allocator, LanceOptions options,
                               org.apache.flink.configuration.Configuration flinkConf) {
        // Plan B: for Hadoop-compatible schemes such as tbdsfs/hdfs, first cache to a local
        // directory via the Hadoop FileSystem.
        org.apache.hadoop.conf.Configuration hadoopConf =
                LanceHadoopPathResolver.buildHadoopConfigurationFromFlink(flinkConf);
        // Inject hadoop.*-prefixed configs from the table DDL WITH clause (such as tbdsfs.meta)
        // at the highest priority, to work around missing core-site.xml on some cluster nodes.
        if (options != null && options.getHadoopConfig() != null) {
            for (java.util.Map.Entry<String, String> e : options.getHadoopConfig().entrySet()) {
                hadoopConf.set(e.getKey(), e.getValue());
                LOG.info("Injected Hadoop conf from Lance options: {} = {}", e.getKey(), e.getValue());
            }
        }
        String resolvedPath = LanceHadoopPathResolver.resolveForRead(datasetPath, hadoopConf, null);

        Long explicitVersion = options.getReadVersion();
        String asOf = options.getReadAsOfTimestamp();

        if (explicitVersion != null) {
            LOG.info("Opening Lance dataset {} at version {} (read.version)", resolvedPath, explicitVersion);
            return openAtVersion(resolvedPath, allocator, explicitVersion);
        }

        if (asOf != null && !asOf.isEmpty()) {
            long resolved = resolveVersionForTimestamp(resolvedPath, allocator, asOf);
            LOG.info("Opening Lance dataset {} at version {} (resolved from read.as-of-timestamp={})",
                    resolvedPath, resolved, asOf);
            return openAtVersion(resolvedPath, allocator, resolved);
        }

        LOG.debug("Opening Lance dataset {} at latest version (no time-travel options set)", resolvedPath);
        return Dataset.open(resolvedPath, allocator);
    }

    private static Dataset openAtVersion(String datasetPath, BufferAllocator allocator, long version) {
        ReadOptions readOptions = new ReadOptions.Builder().setVersion(version).build();
        return Dataset.open(allocator, datasetPath, readOptions);
    }

    /**
     * Resolve {@code asOfTimestamp} to the newest version whose data time is &le; the timestamp.
     * Throws {@link IllegalArgumentException} if the timestamp cannot be parsed or predates all versions.
     */
    private static long resolveVersionForTimestamp(String datasetPath, BufferAllocator allocator,
                                                   String asOfTimestamp) {
        ZonedDateTime target = parseTimestamp(asOfTimestamp);
        // Open latest just to enumerate versions; closing right after.
        try (Dataset ds = Dataset.open(datasetPath, allocator)) {
            List<Version> versions = ds.listVersions();
            long chosen = -1L;
            ZonedDateTime chosenTime = null;
            for (Version v : versions) {
                ZonedDateTime vt = v.getDataTime();
                if (vt == null) continue;
                if (!vt.isAfter(target)) {
                    if (chosenTime == null || vt.isAfter(chosenTime)) {
                        chosen = v.getId();
                        chosenTime = vt;
                    }
                }
            }
            if (chosen < 0) {
                throw new IllegalArgumentException(
                        "read.as-of-timestamp=" + asOfTimestamp
                                + " predates the oldest version of dataset " + datasetPath);
            }
            return chosen;
        }
    }

    /** Parse a variety of ISO-8601 shapes into {@link ZonedDateTime} in UTC. */
    private static ZonedDateTime parseTimestamp(String s) {
        try {
            return Instant.parse(s).atZone(ZoneOffset.UTC);
        } catch (DateTimeParseException ignore) {
            // fall through
        }
        try {
            return OffsetDateTime.parse(s).toZonedDateTime();
        } catch (DateTimeParseException ignore) {
            // fall through
        }
        try {
            return ZonedDateTime.parse(s);
        } catch (DateTimeParseException ignore) {
            // fall through
        }
        // Bare local datetime → assume UTC
        try {
            return ZonedDateTime.parse(s + "Z", java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "read.as-of-timestamp=" + s
                            + " is not a valid ISO-8601 timestamp (e.g. 2026-07-01T00:00:00Z)", e);
        }
    }
}

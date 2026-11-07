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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Plan B: a Hadoop FileSystem front cache layer.
 *
 * <p>Lance reads storage directly through lance-core (Rust), whose {@code object_store} only
 * supports a hard-coded set of schemes ({@code file/s3/gs/az/abfss/oss/cos/hf/memory}). For
 * Hadoop-ecosystem schemes such as {@code tbdsfs://} / {@code hdfs://}, Lance cannot recognize
 * them.
 *
 * <p>The responsibility of this utility: before calling {@code Dataset.open(uri)}, if {@code uri}
 * uses a Hadoop-compatible scheme (and is not in Lance's native supported list), then:
 * <ol>
 *   <li>open the source directory via the Hadoop {@link FileSystem};</li>
 *   <li>recursively sync the entire lance dataset directory to a local temporary cache directory;</li>
 *   <li>return the local {@code file:///...} path to Lance.</li>
 * </ol>
 *
 * <p><b>Prerequisite</b>: the Hadoop {@code FileSystem} implementation for the corresponding
 * scheme must be on the classpath (for example, tbdsfs needs {@code tbdsfs-hadoop-*.jar}). On
 * TBDS clusters these jars are provided under {@code /usr/local/service/flink/lib/}, so
 * lance-flink itself does not need to bundle them.
 *
 * <p><b>Limitation</b>: the current implementation performs a full <b>read-time copy</b> of the
 * entire directory, which works for small/medium-sized lance datasets; for very large datasets it
 * should evolve into range-read or incremental sync strategies.
 *
 * <p>This class is thread-safe; concurrent resolve requests for the same source path are
 * serialized into a single download.
 */
public final class LanceHadoopPathResolver {

    private static final Logger LOG = LoggerFactory.getLogger(LanceHadoopPathResolver.class);

    /** Schemes supported by Lance's native object_store; these are passed through directly. */
    private static final Set<String> LANCE_NATIVE_SCHEMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "file", "s3", "s3+ddb", "gs", "az", "abfss", "oss", "cos",
                    "hf", "memory", "file+uring", "shared-memory", "file-object-store"
            )));

    /** Per-process map of cached source URI → local path; also doubles as the lock monitor. */
    private static final ConcurrentHashMap<String, Object> CACHE_LOCKS = new ConcurrentHashMap<>();

    /**
     * The sentinel version number for Lance's "latest" marker ({@code u64::MAX - 1}). Lance uses it
     * under the {@code _versions/} directory to alias the "latest version" manifest. This file is
     * not real data; Lance can determine the latest version by scanning for the max version number
     * under {@code _versions/}, so a read failure on it can be safely skipped. Some tbdsfs backends
     * store this sentinel manifest incorrectly (returning input/output error on read).
     */
    private static final String LATEST_MANIFEST_SENTINEL = "18446744073709551614";

    private LanceHadoopPathResolver() {}

    /**
     * Resolves a URI, syncing the remote lance dataset directory to a local cache when needed, and
     * returns a path that can be passed directly to {@code Dataset.open()}.
     *
     * @param sourceUri      the user-configured lance dataset URI, which may be
     *                       {@code /local/path}, {@code file:///...}, {@code s3://...},
     *                       {@code tbdsfs://...}, {@code hdfs://...}, etc.
     * @param hadoopConf    Hadoop configuration (may be {@code null}; a default
     *                      {@link Configuration} is used then)
     * @param localCacheRoot local cache root directory (may be {@code null}; defaults to
     *                       {@code java.io.tmpdir/lance-hadoop-cache})
     * @return a path that can be passed directly to lance-core (local path or the original path)
     */
    public static String resolveForRead(String sourceUri, Configuration hadoopConf,
                                        String localCacheRoot) {
        if (sourceUri == null || sourceUri.isEmpty()) {
            return sourceUri;
        }
        // Normalize the URI: some upstreams (e.g. Flink Table planner / Path deserialization)
        // collapse "scheme://authority/path" into "scheme:/authority/path" (single slash), making
        // URI.getAuthority() return null and losing the name tbdsfs requires. Here we restore
        // "scheme:/xxx" (but not "scheme:///") back to "scheme://xxx".
        sourceUri = normalizeUri(sourceUri);

        String scheme = extractScheme(sourceUri);
        if (scheme == null || LANCE_NATIVE_SCHEMES.contains(scheme)) {
            // No scheme (treated as local path) or a scheme natively supported by Lance: pass through.
            return sourceUri;
        }

        // Needs to go through the Hadoop FS cache.
        Configuration conf = buildHadoopConfiguration(hadoopConf);
        applySchemeSpecificDefaults(conf, sourceUri, scheme);
        java.nio.file.Path cacheRoot = resolveCacheRoot(localCacheRoot);
        java.nio.file.Path targetDir = cacheRoot.resolve(sanitize(sourceUri));

        // Serialize concurrent access: download the same sourceUri only once.
        Object lock = CACHE_LOCKS.computeIfAbsent(sourceUri, k -> new Object());
        synchronized (lock) {
            try {
                if (isCacheReady(targetDir)) {
                    LOG.info("Lance dataset {} already cached at {}, reuse local copy",
                            sourceUri, targetDir);
                } else {
                    LOG.info("Lance dataset {} not in native supported schemes ({}); "
                                    + "downloading via Hadoop FileSystem to {}",
                            sourceUri, scheme, targetDir);
                    downloadDirectory(new Path(sourceUri), targetDir, conf);
                }
                return "file://" + targetDir.toAbsolutePath();
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "Failed to cache Lance dataset from " + sourceUri
                                + " to local " + targetDir
                                + ". Ensure the corresponding Hadoop FileSystem impl "
                                + "(e.g. tbdsfs-hadoop-*.jar) is available in classpath, "
                                + "and Kerberos credentials are valid.",
                        e);
            }
        }
    }

    /**
     * Builds a Hadoop {@link Configuration}:
     * <ol>
     *   <li>If {@code userConf} is provided, use it as the base; otherwise create a new one (which
     *       auto-loads {@code core-site.xml} from the classpath).</li>
     *   <li>Scan system properties prefixed with {@code lance.hadoop.}, strip the prefix and inject
     *       them into the Configuration. For example
     *       {@code -Dlance.hadoop.fs.tbdsfs.impl=io.tbdsfs.TbdsFileSystem} maps to the Hadoop
     *       config {@code fs.tbdsfs.impl}.</li>
     *   <li>Scan environment variables prefixed with {@code LANCE_HADOOP_}, using underscore as a
     *       separator. For example {@code LANCE_HADOOP_TBDSFS_META=zk://host:port} maps to
     *       {@code tbdsfs.meta}.</li>
     * </ol>
     *
     * <p>In a Flink TaskManager environment, YARN containers may not see the host's
     * {@code core-site.xml}, so the two mechanisms above act as fallback configuration injection
     * channels.
     */
    static Configuration buildHadoopConfiguration(Configuration userConf) {
        Configuration conf = userConf != null ? userConf : new Configuration();

        // Explicitly load core-site.xml / hdfs-site.xml. Inside a Flink YARN TaskManager container,
        // Flink's child-first classloader isolation means new Configuration() may not load the
        // host's core-site.xml through the context classloader, causing tbdsfs.meta etc. to be
        // missing (tbdsfs's Go library then fails with "invalid uri" and fatals). Load them here
        // from standard paths as a fallback.
        loadHadoopSiteXmls(conf);

        // 1. Inject system properties lance.hadoop.* -> hadoop conf key
        java.util.Properties props = System.getProperties();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("lance.hadoop.")) {
                String hadoopKey = key.substring("lance.hadoop.".length());
                String value = props.getProperty(key);
                conf.set(hadoopKey, value);
                LOG.info("Injected Hadoop conf from system property: {} = {}", hadoopKey, value);
            }
        }

        // 2. Inject environment variables LANCE_HADOOP_XXX -> hadoop conf key
        java.util.Map<String, String> env = System.getenv();
        for (java.util.Map.Entry<String, String> e : env.entrySet()) {
            if (e.getKey().startsWith("LANCE_HADOOP_")) {
                String hadoopKey = e.getKey().substring("LANCE_HADOOP_".length())
                        .toLowerCase(Locale.ROOT).replace('_', '.');
                String value = e.getValue();
                conf.set(hadoopKey, value);
                LOG.info("Injected Hadoop conf from environment variable: {} = {}", hadoopKey, value);
            }
        }

        return conf;
    }

    /**
     * Builds a Hadoop {@link Configuration} from Flink's global configuration.
     *
     * <p>Configurations set via {@code SET 'flink.hadoop.xxx' = 'yyy'} in Flink SQL enter Flink's
     * {@code Configuration} with the {@code flink.hadoop.} prefix. Here we strip that prefix and
     * inject them into the Hadoop {@code Configuration} (for example {@code flink.hadoop.tbdsfs.meta}
     * → {@code tbdsfs.meta}).
     *
     * <p>This solves the problem where, inside a YARN TaskManager container,
     * {@code new Configuration()} cannot load the host's {@code core-site.xml} (and therefore
     * {@code tbdsfs.meta}) due to classloader isolation — tbdsfs's Go library, when {@code meta} is
     * empty, falls back to treating the name as a URI, reporting {@code invalid uri: /internal} and
     * directly fataling out.
     *
     * @param flinkConf Flink runtime configuration (may be {@code null})
     * @return a Hadoop {@code Configuration} with {@code flink.hadoop.*} settings injected
     */
    public static Configuration buildHadoopConfigurationFromFlink(
            org.apache.flink.configuration.Configuration flinkConf) {
        Configuration conf = new Configuration();
        if (flinkConf == null) {
            return conf;
        }
        for (java.util.Map.Entry<String, String> e : flinkConf.toMap().entrySet()) {
            String key = e.getKey();
            if (key != null && key.startsWith("flink.hadoop.")) {
                String hadoopKey = key.substring("flink.hadoop.".length());
                conf.set(hadoopKey, e.getValue());
                LOG.info("Injected Hadoop conf from Flink config: {} = {}", hadoopKey, e.getValue());
            }
        }
        return conf;
    }

    /**
     * Applies fallback defaults for a specific scheme:
     * <ul>
     *   <li>{@code tbdsfs://<authority>/...}: tbdsfs's {@code TbdsFileSystemImpl.initialize}
     *       requires the {@code tbdsfs.name} config to exist. If the user has not set it explicitly
     *       and the URI's authority is non-empty, the authority is automatically injected as
     *       {@code tbdsfs.name}. It also ensures {@code fs.tbdsfs.impl} exists (defaulting to
     *       {@code io.tbdsfs.TbdsFileSystem}).</li>
     * </ul>
     *
     * <p>This method only fills in missing values; it never overrides values the user has already
     * set explicitly.
     */
    static void applySchemeSpecificDefaults(Configuration conf, String sourceUri, String scheme) {
        if (conf == null || scheme == null) return;
        if (!"tbdsfs".equals(scheme)) return;

        // 1. Ensure fs.tbdsfs.impl exists.
        if (conf.get("fs.tbdsfs.impl") == null) {
            conf.set("fs.tbdsfs.impl", "io.tbdsfs.TbdsFileSystem");
            LOG.info("Applied default fs.tbdsfs.impl = io.tbdsfs.TbdsFileSystem");
        }

        // 2. If tbdsfs.name is not configured, derive it from the URI authority.
        if (conf.get("tbdsfs.name") == null) {
            try {
                URI u = new URI(sourceUri);
                String authority = u.getAuthority();
                if (authority != null && !authority.isEmpty()) {
                    conf.set("tbdsfs.name", authority);
                    LOG.info("Applied default tbdsfs.name = {} (from URI authority)", authority);
                }
            } catch (URISyntaxException ignore) {
                // fall through; tbdsfs will raise its own error if truly missing
            }
        }
    }

    /**
     * Normalizes a URI: if the input is of the form {@code scheme:/authority/path} (single slash,
     * no clear separator between authority and path), it is converted to
     * {@code scheme://authority/path} (double slash). A common trigger is Flink Table
     * deserializing a URI via {@code new Path(str)}, which drops a slash. Idempotent: URIs already
     * of the form {@code scheme://...} or {@code scheme:///...} (no authority) are left unchanged.
     */
    static String normalizeUri(String uri) {
        if (uri == null) return null;
        int colon = uri.indexOf(':');
        if (colon <= 0 || colon >= uri.length() - 1) return uri;
        String rest = uri.substring(colon + 1);
        // If it is already "//..." (including "///..." with empty authority), keep as-is.
        if (rest.startsWith("//")) return uri;
        // Only handle "scheme:/xxx" where the second char is not '/' (otherwise it is already double slash).
        if (rest.startsWith("/") && !rest.startsWith("//")) {
            String scheme = uri.substring(0, colon);
            return scheme + ":/" + rest;   // turn "scheme:/xxx" into "scheme://xxx"
        }
        return uri;
    }

    /** Extracts the scheme. Returns the lower-cased scheme, or {@code null} if there is none. */
    static String extractScheme(String uri) {
        try {
            URI u = new URI(uri);
            String s = u.getScheme();
            return s == null ? null : s.toLowerCase(Locale.ROOT);
        } catch (URISyntaxException e) {
            // Non-standard URI (e.g. a Windows path C:\), treat as a local path.
            return null;
        }
    }

    static boolean isNativeSupportedScheme(String scheme) {
        return scheme != null && LANCE_NATIVE_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT));
    }

    /**
     * Explicitly loads Hadoop's {@code core-site.xml} / {@code hdfs-site.xml}. Used as a fallback
     * for the case where, inside a Flink YARN container, {@code new Configuration()} cannot load
     * the host's site files due to classloader isolation.
     */
    private static void loadHadoopSiteXmls(Configuration conf) {
        java.nio.file.Path confDir = resolveHadoopConfDir();
        if (confDir == null) {
            return;
        }
        addSiteXmlIfExists(conf, confDir.resolve("core-site.xml"));
        addSiteXmlIfExists(conf, confDir.resolve("hdfs-site.xml"));
    }

    private static void addSiteXmlIfExists(Configuration conf, java.nio.file.Path siteXml) {
        if (Files.isRegularFile(siteXml)) {
            conf.addResource(new Path(siteXml.toUri()));
            LOG.info("Explicitly loaded Hadoop site config: {}", siteXml);
        }
    }

    /** Probes the Hadoop configuration directory: HADOOP_CONF_DIR → system property → TBDS standard path. */
    private static java.nio.file.Path resolveHadoopConfDir() {
        String env = System.getenv("HADOOP_CONF_DIR");
        if (env != null && !env.isEmpty()) {
            return java.nio.file.Paths.get(env);
        }
        String prop = System.getProperty("hadoop.conf.dir");
        if (prop != null && !prop.isEmpty()) {
            return java.nio.file.Paths.get(prop);
        }
        java.nio.file.Path standard = java.nio.file.Paths.get("/usr/local/service/hadoop/etc/hadoop");
        if (Files.isDirectory(standard)) {
            return standard;
        }
        return null;
    }

    private static java.nio.file.Path resolveCacheRoot(String userSpecified) {
        if (userSpecified != null && !userSpecified.isEmpty()) {
            return java.nio.file.Paths.get(userSpecified);
        }
        String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
        return java.nio.file.Paths.get(tmpDir, "lance-hadoop-cache");
    }

    /** Converts a URI into a safe string usable as a filesystem directory name. */
    private static String sanitize(String uri) {
        return uri.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /** Whether the cache directory is ready (has a {@code _versions/} subdirectory, the Lance dataset marker). */
    private static boolean isCacheReady(java.nio.file.Path targetDir) {
        if (!Files.isDirectory(targetDir)) return false;
        java.nio.file.Path versionsDir = targetDir.resolve("_versions");
        return Files.isDirectory(versionsDir);
    }

    /**
     * Recursively syncs all files under {@code srcDir} to {@code destDir} (a local directory),
     * preserving the relative directory structure.
     */
    private static void downloadDirectory(Path srcDir, java.nio.file.Path destDir,
                                          Configuration conf) throws IOException {
        FileSystem fs = srcDir.getFileSystem(conf);
        LOG.info("Resolved Hadoop FileSystem for {}: uri={}, impl={}",
                srcDir, fs.getUri(), fs.getClass().getName());
        FileStatus rootStatus = fs.getFileStatus(srcDir);
        if (!rootStatus.isDirectory()) {
            throw new IOException("Lance dataset path is not a directory: " + srcDir);
        }
        Files.createDirectories(destDir);

        long fileCount = 0L;
        long byteCount = 0L;
        int skippedSentinelFiles = 0;
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(srcDir, true);
        while (it.hasNext()) {
            LocatedFileStatus st = it.next();
            if (st.isDirectory()) continue;
            Path srcFile = st.getPath();
            String rel = relativize(srcDir, srcFile);
            java.nio.file.Path dst = destDir.resolve(rel);
            Files.createDirectories(dst.getParent());
            // Use copyToLocalFile: deleteSource=false, useRawLocalFileSystem=true to avoid checksum validation.
            try {
                fs.copyToLocalFile(false, srcFile, new Path(dst.toUri()), true);
                fileCount++;
                byteCount += st.getLen();
            } catch (IOException e) {
                // The latest sentinel manifest (u64::MAX-1) is only an alias of the latest version;
                // being corrupt/unreadable does not stop Lance from determining the latest by scanning
                // the _versions directory, so it is safely skipped.
                if (isLatestSentinelManifest(rel)) {
                    LOG.warn("Skipping unreadable latest-sentinel manifest {} (tbdsfs read error: {})",
                            srcFile, e.getMessage());
                    skippedSentinelFiles++;
                } else {
                    throw e;
                }
            }
        }
        LOG.info("Downloaded lance dataset from {} to {}: {} files, {} bytes (skipped {} unreadable latest-sentinel manifests)",
                srcDir, destDir, fileCount, byteCount, skippedSentinelFiles);
    }

    /**
     * Determines whether the relative path {@code rel} points to Lance's latest sentinel manifest
     * (i.e. {@code _versions/18446744073709551614.manifest}).
     */
    private static boolean isLatestSentinelManifest(String rel) {
        if (rel == null) return false;
        int slash = rel.lastIndexOf('/');
        String name = slash >= 0 ? rel.substring(slash + 1) : rel;
        return name.equals(LATEST_MANIFEST_SENTINEL + ".manifest");
    }

    /**
     * Computes the path of {@code srcFile} relative to {@code srcDir}, always using '/' as the
     * separator.
     */
    private static String relativize(Path srcDir, Path srcFile) {
        String base = srcDir.toUri().getPath();
        String full = srcFile.toUri().getPath();
        if (!base.endsWith("/")) base = base + "/";
        if (full.startsWith(base)) {
            return full.substring(base.length());
        }
        // Fallback: take the file name directly.
        return srcFile.getName();
    }
}

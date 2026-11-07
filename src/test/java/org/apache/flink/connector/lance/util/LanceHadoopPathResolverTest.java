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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LanceHadoopPathResolver}.
 *
 * <p>Focus areas:
 * <ul>
 *   <li>Native Lance schemes should be passed through unchanged.</li>
 *   <li>Local file URIs should be passed through unchanged.</li>
 *   <li>Hadoop-family schemes should trigger download to local cache and return {@code file://} path.</li>
 *   <li>Scheme detection utility should be case-insensitive and null-safe.</li>
 * </ul>
 */
class LanceHadoopPathResolverTest {

    @TempDir
    Path tempDir;

    @Test
    void extractSchemeReturnsLowercaseScheme() {
        assertThat(LanceHadoopPathResolver.extractScheme("S3://bucket/foo")).isEqualTo("s3");
        assertThat(LanceHadoopPathResolver.extractScheme("TBDSFS://svc/foo")).isEqualTo("tbdsfs");
        assertThat(LanceHadoopPathResolver.extractScheme("hdfs://nn/p")).isEqualTo("hdfs");
        assertThat(LanceHadoopPathResolver.extractScheme("file:///tmp/x")).isEqualTo("file");
    }

    @Test
    void extractSchemeReturnsNullForBarePath() {
        assertThat(LanceHadoopPathResolver.extractScheme("/tmp/foo/bar")).isNull();
        assertThat(LanceHadoopPathResolver.extractScheme("relative/path")).isNull();
    }

    @Test
    void normalizeUriRestoresMissingSlashInAuthority() {
        // Case 1: Flink Path deserialization turns "tbdsfs://internal/x" into
        // "tbdsfs:/internal/x"; normalizeUri should restore it.
        assertThat(LanceHadoopPathResolver.normalizeUri("tbdsfs:/internal/lance_poc/db1"))
                .isEqualTo("tbdsfs://internal/lance_poc/db1");
        // Case 2: hdfs://ns1/path behaves the same.
        assertThat(LanceHadoopPathResolver.normalizeUri("hdfs:/ns1/path"))
                .isEqualTo("hdfs://ns1/path");
    }

    @Test
    void normalizeUriIsIdempotent() {
        // Already has double slash: unchanged.
        assertThat(LanceHadoopPathResolver.normalizeUri("tbdsfs://internal/x"))
                .isEqualTo("tbdsfs://internal/x");
        // "scheme:///" with empty authority is also unchanged.
        assertThat(LanceHadoopPathResolver.normalizeUri("file:///tmp/x"))
                .isEqualTo("file:///tmp/x");
        // Local paths are unchanged.
        assertThat(LanceHadoopPathResolver.normalizeUri("/tmp/x")).isEqualTo("/tmp/x");
        assertThat(LanceHadoopPathResolver.normalizeUri(null)).isNull();
    }

    @Test
    void isNativeSupportedSchemeRecognizesLanceSchemes() {
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("s3")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("S3")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("file")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("cos")).isTrue();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("tbdsfs")).isFalse();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme("hdfs")).isFalse();
        assertThat(LanceHadoopPathResolver.isNativeSupportedScheme(null)).isFalse();
    }

    @Test
    void resolveForReadPassesThroughNullAndEmpty() {
        assertThat(LanceHadoopPathResolver.resolveForRead(null, null, null)).isNull();
        assertThat(LanceHadoopPathResolver.resolveForRead("", null, null)).isEmpty();
    }

    @Test
    void resolveForReadPassesThroughLocalPath() {
        String local = "/tmp/lance/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(local, null, null);
        assertThat(resolved).isEqualTo(local);
    }

    @Test
    void resolveForReadPassesThroughFileUri() {
        String fileUri = "file:///tmp/lance/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(fileUri, null, null);
        assertThat(resolved).isEqualTo(fileUri);
    }

    @Test
    void resolveForReadPassesThroughS3Uri() {
        String s3 = "s3://bucket/prefix/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(s3, null, null);
        assertThat(resolved).isEqualTo(s3);
    }

    @Test
    void resolveForReadPassesThroughCosUri() {
        String cos = "cos://bucket/prefix/dataset.lance";
        String resolved = LanceHadoopPathResolver.resolveForRead(cos, null, null);
        assertThat(resolved).isEqualTo(cos);
    }

    /**
     * Uses the custom {@code mytestfs://} scheme (not natively supported by Lance) and
     * redirects it to {@link org.apache.hadoop.fs.LocalFileSystem} via Hadoop configuration,
     * exercising the full Hadoop FS download path without depending on a real HDFS.
     */
    @Test
    void resolveForReadDownloadsFromHadoopFileSystem() throws IOException {
        // 1. Build a "source" lance dataset directory (simulated on local disk).
        Path srcDir = tempDir.resolve("src_dataset.lance");
        Path versionsDir = srcDir.resolve("_versions");
        Path dataDir = srcDir.resolve("data");
        Files.createDirectories(versionsDir);
        Files.createDirectories(dataDir);
        Files.write(versionsDir.resolve("1.manifest"), new byte[] {1, 2, 3, 4});
        Files.write(dataDir.resolve("part-0.lance"), "hello".getBytes());
        Files.write(srcDir.resolve("_latest.manifest"), new byte[] {9});

        // 2. Configure: redirect the custom mytestfs scheme to Hadoop LocalFileSystem.
        Configuration conf = new Configuration(false);
        conf.set("fs.mytestfs.impl", LocalFsWithMytestfsScheme.class.getName());

        // 3. Access the source directory via the mytestfs:// prefix to trigger the
        // Hadoop FS download path.
        String srcUri = "mytestfs://" + srcDir.toAbsolutePath();
        Path cacheRoot = tempDir.resolve("cache");

        String resolved = LanceHadoopPathResolver.resolveForRead(
                srcUri, conf, cacheRoot.toAbsolutePath().toString());

        // 4. Verify the result is a local file:// path containing the expected files.
        assertThat(resolved).startsWith("file://");
        Path resolvedDir = Path.of(resolved.substring("file://".length()));
        assertThat(Files.isDirectory(resolvedDir)).isTrue();
        assertThat(Files.isDirectory(resolvedDir.resolve("_versions"))).isTrue();
        assertThat(Files.isRegularFile(resolvedDir.resolve("_versions/1.manifest"))).isTrue();
        assertThat(Files.isRegularFile(resolvedDir.resolve("data/part-0.lance"))).isTrue();
        assertThat(Files.readString(resolvedDir.resolve("data/part-0.lance"))).isEqualTo("hello");
    }

    /**
     * Resolving the same URI a second time should hit the cache without re-downloading.
     */
    @Test
    void resolveForReadReusesCacheOnSecondCall() throws IOException {
        Path srcDir = tempDir.resolve("cached_dataset.lance");
        Path versionsDir = srcDir.resolve("_versions");
        Files.createDirectories(versionsDir);
        Files.write(versionsDir.resolve("1.manifest"), new byte[] {1});

        Configuration conf = new Configuration(false);
        conf.set("fs.mytestfs.impl", LocalFsWithMytestfsScheme.class.getName());

        String srcUri = "mytestfs://" + srcDir.toAbsolutePath();
        Path cacheRoot = tempDir.resolve("cache2");

        String first = LanceHadoopPathResolver.resolveForRead(
                srcUri, conf, cacheRoot.toAbsolutePath().toString());
        // Modify a file in the cached directory to simulate an existing cache.
        Path firstDir = Path.of(first.substring("file://".length()));
        Path sentinel = firstDir.resolve("_versions/1.manifest");
        Files.write(sentinel, new byte[] {42});

        String second = LanceHadoopPathResolver.resolveForRead(
                srcUri, conf, cacheRoot.toAbsolutePath().toString());
        // The second resolve should return the same directory, and the sentinel must not
        // be overwritten by a re-download.
        assertThat(second).isEqualTo(first);
        assertThat(Files.readAllBytes(sentinel)).containsExactly(42);
    }

    /**
     * Wraps Hadoop {@link org.apache.hadoop.fs.LocalFileSystem} as the {@code mytestfs://}
     * scheme for test purposes. It only overrides {@link #getScheme()} and rewrites incoming
     * {@code mytestfs://<abs-path>} URIs into {@code file:/<abs-path>} (accepted by the local
     * filesystem) before forwarding to LocalFileSystem.
     */
    public static final class LocalFsWithMytestfsScheme extends org.apache.hadoop.fs.LocalFileSystem {
        @Override
        public String getScheme() {
            return "mytestfs";
        }

        @Override
        public java.net.URI getUri() {
            return java.net.URI.create("mytestfs:///");
        }

        @Override
        public void initialize(java.net.URI name, Configuration conf) throws IOException {
            super.initialize(java.net.URI.create("file:///"), conf);
        }

        @Override
        protected void checkPath(org.apache.hadoop.fs.Path path) {
            // Accept any scheme (mytestfs or file).
        }

        @Override
        public org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path path) {
            return toLocal(path);
        }

        @Override
        public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path f)
                throws IOException {
            return super.getFileStatus(toLocal(f));
        }

        @Override
        public org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> listFiles(
                org.apache.hadoop.fs.Path f, boolean recursive) throws IOException {
            return super.listFiles(toLocal(f), recursive);
        }

        @Override
        public void copyToLocalFile(boolean delSrc, org.apache.hadoop.fs.Path src,
                                    org.apache.hadoop.fs.Path dst, boolean useRawLocalFileSystem)
                throws IOException {
            super.copyToLocalFile(delSrc, toLocal(src), dst, useRawLocalFileSystem);
        }

        private static org.apache.hadoop.fs.Path toLocal(org.apache.hadoop.fs.Path p) {
            java.net.URI u = p.toUri();
            String path = u.getPath();
            if (u.getAuthority() != null && !u.getAuthority().isEmpty()) {
                // mytestfs://<abs-path> parses the first segment of the absolute path as authority.
                path = "/" + u.getAuthority() + (path == null ? "" : path);
            }
            return new org.apache.hadoop.fs.Path("file://" + path);
        }
    }
}

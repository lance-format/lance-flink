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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Checks the shaded jar the build produces, rather than the classes the other tests run against.
 *
 * <p>The rest of the suite runs on {@code target/classes}, where Arrow is not relocated. That is
 * deliberate: the tests are compiled against the plain Arrow packages. It does mean nothing else
 * exercises the jar users actually deploy, so this case loads that jar in an isolated
 * classloader and asserts the relocation held.
 *
 * <p>The jar path comes from system properties set by failsafe. When they are absent, as in an
 * IDE run, the case is skipped rather than failed. A missing jar when the build did pass the
 * properties is a failure, not a skip -- shade runs at {@code package}, before this phase.
 */
@DisplayName("Shaded Jar Relocation Tests")
class LanceShadedJarITCase {

    private static final String SHADED_ARROW_SCHEMA =
            "org.apache.flink.connector.lance.shaded.arrow.vector.types.pojo.Schema";

    private static File shadedJar() {
        String dir = System.getProperty("lance.shaded.jar.dir");
        String name = System.getProperty("lance.shaded.jar.name");
        assumeTrue(dir != null && name != null, "shaded jar location not provided by the build");
        File jar = new File(dir, name);
        assertThat(jar).as("the build should have produced the shaded jar by now").isFile();
        return jar;
    }

    private static ShadedJarLoader isolatedLoader(File jar) throws Exception {
        return new ShadedJarLoader(jar);
    }

    /**
     * Answers connector and Arrow class requests from the jar alone, and delegates the rest to the
     * parent.
     *
     * <p>A loader with no parent at all would be simpler, but then reflecting over a connector
     * method fails: resolving a signature such as {@code toArrowSchema(RowType)} needs the Flink
     * API, which the jar does not bundle. Delegating everything to the parent is no good either,
     * since parent-first would answer both prefixes from the test classpath, where Arrow is not
     * relocated, and the relocation assertions would then pass or fail on the wrong classes.
     */
    private static final class ShadedJarLoader extends URLClassLoader {

        private static final String[] JAR_ONLY = {
            "org.apache.flink.connector.lance.", "org.apache.arrow."
        };

        ShadedJarLoader(File jar) throws Exception {
            super(new URL[] {jar.toURI().toURL()}, ShadedJarLoader.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            for (String prefix : JAR_ONLY) {
                if (name.startsWith(prefix)) {
                    return loadFromJarOnly(name, resolve);
                }
            }
            return super.loadClass(name, resolve);
        }

        private Class<?> loadFromJarOnly(String name, boolean resolve)
                throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    // Throws ClassNotFoundException when the jar does not carry the class, which
                    // is what the "plain Arrow is absent" assertion relies on.
                    loaded = findClass(name);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }
    }

    @Test
    @DisplayName("Arrow is relocated in the shaded jar")
    void testArrowIsRelocated() throws Exception {
        File jar = shadedJar();
        try (ShadedJarLoader loader = isolatedLoader(jar)) {
            // Throws ClassNotFoundException if the relocation stopped happening.
            assertThat(loader.loadClass(SHADED_ARROW_SCHEMA)).isNotNull();
        }

        // One class is not enough. Relocations get carved out per package often enough --
        // a JNI package that breaks when its classes move is the usual reason -- and such an
        // exclusion leaves plain Arrow in the jar while the class above still resolves. So
        // walk every entry instead of trusting a single probe.
        try (JarFile entries = new JarFile(jar)) {
            List<String> plainArrow =
                    entries.stream()
                            .map(ZipEntry::getName)
                            .filter(n -> n.startsWith("org/apache/arrow/") && n.endsWith(".class"))
                            .limit(10)
                            .collect(Collectors.toList());
            assertThat(plainArrow)
                    .as("non-relocated Arrow classes must not be bundled (first 10 shown)")
                    .isEmpty();
        }
    }

    @Test
    @DisplayName("Connector signatures reference the relocated Arrow packages")
    void testConnectorSignaturesRelocated() throws Exception {
        File jar = shadedJar();
        try (ShadedJarLoader loader = isolatedLoader(jar)) {
            Class<?> converter =
                    loader.loadClass(
                            "org.apache.flink.connector.lance.converter.LanceTypeConverter");
            List<Method> toArrowSchema =
                    Arrays.stream(converter.getDeclaredMethods())
                            .filter(m -> m.getName().equals("toArrowSchema"))
                            .collect(Collectors.toList());

            assertThat(toArrowSchema).as("toArrowSchema should exist").isNotEmpty();
            // Every overload, not just whichever one reflection happens to return first.
            assertThat(toArrowSchema)
                    .as("the return type must be the relocated Arrow Schema")
                    .allSatisfy(
                            m ->
                                    assertThat(m.getReturnType().getName())
                                            .isEqualTo(SHADED_ARROW_SCHEMA));
        }
    }
}

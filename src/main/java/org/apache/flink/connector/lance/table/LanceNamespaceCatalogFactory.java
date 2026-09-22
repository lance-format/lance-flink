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

import org.apache.flink.connector.lance.catalog.namespace.LanceNamespaceConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.lance.namespace.LanceNamespace;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.util.LanceAllocators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Flink {@link CatalogFactory} for {@link LanceNamespaceCatalog}.
 *
 * <p>Usage via Flink SQL:
 * <pre>{@code
 * -- Directory-based namespace (local)
 * CREATE CATALOG my_lance WITH (
 *     'type' = 'lance-namespace',
 *     'impl' = 'dir',
 *     'root' = '/tmp/lance-warehouse'
 * );
 *
 * -- REST-based namespace
 * CREATE CATALOG my_lance WITH (
 *     'type' = 'lance-namespace',
 *     'impl' = 'rest',
 *     'uri' = 'http://localhost:8080'
 * );
 * }</pre>
 */
public class LanceNamespaceCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "lance-namespace";

    public static final ConfigOption<String> IMPL = ConfigOptions
            .key(LanceNamespaceConfig.KEY_IMPL)
            .stringType()
            .noDefaultValue()
            .withDescription("Lance namespace implementation: 'dir' or 'rest'");

    public static final ConfigOption<String> ROOT = ConfigOptions
            .key(LanceNamespaceConfig.KEY_ROOT)
            .stringType()
            .noDefaultValue()
            .withDescription("Root path for directory namespace");

    public static final ConfigOption<String> URI = ConfigOptions
            .key(LanceNamespaceConfig.KEY_URI)
            .stringType()
            .noDefaultValue()
            .withDescription("URI for REST namespace");

    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key("default-database")
            .stringType()
            .defaultValue(LanceNamespaceCatalog.DEFAULT_DATABASE)
            .withDescription("Default database name");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IMPL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROOT);
        options.add(URI);
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        String catalogName = context.getName();
        String impl = helper.getOptions().get(IMPL);
        String defaultDatabase = helper.getOptions().get(DEFAULT_DATABASE);

        // Build namespace connection properties
        Map<String, String> namespaceProps = new HashMap<>();
        namespaceProps.put(LanceNamespaceConfig.KEY_IMPL, impl);

        String root = helper.getOptions().get(ROOT);
        if (root != null) {
            namespaceProps.put(LanceNamespaceConfig.KEY_ROOT, root);
        }

        String uri = helper.getOptions().get(URI);
        if (uri != null) {
            namespaceProps.put(LanceNamespaceConfig.KEY_URI, uri);
        }

        // Collect remaining options for the namespace catalog
        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put(LanceNamespaceConfig.KEY_IMPL, impl);
        if (root != null) {
            catalogOptions.put(LanceNamespaceConfig.KEY_ROOT, root);
        }
        if (uri != null) {
            catalogOptions.put(LanceNamespaceConfig.KEY_URI, uri);
        }

        // Connect to Lance namespace
        // Use TCCL switch to ensure arrow-memory-netty SPI is visible from
        // Flink's test classloader which may not include it by default.
        ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread()
                    .setContextClassLoader(LanceNamespaceCatalogFactory.class.getClassLoader());
            BufferAllocator allocator = LanceAllocators.create("lance-namespace-catalog");
            LanceNamespace namespace =
                    LanceNamespace.connect(impl, namespaceProps, allocator);
            return new LanceNamespaceCatalog(
                    catalogName, defaultDatabase, namespace, allocator, catalogOptions);
        } finally {
            Thread.currentThread().setContextClassLoader(originalCL);
        }
    }
}

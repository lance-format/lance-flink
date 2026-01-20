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

package org.apache.flink.connector.lance.catalog.namespace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Configuration for Lance Namespace integration.
 * 
 * Supports:
 * - Namespace implementation selection (dir, rest, custom)
 * - Implementation-specific parameters (root path, REST URI)
 * - Extra level configuration (for Spark compatibility)
 * - Parent prefix support (for Hive 3 compatibility)
 * 
 * All hardcoded strings are managed through constants and enums
 * to ensure maintainability and type safety.
 */
public class LanceNamespaceConfig {
    
    // Configuration keys
    public static final String KEY_IMPL = "impl";
    public static final String KEY_ROOT = "root";
    public static final String KEY_URI = "uri";
    public static final String KEY_EXTRA_LEVEL = "extra_level";
    public static final String KEY_PARENT = "parent";
    public static final String KEY_PARENT_DELIMITER = "parent_delimiter";
    
    /**
     * Default parent path delimiter.
     * Extracted as constant to eliminate hardcoding in logic.
     */
    public static final String DEFAULT_PARENT_DELIMITER = ".";
    
    /**
     * Enumeration for supported namespace implementations.
     * 
     * This enum manages:
     * - Implementation type values (no hardcoding in business logic)
     * - Type validation and parsing
     * - Easy extension for new implementation types
     */
    public enum ImplType {
        /**
         * Directory-based namespace implementation (local file system).
         * Value: "dir"
         */
        DIRECTORY("dir"),
        
        /**
         * REST-based namespace implementation (remote server).
         * Value: "rest"
         */
        REST("rest");
        
        private final String typeValue;
        
        /**
         * Constructor for implementation type.
         * 
         * @param typeValue the string representation of this implementation type
         */
        ImplType(String typeValue) {
            this.typeValue = typeValue;
        }
        
        /**
         * Get the string representation of this implementation type.
         * 
         * @return the type value string
         */
        public String getTypeValue() {
            return typeValue;
        }
        
        /**
         * Get ImplType from string value.
         * 
         * @param value the string value to parse
         * @return Optional containing the ImplType, or empty if not found
         */
        public static Optional<ImplType> fromValue(String value) {
            if (value == null) {
                return Optional.empty();
            }
            for (ImplType type : ImplType.values()) {
                if (type.typeValue.equals(value)) {
                    return Optional.of(type);
                }
            }
            return Optional.empty();
        }
        
        /**
         * Get ImplType from string value, or throw exception if not found.
         * 
         * @param value the string value to parse
         * @return the ImplType for this value
         * @throws IllegalArgumentException if the value is not recognized
         */
        public static ImplType fromValueOrThrow(String value) {
            return fromValue(value)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "Unknown implementation type: " + value +
                            ". Supported types: " + getAllValues()
                    ));
        }
        
        /**
         * Get comma-separated list of all supported type values.
         * 
         * @return comma-separated values of all implementation types
         */
        public static String getAllValues() {
            StringBuilder sb = new StringBuilder();
            for (ImplType type : ImplType.values()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(type.typeValue);
            }
            return sb.toString();
        }
    }
    
    private final String impl;
    private final ImplType implType;
    private final Map<String, String> properties;
    private final Optional<String> extraLevel;
    private final Optional<String> parent;
    private final String parentDelimiter;
    
    /**
     * Create configuration from properties map.
     * 
     * @param properties the configuration properties map
     * @return a new LanceNamespaceConfig instance
     */
    public static LanceNamespaceConfig from(Map<String, String> properties) {
        return new LanceNamespaceConfig(properties);
    }
    
    /**
     * Create builder for configuration.
     * 
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Private constructor for configuration.
     * 
     * @param properties the configuration properties map
     * @throws IllegalArgumentException if required configuration is missing
     */
    private LanceNamespaceConfig(Map<String, String> properties) {
        this.properties = new HashMap<>(Objects.requireNonNull(properties, "Properties cannot be null"));
        
        // Extract required impl
        this.impl = properties.get(KEY_IMPL);
        if (this.impl == null || this.impl.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required configuration: " + KEY_IMPL);
        }
        
        // Validate and extract implementation type (eliminates hardcoding)
        this.implType = ImplType.fromValueOrThrow(this.impl);
        
        // Extract optional extra level
        String extraLevelValue = properties.get(KEY_EXTRA_LEVEL);
        this.extraLevel = extraLevelValue != null && !extraLevelValue.isEmpty() ?
                Optional.of(extraLevelValue) : Optional.empty();
        
        // Extract optional parent prefix
        String parentValue = properties.get(KEY_PARENT);
        this.parent = parentValue != null && !parentValue.isEmpty() ?
                Optional.of(parentValue) : Optional.empty();
        
        // Extract parent delimiter with default (eliminates hardcoded default)
        this.parentDelimiter = properties.getOrDefault(KEY_PARENT_DELIMITER, DEFAULT_PARENT_DELIMITER);
    }
    
    /**
     * Get namespace implementation type as enum.
     * 
     * @return the ImplType enum value
     */
    public ImplType getImplType() {
        return implType;
    }
    
    /**
     * Get namespace implementation type as string.
     * 
     * @return the implementation type string value
     */
    public String getImpl() {
        return impl;
    }
    
    /**
     * Get all configuration properties.
     * 
     * @return unmodifiable view of the properties map
     */
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
    
    /**
     * Get root path for directory namespace implementation.
     * 
     * @return Optional containing the root path if present
     */
    public Optional<String> getRoot() {
        return Optional.ofNullable(properties.get(KEY_ROOT));
    }
    
    /**
     * Get URI for REST namespace implementation.
     * 
     * @return Optional containing the URI if present
     */
    public Optional<String> getUri() {
        return Optional.ofNullable(properties.get(KEY_URI));
    }
    
    /**
     * Get extra level configuration (for Spark compatibility).
     * 
     * @return Optional containing the extra level if present
     */
    public Optional<String> getExtraLevel() {
        return extraLevel;
    }
    
    /**
     * Get parent prefix configuration (for Hive 3 compatibility).
     * 
     * @return Optional containing the parent prefix if present
     */
    public Optional<String> getParent() {
        return parent;
    }
    
    /**
     * Get parent delimiter.
     * 
     * @return the delimiter string (default: ".")
     */
    public String getParentDelimiter() {
        return parentDelimiter;
    }
    
    /**
     * Get parent prefix as array.
     * 
     * @return Optional containing the parent prefix split by delimiter
     */
    public Optional<String[]> getParentArray() {
        return parent.map(p -> p.split(java.util.regex.Pattern.quote(parentDelimiter)));
    }
    
    /**
     * Check if directory namespace implementation is configured.
     * Uses enum comparison instead of hardcoded string comparison.
     * 
     * @return true if this is a directory namespace implementation
     */
    public boolean isDirectoryNamespace() {
        return implType == ImplType.DIRECTORY;
    }
    
    /**
     * Check if REST namespace implementation is configured.
     * Uses enum comparison instead of hardcoded string comparison.
     * 
     * @return true if this is a REST namespace implementation
     */
    public boolean isRestNamespace() {
        return implType == ImplType.REST;
    }
    
    /**
     * Check if extra level should be automatically configured.
     * 
     * @return true if auto-configuration of extra level is needed
     */
    public boolean shouldAutoConfigureExtraLevel() {
        return !extraLevel.isPresent() && isDirectoryNamespace();
    }
    
    @Override
    public String toString() {
        return "LanceNamespaceConfig{" +
                "impl='" + impl + '\'' +
                ", extraLevel=" + extraLevel +
                ", parent=" + parent +
                ", parentDelimiter='" + parentDelimiter + '\'' +
                ", properties=" + properties +
                '}';
    }
    
    /**
     * Builder for LanceNamespaceConfig.
     * 
     * Provides a fluent interface for building configuration instances.
     */
    public static class Builder {
        private final Map<String, String> properties = new HashMap<>();
        
        /**
         * Set the namespace implementation type.
         * 
         * @param impl the implementation type ("dir" or "rest")
         * @return this builder instance
         */
        public Builder impl(String impl) {
            properties.put(KEY_IMPL, impl);
            return this;
        }
        
        /**
         * Set the namespace implementation type using enum.
         * 
         * @param implType the ImplType enum value
         * @return this builder instance
         */
        public Builder impl(ImplType implType) {
            properties.put(KEY_IMPL, implType.getTypeValue());
            return this;
        }
        
        /**
         * Set the root path for directory implementation.
         * 
         * @param root the root warehouse path
         * @return this builder instance
         */
        public Builder root(String root) {
            properties.put(KEY_ROOT, root);
            return this;
        }
        
        /**
         * Set the URI for REST implementation.
         * 
         * @param uri the REST server URI
         * @return this builder instance
         */
        public Builder uri(String uri) {
            properties.put(KEY_URI, uri);
            return this;
        }
        
        /**
         * Set the extra level configuration.
         * 
         * @param extraLevel the extra level value
         * @return this builder instance
         */
        public Builder extraLevel(String extraLevel) {
            properties.put(KEY_EXTRA_LEVEL, extraLevel);
            return this;
        }
        
        /**
         * Set the parent prefix.
         * 
         * @param parent the parent prefix value
         * @return this builder instance
         */
        public Builder parent(String parent) {
            properties.put(KEY_PARENT, parent);
            return this;
        }
        
        /**
         * Set the parent delimiter.
         * 
         * @param delimiter the delimiter string (default: ".")
         * @return this builder instance
         */
        public Builder parentDelimiter(String delimiter) {
            properties.put(KEY_PARENT_DELIMITER, delimiter);
            return this;
        }
        
        /**
         * Set a custom property.
         * 
         * @param key the property key
         * @param value the property value
         * @return this builder instance
         */
        public Builder property(String key, String value) {
            properties.put(key, value);
            return this;
        }
        
        /**
         * Add all properties from a map.
         * 
         * @param props the properties map to add
         * @return this builder instance
         */
        public Builder properties(Map<String, String> props) {
            properties.putAll(props);
            return this;
        }
        
        /**
         * Build the configuration instance.
         * 
         * @return a new LanceNamespaceConfig instance
         * @throws IllegalArgumentException if required properties are missing or invalid
         */
        public LanceNamespaceConfig build() {
            return new LanceNamespaceConfig(properties);
        }
    }
}

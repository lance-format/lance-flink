# Install

## Requirements

* JDK 11+ (Flink 1.20) or JDK 17+ (Flink 2.1)
* Maven 3.6+
* Apache Flink 1.20 / 2.1

The connector ships one artifact per supported Flink minor version:

| Flink version | Artifact | Required JDK |
|---|---|---|
| 1.20 | `lance-flink-1.20` | JDK 11+ |
| 2.1 | `lance-flink-2.1` | JDK 17+ |

## Dependencies

The connector depends on `org.lance:lance-core` (7.0.0) and Apache Arrow (18.3.0). These are pulled
in transitively; you only need to add the connector artifact for your Flink version.

Lance's Java bindings ship a platform-specific JNI native library
(`liblance_jni.so` / `liblance_jni.dylib`) inside the `lance-core` jar. Ensure you run on a
supported platform (linux-x86-64, linux-aarch64, darwin-aarch64).

## Maven

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>lance-flink-2.1</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Build from source

```bash
mvn clean verify
```

The build produces a fat jar per module (e.g. `lance-flink-2.1/target/lance-flink-2.1-*.jar`).
Add the jar to your Flink cluster or job classpath, then use the `lance` / `lance-namespace`
catalog types in SQL.

## Note on Arrow and Netty

The Arrow allocator defaults are set at runtime; if you see classloader-related SPI issues in
tests, set the system property `arrow.memory.allocator.type=Netty`.

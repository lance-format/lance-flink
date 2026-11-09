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

The connector depends on `org.lance:lance-core` (7.0.0), `lance-namespace` (0.7.5) and Apache Arrow
(18.3.0). These are pulled in transitively; you only need to add the connector artifact for your
Flink version.

Lance's Java bindings ship a platform-specific JNI native library
(`liblance_jni.so` / `liblance_jni.dylib`) inside the `lance-core` jar. Ensure you run on a
supported platform (linux-x86-64, linux-aarch64, darwin-aarch64).

## Maven

```xml
<!-- Flink 1.20 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>lance-flink-1.20</artifactId>
    <version>0.2.0</version>
</dependency>

<!-- Flink 2.1 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>lance-flink-2.1</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Build from source

```bash
# Both modules (requires JDK 17+, since the Flink 2.1 module targets Java 17)
mvn clean package

# A single module
mvn -pl lance-flink-1.20 test
mvn -pl lance-flink-2.1 test
```

The build produces a fat jar per module (e.g. `lance-flink-2.1/target/lance-flink-2.1-*.jar`).
Add the jar to your Flink cluster or job classpath, then use the `lance` / `lance-namespace`
catalog types in SQL.

## Note on Arrow and Netty

The Arrow allocator defaults are set at runtime; if you see classloader-related SPI issues in
tests, set the system property `arrow.memory.allocator.type=Netty`.

On JDK 17+, Arrow reaches `java.nio.Buffer.address` reflectively, so add
`--add-opens=java.base/java.nio=ALL-UNNAMED` to the JVM arguments (the Maven build already does
this through `argLine`).

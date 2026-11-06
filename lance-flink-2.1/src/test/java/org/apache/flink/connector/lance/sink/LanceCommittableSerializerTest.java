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

package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.FragmentMetadata;
import org.lance.fragment.DataFile;
import org.lance.fragment.DeletionFile;
import org.lance.fragment.DeletionFileType;
import org.lance.fragment.RowIdMeta;
import org.lance.fragment.VersionMeta;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceCommittableSerializer round-trip tests.
 *
 * <p>B2 (maintainer review PR#86): committable serialization is on the Flink 2PC
 * checkpoint path; a format regression silently breaks Exactly-Once on recovery.
 *
 * <p>#88 (M5): the fragment segment moved from Java native serialization to explicit
 * field encoding ({@link FragmentMetadataCodec}). These tests now construct real
 * {@code FragmentMetadata} (its nested types all expose public constructors, so no
 * live Lance JNI handle is needed) and verify lossless round-trip plus v1/v2 read
 * compatibility.
 */
class LanceCommittableSerializerTest {

    private final LanceCommittableSerializer serializer = LanceCommittableSerializer.INSTANCE;

    @Test
    @DisplayName("Empty-fragment APPEND committable round-trips")
    void appendEmptyCommittableRoundTrip() throws IOException {
        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        /* arrowSchema */ null,
                        LanceOptions.WriteMode.APPEND,
                        Collections.emptyList());

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getDatasetPath()).isEqualTo("/tmp/x");
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(out.getArrowSchema()).isNull();
        assertThat(out.getFragments()).isEmpty();
    }

    @Test
    @DisplayName("Empty-fragment OVERWRITE committable round-trips preserving writeMode")
    void overwriteEmptyCommittableRoundTrip() throws IOException {
        LanceCommittable in =
                new LanceCommittable(
                        "s3://bucket/foo",
                        /* arrowSchema */ null,
                        LanceOptions.WriteMode.OVERWRITE,
                        Collections.emptyList());

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getDatasetPath()).isEqualTo("s3://bucket/foo");
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
    }

    @Test
    @DisplayName("Committable with Arrow Schema round-trips via JSON")
    void committableWithSchemaRoundTrip() throws IOException {
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        schema,
                        LanceOptions.WriteMode.APPEND,
                        Collections.emptyList());

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(out.getArrowSchema()).isNotNull();
        assertThat(out.getArrowSchema().getFields()).hasSize(2);
        assertThat(out.getArrowSchema().getFields().get(0).getName()).isEqualTo("id");
        assertThat(out.getArrowSchema().getFields().get(1).getName()).isEqualTo("name");
    }

    @Test
    @DisplayName("Null writeMode in constructor is normalized to APPEND and round-trips")
    void nullWriteModeNormalizedToAppend() throws IOException {
        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        /* schema */ null,
                        /* writeMode */ null,
                        Collections.emptyList());

        assertThat(in.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
    }

    @Test
    @DisplayName("deserialize rejects unknown serializer version")
    void rejectsUnknownVersion() {
        byte[] bytes = new byte[16]; // garbage
        assertThatThrownBy(() -> serializer.deserialize(999, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    @DisplayName("getVersion is stable positive")
    void getVersionReturnsPositive() {
        assertThat(serializer.getVersion()).isGreaterThan(0);
    }

    // ------------------------------------------------------------------------
    // R1 regression: v1 backward-compatibility
    //
    // Pre-B1-fix committables had the format:
    //   [datasetPath:utf][createDataset:bool]
    //   [schemaJsonLen:int][schemaJson:bytes]
    //   [fragmentCount:int][fragmentBlobLen:int][fragmentBlob:bytes]
    //
    // After B1, the Writer no longer emits createDataset; we store writeMode
    // instead. The serializer must still read old v1 bytes from in-flight
    // checkpoints taken against an alpha 2.1 build, mapping createDataset=true
    // to OVERWRITE (the implicit v1 semantics: "this committable creates the
    // dataset" == "run Overwrite") and false to APPEND.
    // ------------------------------------------------------------------------

    @Test
    @DisplayName("v1 committable with createDataset=true deserializes to OVERWRITE")
    void v1CreateDatasetTrueMapsToOverwrite() throws IOException {
        byte[] v1Bytes = encodeV1("/tmp/legacy", /* createDataset */ true, /* schema */ null);

        LanceCommittable out = serializer.deserialize(1, v1Bytes);

        assertThat(out.getDatasetPath()).isEqualTo("/tmp/legacy");
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
        assertThat(out.getArrowSchema()).isNull();
        assertThat(out.getFragments()).isEmpty();
    }

    @Test
    @DisplayName("v1 committable with createDataset=false deserializes to APPEND and preserves schema")
    void v1CreateDatasetFalseMapsToAppendWithSchema() throws IOException {
        Schema schema =
                new Schema(
                        Collections.singletonList(
                                new Field(
                                        "x",
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null)));

        byte[] v1Bytes = encodeV1("hdfs://ns/legacy", /* createDataset */ false, schema);

        LanceCommittable out = serializer.deserialize(1, v1Bytes);

        assertThat(out.getDatasetPath()).isEqualTo("hdfs://ns/legacy");
        assertThat(out.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(out.getArrowSchema()).isNotNull();
        assertThat(out.getArrowSchema().getFields()).hasSize(1);
        assertThat(out.getArrowSchema().getFields().get(0).getName()).isEqualTo("x");
    }

    /**
     * Hand-rolled v1 byte layout (matches the pre-B1 LanceCommittableSerializer).
     * Keeping this as a test-only helper so the production serializer never
     * writes v1 again (write path only emits v2 via {@code getVersion()}).
     */
    private static byte[] encodeV1(String path, boolean createDataset, Schema schema)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(path);
            out.writeBoolean(createDataset);

            byte[] schemaJson =
                    schema == null
                            ? new byte[0]
                            : schema.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(schemaJson.length);
            out.write(schemaJson);

            // zero fragments + zero-length blob (consistent with real v1 for empty list)
            out.writeInt(0);
            byte[] emptyListBlob;
            try (ByteArrayOutputStream bbos = new ByteArrayOutputStream(64);
                 ObjectOutputStream oos = new ObjectOutputStream(bbos)) {
                oos.writeObject(new ArrayList<>());
                oos.flush();
                emptyListBlob = bbos.toByteArray();
            }
            out.writeInt(emptyListBlob.length);
            out.write(emptyListBlob);
        }
        return bos.toByteArray();
    }

    // ------------------------------------------------------------------------
    // #88 regression: non-empty FragmentMetadata round-trip + v2 compat
    // ------------------------------------------------------------------------

    @Test
    @DisplayName("Non-empty FragmentMetadata round-trips losslessly via explicit encoding")
    void nonEmptyFragmentsRoundTrip() throws IOException {
        DataFile dataFile =
                new DataFile(
                        "file:///tmp/data/0.lance",
                        new int[] {0, 1},
                        new int[] {0, 1},
                        /* fileMajorVersion */ 1,
                        /* fileMinorVersion */ 0,
                        /* fileSizeBytes */ 1024L,
                        /* baseId */ null);

        DeletionFile deletionFile =
                new DeletionFile(
                        /* id */ 7L,
                        /* readVersion */ 3L,
                        /* numDeletedRows */ 10L,
                        DeletionFileType.ARRAY,
                        /* baseId */ null);

        RowIdMeta rowIdMeta = RowIdMeta.fromRowIds(new long[] {1L, 2L, 3L});
        VersionMeta createdAt = new VersionMeta("created-at-metadata");
        VersionMeta lastUpdated = new VersionMeta("last-updated-metadata");

        FragmentMetadata fragment =
                new FragmentMetadata(
                        /* id */ 1,
                        Collections.singletonList(dataFile),
                        /* physicalRows */ 100L,
                        deletionFile,
                        rowIdMeta,
                        createdAt,
                        lastUpdated);

        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/x",
                        /* schema */ null,
                        LanceOptions.WriteMode.APPEND,
                        Collections.singletonList(fragment));

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        List<FragmentMetadata> outFragments = out.getFragments();
        assertThat(outFragments).hasSize(1);

        FragmentMetadata ofm = outFragments.get(0);
        assertThat(ofm.getId()).isEqualTo(1);
        assertThat(ofm.getPhysicalRows()).isEqualTo(100L);

        assertThat(ofm.getFiles()).hasSize(1);
        DataFile odf = ofm.getFiles().get(0);
        assertThat(odf.getPath()).isEqualTo("file:///tmp/data/0.lance");
        assertThat(odf.getFields()).containsExactly(0, 1);
        assertThat(odf.getColumnIndices()).containsExactly(0, 1);
        assertThat(odf.getFileMajorVersion()).isEqualTo(1);
        assertThat(odf.getFileMinorVersion()).isEqualTo(0);
        assertThat(odf.getFileSizeBytes()).isEqualTo(1024L);
        assertThat(odf.getBaseId()).isEmpty();

        DeletionFile odel = ofm.getDeletionFile();
        assertThat(odel).isNotNull();
        assertThat(odel.getId()).isEqualTo(7L);
        assertThat(odel.getReadVersion()).isEqualTo(3L);
        assertThat(odel.getNumDeletedRows()).isEqualTo(10L);
        assertThat(odel.getFileType()).isEqualTo(DeletionFileType.ARRAY);
        assertThat(odel.getBaseId()).isEmpty();

        // RowIdMeta / VersionMeta: assert round-trip equality on getMetadata(),
        // without assuming Lance's internal encoding of the metadata string.
        assertThat(ofm.getRowIdMeta()).isNotNull();
        assertThat(ofm.getRowIdMeta().getMetadata()).isEqualTo(rowIdMeta.getMetadata());
        assertThat(ofm.getCreatedAtVersionMeta().getMetadata()).isEqualTo(createdAt.getMetadata());
        assertThat(ofm.getLastUpdatedAtVersionMeta().getMetadata())
                .isEqualTo(lastUpdated.getMetadata());
    }

    @Test
    @DisplayName("Nullable fragment fields round-trip: no deletion file / row-id / version meta, present baseId")
    void nullableFragmentFieldsRoundTrip() throws IOException {
        DataFile dataFile =
                new DataFile(
                        "s3://bucket/data/1.lance",
                        new int[] {0, 2},
                        new int[] {1, 3},
                        /* fileMajorVersion */ 2,
                        /* fileMinorVersion */ 1,
                        /* fileSizeBytes */ null,
                        /* baseId */ 5);

        FragmentMetadata fragment =
                new FragmentMetadata(
                        /* id */ 2,
                        Collections.singletonList(dataFile),
                        /* physicalRows */ 200L,
                        /* deletionFile */ null,
                        /* rowIdMeta */ null);

        LanceCommittable in =
                new LanceCommittable(
                        "/tmp/y",
                        /* schema */ null,
                        LanceOptions.WriteMode.APPEND,
                        Collections.singletonList(fragment));

        byte[] bytes = serializer.serialize(in);
        LanceCommittable out = serializer.deserialize(serializer.getVersion(), bytes);

        FragmentMetadata ofm = out.getFragments().get(0);
        assertThat(ofm.getDeletionFile()).isNull();
        assertThat(ofm.getRowIdMeta()).isNull();

        DataFile odf = ofm.getFiles().get(0);
        assertThat(odf.getFileSizeBytes()).isNull();
        assertThat(odf.getBaseId()).contains(5);
        assertThat(odf.getFields()).containsExactly(0, 2);
        assertThat(odf.getColumnIndices()).containsExactly(1, 3);
    }

    @Test
    @DisplayName("v2 committable with Java-serialized fragments still deserializes")
    void v2JavaSerializedFragmentsStillDeserialize() throws IOException {
        DataFile dataFile =
                new DataFile(
                        "file:///tmp/legacy/0.lance",
                        new int[] {0},
                        new int[] {0},
                        /* fileMajorVersion */ 1,
                        /* fileMinorVersion */ 0,
                        /* fileSizeBytes */ 42L,
                        /* baseId */ null);

        FragmentMetadata fragment =
                new FragmentMetadata(
                        /* id */ 9,
                        Collections.singletonList(dataFile),
                        /* physicalRows */ 9L,
                        /* deletionFile */ null,
                        /* rowIdMeta */ null);

        byte[] v2Bytes =
                encodeV2(
                        "/tmp/legacy",
                        LanceOptions.WriteMode.APPEND,
                        /* schema */ null,
                        Collections.singletonList(fragment));

        LanceCommittable out = serializer.deserialize(2, v2Bytes);

        assertThat(out.getDatasetPath()).isEqualTo("/tmp/legacy");
        assertThat(out.getFragments()).hasSize(1);
        assertThat(out.getFragments().get(0).getId()).isEqualTo(9);
        assertThat(out.getFragments().get(0).getFiles().get(0).getPath())
                .isEqualTo("file:///tmp/legacy/0.lance");
    }

    /**
     * Hand-rolled v2 byte layout (matches the pre-#88 LanceCommittableSerializer,
     * whose fragment segment used Java native serialization). Test-only helper; the
     * production serializer never writes v2 again (write path only emits v3).
     */
    private static byte[] encodeV2(
            String path,
            LanceOptions.WriteMode mode,
            Schema schema,
            List<FragmentMetadata> fragments)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(path);
            out.writeByte(mode == LanceOptions.WriteMode.OVERWRITE ? 1 : 0);

            byte[] schemaJson =
                    schema == null
                            ? new byte[0]
                            : schema.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(schemaJson.length);
            out.write(schemaJson);

            out.writeInt(fragments.size());
            byte[] blob;
            try (ByteArrayOutputStream bbos = new ByteArrayOutputStream(256);
                 ObjectOutputStream oos = new ObjectOutputStream(bbos)) {
                oos.writeObject(new ArrayList<>(fragments));
                oos.flush();
                blob = bbos.toByteArray();
            }
            out.writeInt(blob.length);
            out.write(blob);
        }
        return bos.toByteArray();
    }
}

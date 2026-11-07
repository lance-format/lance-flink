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
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.FragmentMetadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link LanceCommittable}.
 *
 * <p>Encoding format (version=3):
 * <pre>
 *   [datasetPath:utf]
 *   [writeMode:byte]                                  // 0 = APPEND, 1 = OVERWRITE
 *   [schemaJsonLen:int][schemaJson:bytes]             // JSON representation of the Arrow Schema (may be empty)
 *   [fragmentCount:int][fragment...]                  // explicit field encoding, see {@link FragmentMetadataCodec}
 * </pre>
 *
 * <p>Changes from version=2 (issue #88 / M5): the fragment section changed from "serializing the
 * entire list via Java native serialization in one shot" to explicit field encoding via
 * {@link FragmentMetadataCodec}, removing the {@code ObjectOutputStream} write path and
 * eliminating the native-deserialization attack surface and serialVersionUID fragility.
 *
 * <p>Changes from version=1: removed {@code createDataset:bool} (a writer-side decision that, after
 * the B1 fix, is now handled by the Committer), and added {@code writeMode:byte} so the Committer
 * knows the write semantics declared by the job.
 *
 * <p><b>Backward compatibility (R1 + #88)</b>: {@link #deserialize(int, byte[])} retains read
 * paths for version=1 and version=2. v1 maps the legacy {@code createDataset:bool} to
 * {@link LanceOptions.WriteMode#OVERWRITE} (true) or {@link LanceOptions.WriteMode#APPEND} (false)
 * — the implicit semantics before the B1 fix: a committable with createDataset=true creates the
 * table via Overwrite, others use Append. The fragment section of v1/v2 is still a Java
 * native-serialization blob, so the read path keeps {@code ObjectInputStream} (only for historical
 * compatibility; new data no longer writes that format). This lets an upgraded job seamlessly read
 * v1/v2 committables that were persisted in checkpoints by earlier alpha builds.
 *
 * <p>Arrow Schema does not {@code implements Serializable}, but provides the round-trip API
 * {@code toJson()} and {@code Schema.fromJSON(String)}; using JSON text is both simple and stable.
 *
 * <p>{@link FragmentMetadata} and its nested types do implement {@code Serializable}, but this class
 * does not use Java native serialization; it uses {@link FragmentMetadataCodec}'s explicit field
 * encoding instead.
 */
public final class LanceCommittableSerializer
        implements SimpleVersionedSerializer<LanceCommittable> {

    public static final LanceCommittableSerializer INSTANCE = new LanceCommittableSerializer();

    private static final int VERSION = 3;

    /** Historical versions still supported for reading (the write side only uses {@link #VERSION}). */
    private static final int VERSION_V1 = 1;

    /** version=2: the fragment section still uses Java native serialization (the pre-M5 write format). */
    private static final int VERSION_V2 = 2;

    private static final byte WRITE_MODE_APPEND = 0;
    private static final byte WRITE_MODE_OVERWRITE = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LanceCommittable c) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeUTF(c.getDatasetPath());
            out.writeByte(encodeWriteMode(c.getWriteMode()));

            Schema schema = c.getArrowSchema();
            byte[] schemaJson =
                    schema == null
                            ? new byte[0]
                            : schema.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.writeInt(schemaJson.length);
            out.write(schemaJson);

            FragmentMetadataCodec.writeFragments(out, c.getFragments());
        }
        return bos.toByteArray();
    }

    @Override
    public LanceCommittable deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION:
                return deserializeV3(serialized);
            case VERSION_V2:
                return deserializeV2(serialized);
            case VERSION_V1:
                return deserializeV1(serialized);
            default:
                throw new IOException(
                        "Unsupported LanceCommittable serializer version: "
                                + version
                                + " (supported: "
                                + VERSION_V1
                                + ", "
                                + VERSION_V2
                                + ", "
                                + VERSION
                                + ")");
        }
    }

    private LanceCommittable deserializeV3(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            LanceOptions.WriteMode writeMode = decodeWriteMode(in.readByte());

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema =
                        Schema.fromJSON(
                                new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            List<FragmentMetadata> fragments = FragmentMetadataCodec.readFragments(in);

            return new LanceCommittable(path, schema, writeMode, fragments);
        }
    }

    @SuppressWarnings("unchecked")
    private LanceCommittable deserializeV2(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            LanceOptions.WriteMode writeMode = decodeWriteMode(in.readByte());

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema =
                        Schema.fromJSON(
                                new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            int count = in.readInt();
            int blobLen = in.readInt();
            byte[] blob = new byte[blobLen];
            in.readFully(blob);
            List<FragmentMetadata> fragments =
                    count == 0 ? new ArrayList<>() : (List<FragmentMetadata>) javaDeserialize(blob);

            return new LanceCommittable(path, schema, writeMode, fragments);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FragmentMetadata list", e);
        }
    }

    /**
     * Reads the legacy version=1 format (committables written before the B1 fix):
     * <pre>
     *   [datasetPath:utf]
     *   [createDataset:bool]                              // v1-specific
     *   [schemaJsonLen:int][schemaJson:bytes]
     *   [fragmentCount:int][fragmentBlobLen:int][fragmentBlob:bytes]
     * </pre>
     * Mapping rule: {@code createDataset=true → WriteMode.OVERWRITE}; otherwise APPEND.
     * This matches v1's implicit semantics: a committable with createDataset=true creates the table
     * via Overwrite in the Committer.
     */
    @SuppressWarnings("unchecked")
    private LanceCommittable deserializeV1(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String path = in.readUTF();
            boolean createDataset = in.readBoolean();
            LanceOptions.WriteMode writeMode =
                    createDataset
                            ? LanceOptions.WriteMode.OVERWRITE
                            : LanceOptions.WriteMode.APPEND;

            int schemaLen = in.readInt();
            Schema schema = null;
            if (schemaLen > 0) {
                byte[] schemaJson = new byte[schemaLen];
                in.readFully(schemaJson);
                schema =
                        Schema.fromJSON(
                                new String(schemaJson, java.nio.charset.StandardCharsets.UTF_8));
            }

            int count = in.readInt();
            int blobLen = in.readInt();
            byte[] blob = new byte[blobLen];
            in.readFully(blob);
            List<FragmentMetadata> fragments =
                    count == 0 ? new ArrayList<>() : (List<FragmentMetadata>) javaDeserialize(blob);

            return new LanceCommittable(path, schema, writeMode, fragments);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize FragmentMetadata list (v1)", e);
        }
    }

    private static byte encodeWriteMode(LanceOptions.WriteMode mode) {
        if (mode == LanceOptions.WriteMode.OVERWRITE) {
            return WRITE_MODE_OVERWRITE;
        }
        return WRITE_MODE_APPEND;
    }

    private static LanceOptions.WriteMode decodeWriteMode(byte b) throws IOException {
        switch (b) {
            case WRITE_MODE_APPEND:
                return LanceOptions.WriteMode.APPEND;
            case WRITE_MODE_OVERWRITE:
                return LanceOptions.WriteMode.OVERWRITE;
            default:
                throw new IOException("Unknown write mode byte: " + b);
        }
    }

    private static Object javaDeserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return ois.readObject();
        }
    }
}

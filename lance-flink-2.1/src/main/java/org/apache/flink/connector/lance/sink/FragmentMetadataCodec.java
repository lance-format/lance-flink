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

import org.lance.FragmentMetadata;
import org.lance.fragment.DataFile;
import org.lance.fragment.DeletionFile;
import org.lance.fragment.DeletionFileType;
import org.lance.fragment.RowIdMeta;
import org.lance.fragment.VersionMeta;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Explicit field codec for a list of {@link FragmentMetadata} (issue #88 / round-2 M5).
 *
 * <p>Replaces the Java native serialization of {@code List<FragmentMetadata>} in
 * {@link LanceCommittableSerializer} ({@code ObjectOutputStream} / {@code ObjectInputStream}).
 * Native serialization has two problems:
 * <ol>
 *   <li><b>Security</b>: {@code ObjectInputStream.readObject()} is a known high-risk
 *       deserialization surface;
 *   <li><b>Fragile forward compatibility</b>: any reordering/renaming of {@code FragmentMetadata}'s
 *       private fields changes its serialVersionUID, making old checkpoints unrecoverable with no
 *       escape hatch.
 * </ol>
 *
 * <p>This class encodes every field (including nullable and {@link Optional} fields) one by one into
 * a stable binary layout, independent of the class's {@code serialVersionUID}. The layout is
 * aligned with Lance 7.0.0's getter/constructor signatures; if a future Lance upgrade changes a
 * field, this code fails to compile in an explicit, trackable way (rather than silently throwing
 * {@code InvalidClassException} at runtime).
 *
 * <p>Encoding layout (the fragment section of a v3 committable):
 * <pre>
 *   [fragmentCount:int]
 *   for each FragmentMetadata:
 *     [id:int]
 *     [physicalRows:long]
 *     [fileCount:int]
 *     for each DataFile:
 *       [path:utf]
 *       [fieldsCount:int][fields:int...]
 *       [columnIndicesCount:int][columnIndices:int...]
 *       [fileMajorVersion:int]
 *       [fileMinorVersion:int]
 *       [fileSizeBytesPresent:bool][fileSizeBytes:long?]
 *       [baseIdPresent:bool][baseId:int?]
 *     [deletionFilePresent:bool]
 *     if present:
 *       [deletionId:long]
 *       [readVersion:long]
 *       [numDeletedRowsPresent:bool][numDeletedRows:long?]
 *       [fileTypeName:utf]
 *       [baseIdPresent:bool][baseId:int?]
 *     [rowIdMetaPresent:bool][rowIdMetaMetadata:utf?]
 *     [createdAtVersionMetaPresent:bool][createdAtMetadata:utf?]
 *     [lastUpdatedAtVersionMetaPresent:bool][lastUpdatedAtMetadata:utf?]
 * </pre>
 */
public final class FragmentMetadataCodec {

    private FragmentMetadataCodec() {}

    /** Writes the fragment count + each fragment. */
    public static void writeFragments(DataOutputStream out, List<FragmentMetadata> fragments)
            throws IOException {
        out.writeInt(fragments.size());
        for (FragmentMetadata fragment : fragments) {
            writeFragment(out, fragment);
        }
    }

    /** Reads the fragment count + each fragment. */
    public static List<FragmentMetadata> readFragments(DataInputStream in) throws IOException {
        int count = in.readInt();
        List<FragmentMetadata> fragments = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            fragments.add(readFragment(in));
        }
        return fragments;
    }

    private static void writeFragment(DataOutputStream out, FragmentMetadata f)
            throws IOException {
        out.writeInt(f.getId());
        out.writeLong(f.getPhysicalRows());

        List<DataFile> files = f.getFiles();
        out.writeInt(files.size());
        for (DataFile file : files) {
            writeDataFile(out, file);
        }

        DeletionFile deletionFile = f.getDeletionFile();
        out.writeBoolean(deletionFile != null);
        if (deletionFile != null) {
            writeDeletionFile(out, deletionFile);
        }

        RowIdMeta rowIdMeta = f.getRowIdMeta();
        out.writeBoolean(rowIdMeta != null);
        if (rowIdMeta != null) {
            out.writeUTF(rowIdMeta.getMetadata());
        }

        VersionMeta createdAt = f.getCreatedAtVersionMeta();
        out.writeBoolean(createdAt != null);
        if (createdAt != null) {
            out.writeUTF(createdAt.getMetadata());
        }

        VersionMeta lastUpdated = f.getLastUpdatedAtVersionMeta();
        out.writeBoolean(lastUpdated != null);
        if (lastUpdated != null) {
            out.writeUTF(lastUpdated.getMetadata());
        }
    }

    private static FragmentMetadata readFragment(DataInputStream in) throws IOException {
        int id = in.readInt();
        long physicalRows = in.readLong();

        int fileCount = in.readInt();
        List<DataFile> files = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            files.add(readDataFile(in));
        }

        DeletionFile deletionFile = null;
        if (in.readBoolean()) {
            deletionFile = readDeletionFile(in);
        }

        RowIdMeta rowIdMeta = null;
        if (in.readBoolean()) {
            rowIdMeta = new RowIdMeta(in.readUTF());
        }

        VersionMeta createdAt = null;
        if (in.readBoolean()) {
            createdAt = new VersionMeta(in.readUTF());
        }

        VersionMeta lastUpdated = null;
        if (in.readBoolean()) {
            lastUpdated = new VersionMeta(in.readUTF());
        }

        // The 7-arg constructor covers all fields; the boxed physicalRows argument is equivalent
        // to the getter's primitive long.
        return new FragmentMetadata(
                id,
                files,
                Long.valueOf(physicalRows),
                deletionFile,
                rowIdMeta,
                createdAt,
                lastUpdated);
    }

    private static void writeDataFile(DataOutputStream out, DataFile f) throws IOException {
        out.writeUTF(f.getPath());

        int[] fields = f.getFields();
        out.writeInt(fields.length);
        for (int field : fields) {
            out.writeInt(field);
        }

        int[] columnIndices = f.getColumnIndices();
        out.writeInt(columnIndices.length);
        for (int columnIndex : columnIndices) {
            out.writeInt(columnIndex);
        }

        out.writeInt(f.getFileMajorVersion());
        out.writeInt(f.getFileMinorVersion());

        Long fileSizeBytes = f.getFileSizeBytes();
        out.writeBoolean(fileSizeBytes != null);
        if (fileSizeBytes != null) {
            out.writeLong(fileSizeBytes);
        }

        Optional<Integer> baseId = f.getBaseId();
        out.writeBoolean(baseId.isPresent());
        if (baseId.isPresent()) {
            out.writeInt(baseId.get());
        }
    }

    private static DataFile readDataFile(DataInputStream in) throws IOException {
        String path = in.readUTF();

        int fieldsCount = in.readInt();
        int[] fields = new int[fieldsCount];
        for (int i = 0; i < fieldsCount; i++) {
            fields[i] = in.readInt();
        }

        int columnIndicesCount = in.readInt();
        int[] columnIndices = new int[columnIndicesCount];
        for (int i = 0; i < columnIndicesCount; i++) {
            columnIndices[i] = in.readInt();
        }

        int fileMajorVersion = in.readInt();
        int fileMinorVersion = in.readInt();

        Long fileSizeBytes = null;
        if (in.readBoolean()) {
            fileSizeBytes = in.readLong();
        }

        Integer baseId = null;
        if (in.readBoolean()) {
            baseId = in.readInt();
        }

        return new DataFile(
                path,
                fields,
                columnIndices,
                fileMajorVersion,
                fileMinorVersion,
                fileSizeBytes,
                baseId);
    }

    private static void writeDeletionFile(DataOutputStream out, DeletionFile f)
            throws IOException {
        out.writeLong(f.getId());
        out.writeLong(f.getReadVersion());

        Long numDeletedRows = f.getNumDeletedRows();
        out.writeBoolean(numDeletedRows != null);
        if (numDeletedRows != null) {
            out.writeLong(numDeletedRows);
        }

        out.writeUTF(f.getFileType().name());

        Optional<Integer> baseId = f.getBaseId();
        out.writeBoolean(baseId.isPresent());
        if (baseId.isPresent()) {
            out.writeInt(baseId.get());
        }
    }

    private static DeletionFile readDeletionFile(DataInputStream in) throws IOException {
        long id = in.readLong();
        long readVersion = in.readLong();

        Long numDeletedRows = null;
        if (in.readBoolean()) {
            numDeletedRows = in.readLong();
        }

        DeletionFileType fileType = DeletionFileType.valueOf(in.readUTF());

        Integer baseId = null;
        if (in.readBoolean()) {
            baseId = in.readInt();
        }

        return new DeletionFile(id, readVersion, numDeletedRows, fileType, baseId);
    }
}

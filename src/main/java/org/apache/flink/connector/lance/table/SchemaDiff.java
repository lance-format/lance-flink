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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Column-level diff between an existing Lance dataset schema (old) and the schema requested by an
 * {@code ALTER TABLE} statement (new).
 *
 * <p>Detects {@code ADD COLUMN}, {@code DROP COLUMN}, in-place type changes and column renames.
 * A rename is inferred when the old and new schemas differ by exactly one removed column and one
 * added column at the same position — Flink never issues {@code ADD} and {@code DROP} in a single
 * {@code ALTER TABLE}, so such a pairing can only be a {@code RENAME COLUMN}. If the diff cannot
 * be safely classified (an unsafe drop+add that would lose data), {@link #compute} throws rather
 * than guessing.
 */
public final class SchemaDiff {

    private final List<RowType.RowField> addedColumns;
    private final List<String> droppedColumns;
    private final List<String> typeChangedColumns;
    private final Map<String, LogicalType> typeChangeByColumn;
    private final List<Rename> renames;

    private SchemaDiff(
            List<RowType.RowField> addedColumns,
            List<String> droppedColumns,
            Map<String, LogicalType> typeChangeByColumn,
            List<Rename> renames) {
        this.addedColumns = Collections.unmodifiableList(addedColumns);
        this.droppedColumns = Collections.unmodifiableList(droppedColumns);
        this.typeChangeByColumn = Collections.unmodifiableMap(typeChangeByColumn);
        this.typeChangedColumns =
                Collections.unmodifiableList(new ArrayList<>(typeChangeByColumn.keySet()));
        this.renames = Collections.unmodifiableList(renames);
    }

    public static SchemaDiff compute(RowType oldRowType, RowType newRowType) {
        Map<String, RowType.RowField> oldFields = new LinkedHashMap<>();
        for (RowType.RowField field : oldRowType.getFields()) {
            oldFields.put(field.getName(), field);
        }

        Map<String, RowType.RowField> newFields = new LinkedHashMap<>();
        for (RowType.RowField field : newRowType.getFields()) {
            newFields.put(field.getName(), field);
        }

        Map<String, LogicalType> typeChanges = new LinkedHashMap<>();
        List<RowType.RowField> droppedCandidates = new ArrayList<>();
        List<RowType.RowField> addedCandidates = new ArrayList<>();

        for (RowType.RowField newField : newRowType.getFields()) {
            RowType.RowField oldField = oldFields.get(newField.getName());
            if (oldField == null) {
                addedCandidates.add(newField);
            } else if (!oldField.getType().equals(newField.getType())) {
                typeChanges.put(newField.getName(), newField.getType());
            }
        }
        for (RowType.RowField oldField : oldRowType.getFields()) {
            if (!newFields.containsKey(oldField.getName())) {
                droppedCandidates.add(oldField);
            }
        }

        List<RowType.RowField> added = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        List<Rename> renames = new ArrayList<>();

        if (!droppedCandidates.isEmpty() && !addedCandidates.isEmpty()) {
            // A single ALTER never combines ADD and DROP; a paired remove+add at the same position
            // can only be RENAME COLUMN. Anything else is unsafe and must be rejected to avoid
            // silently dropping the old column's data.
            if (droppedCandidates.size() != addedCandidates.size()) {
                throw new IllegalArgumentException(
                        "Cannot safely distinguish RENAME from DROP+ADD: dropped "
                                + fieldNames(droppedCandidates) + " but added "
                                + fieldNames(addedCandidates));
            }

            List<String> oldNames = oldRowType.getFieldNames();
            List<String> newNames = newRowType.getFieldNames();
            for (int i = 0; i < droppedCandidates.size(); i++) {
                RowType.RowField oldField = droppedCandidates.get(i);
                RowType.RowField newField = addedCandidates.get(i);
                if (oldNames.indexOf(oldField.getName()) != newNames.indexOf(newField.getName())) {
                    throw new IllegalArgumentException(
                            "Cannot safely distinguish RENAME from DROP+ADD: column positions differ for '"
                                    + oldField.getName() + "' -> '" + newField.getName() + "'");
                }
                LogicalType newType = oldField.getType().equals(newField.getType())
                        ? null : newField.getType();
                renames.add(new Rename(oldField.getName(), newField.getName(), newType));
            }
        } else {
            added = addedCandidates;
            for (RowType.RowField oldField : droppedCandidates) {
                dropped.add(oldField.getName());
            }
        }

        return new SchemaDiff(added, dropped, typeChanges, renames);
    }

    private static List<String> fieldNames(List<RowType.RowField> fields) {
        List<String> names = new ArrayList<>();
        for (RowType.RowField field : fields) {
            names.add(field.getName());
        }
        return names;
    }

    public List<RowType.RowField> getAddedColumns() {
        return addedColumns;
    }

    public List<String> getDroppedColumns() {
        return droppedColumns;
    }

    /**
     * Columns whose data type changed in place (same name, different type).
     */
    public List<String> getTypeChangedColumns() {
        return typeChangedColumns;
    }

    /**
     * In-place type changes keyed by column name, mapping to the new type.
     */
    public Map<String, LogicalType> getTypeChangeByColumn() {
        return typeChangeByColumn;
    }

    /**
     * Column renames detected as a remove+add pair at the same position.
     */
    public List<Rename> getRenames() {
        return renames;
    }

    public boolean hasTypeChanges() {
        if (!typeChangedColumns.isEmpty()) {
            return true;
        }
        for (Rename rename : renames) {
            if (rename.getNewType() != null) {
                return true;
            }
        }
        return false;
    }

    public boolean isEmpty() {
        return addedColumns.isEmpty()
                && droppedColumns.isEmpty()
                && typeChangeByColumn.isEmpty()
                && renames.isEmpty();
    }

    /**
     * A detected {@code RENAME COLUMN}, optionally combined with a data type change.
     */
    public static final class Rename {

        private final String oldName;
        private final String newName;
        private final LogicalType newType;

        Rename(String oldName, String newName, LogicalType newType) {
            this.oldName = oldName;
            this.newName = newName;
            this.newType = newType;
        }

        public String getOldName() {
            return oldName;
        }

        public String getNewName() {
            return newName;
        }

        /** The new type when the rename also changes the type, otherwise {@code null}. */
        public LogicalType getNewType() {
            return newType;
        }
    }
}

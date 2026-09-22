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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaDiffTest {

    private static RowType rowType(RowType.RowField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    private static final RowType.RowField ID = new RowType.RowField("id", new BigIntType(false));
    private static final RowType.RowField NAME =
            new RowType.RowField("name", new VarCharType(true, VarCharType.MAX_LENGTH));
    private static final RowType.RowField AGE = new RowType.RowField("age", new BigIntType(true));

    @Test
    void detectsAddedColumn() {
        RowType oldRow = rowType(ID);
        RowType newRow = rowType(ID, NAME);

        SchemaDiff diff = SchemaDiff.compute(oldRow, newRow);

        assertThat(diff.getAddedColumns()).extracting(RowType.RowField::getName).containsExactly("name");
        assertThat(diff.getDroppedColumns()).isEmpty();
        assertThat(diff.hasTypeChanges()).isFalse();
    }

    @Test
    void detectsDroppedColumn() {
        RowType oldRow = rowType(ID, NAME);
        RowType newRow = rowType(ID);

        SchemaDiff diff = SchemaDiff.compute(oldRow, newRow);

        assertThat(diff.getAddedColumns()).isEmpty();
        assertThat(diff.getDroppedColumns()).containsExactly("name");
        assertThat(diff.hasTypeChanges()).isFalse();
    }

    @Test
    void detectsTypeChange() {
        RowType oldRow = rowType(ID, new RowType.RowField("name", new VarCharType(true, VarCharType.MAX_LENGTH)));
        RowType newRow = rowType(ID, new RowType.RowField("name", new BigIntType(true)));

        SchemaDiff diff = SchemaDiff.compute(oldRow, newRow);

        assertThat(diff.getTypeChangedColumns()).containsExactly("name");
        assertThat(diff.hasTypeChanges()).isTrue();
    }

    @Test
    void reportsEmptyDiffForIdenticalSchemas() {
        RowType row = rowType(ID, NAME);

        SchemaDiff diff = SchemaDiff.compute(row, row);

        assertThat(diff.isEmpty()).isTrue();
        assertThat(diff.getAddedColumns()).isEmpty();
        assertThat(diff.getDroppedColumns()).isEmpty();
        assertThat(diff.hasTypeChanges()).isFalse();
    }

    @Test
    void detectsRename() {
        RowType oldRow = rowType(ID, NAME);
        RowType newRow = rowType(ID,
                new RowType.RowField("full_name", new VarCharType(true, VarCharType.MAX_LENGTH)));

        SchemaDiff diff = SchemaDiff.compute(oldRow, newRow);

        assertThat(diff.getRenames()).hasSize(1);
        SchemaDiff.Rename rename = diff.getRenames().get(0);
        assertThat(rename.getOldName()).isEqualTo("name");
        assertThat(rename.getNewName()).isEqualTo("full_name");
        assertThat(rename.getNewType()).isNull();
        assertThat(diff.getAddedColumns()).isEmpty();
        assertThat(diff.getDroppedColumns()).isEmpty();
        assertThat(diff.hasTypeChanges()).isFalse();
    }

    @Test
    void detectsRenameWithTypeChange() {
        RowType oldRow = rowType(ID, NAME);
        RowType newRow = rowType(ID, new RowType.RowField("full_name", new BigIntType(true)));

        SchemaDiff diff = SchemaDiff.compute(oldRow, newRow);

        assertThat(diff.getRenames()).hasSize(1);
        SchemaDiff.Rename rename = diff.getRenames().get(0);
        assertThat(rename.getOldName()).isEqualTo("name");
        assertThat(rename.getNewName()).isEqualTo("full_name");
        assertThat(rename.getNewType()).isInstanceOf(BigIntType.class);
        assertThat(diff.hasTypeChanges()).isTrue();
    }

    @Test
    void exposesTypeChangeMapping() {
        RowType oldRow = rowType(ID, NAME);
        RowType newRow = rowType(ID, new RowType.RowField("name", new BigIntType(true)));

        SchemaDiff diff = SchemaDiff.compute(oldRow, newRow);

        assertThat(diff.getTypeChangeByColumn()).containsKey("name");
        assertThat(diff.getTypeChangeByColumn().get("name")).isInstanceOf(BigIntType.class);
    }

    @Test
    void rejectsUnsafeDropAndAddAtDifferentPositions() {
        // 'name' removed at position 1; 'nickname' added at position 2. Not position-aligned,
        // so it cannot be a pure RENAME and must be rejected rather than treated as DROP+ADD.
        RowType oldRow = rowType(ID, NAME, AGE);
        RowType newRow = rowType(ID, AGE,
                new RowType.RowField("nickname", new VarCharType(true, VarCharType.MAX_LENGTH)));

        assertThatThrownBy(() -> SchemaDiff.compute(oldRow, newRow))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RENAME");
    }
}

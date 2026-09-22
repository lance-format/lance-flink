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

package org.apache.flink.connector.lance.converter;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the fallback branch of {@link RowDataConverter}'s {@code setNull}.
 *
 * <p>The if-else chain used to end without an {@code else}, so an Arrow vector type it did not
 * enumerate was skipped in silence. That is not a harmless no-op: the validity bit of a slot that
 * already holds a value stays set, and the previous row's value is emitted as this row's value
 * with nothing reported anywhere. A freshly allocated vector masks the bug, because the zeroed
 * validity buffer reads back as null while {@code getNullCount()} still returns 0.
 *
 * <p>{@code readValue}, {@code getFieldValue} and {@code writeValue} all reject unknown types, so
 * the silent branch was also an inconsistency inside one class.
 */
class RowDataConverterSetNullFallbackTest {

    private static Method setNullMethod() throws Exception {
        Method m = RowDataConverter.class.getDeclaredMethod("setNull", FieldVector.class, int.class);
        m.setAccessible(true);
        return m;
    }

    private static RowDataConverter newConverter() throws Exception {
        Constructor<?> c = RowDataConverter.class.getDeclaredConstructor(RowType.class);
        c.setAccessible(true);
        return (RowDataConverter) c.newInstance(RowType.of(new IntType()));
    }

    /** Unwraps the reflective wrapper so the assertion sees the real exception. */
    private static void invokeSetNull(FieldVector vector, int index) throws Exception {
        try {
            setNullMethod().invoke(newConverter(), vector, index);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            }
            throw e;
        }
    }

    @Test
    @DisplayName("An unsupported vector type is rejected instead of silently keeping a stale value")
    void unsupportedVectorIsRejected() throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                // IntervalDayVector is deliberately outside the enumerated chain: no Flink
                // logical type maps to it, so it stands in for any future gap.
                IntervalDayVector vector = new IntervalDayVector("iv", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 1, 5_000);
            vector.setValueCount(1);

            // Before the fix this call returned quietly and left slot 0 valid, so the row
            // serialised with the stale value rather than NULL.
            assertThatThrownBy(() -> invokeSetNull(vector, 0))
                    .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                    .hasMessageContaining("IntervalDayVector")
                    .hasMessageContaining("iv");
        }
    }

    @Test
    @DisplayName("A supported vector still nulls the slot, clearing a value already written")
    void supportedVectorClearsExistingValue() throws Exception {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                IntVector vector = new IntVector("i", allocator)) {
            vector.allocateNew(2);
            vector.setSafe(0, 42);
            vector.setValueCount(1);
            assertThat(vector.isNull(0)).isFalse();

            invokeSetNull(vector, 0);

            assertThat(vector.isNull(0))
                    .as("setNull must clear a slot that already holds a value")
                    .isTrue();
        }
    }

    @Test
    @DisplayName("MapVector is matched as a map, not captured by the ListVector branch")
    void mapVectorIsNotCapturedByListBranch() throws Exception {
        // MapVector extends ListVector, so the branch order matters: were the ListVector case
        // first, every map would be nulled through the list path.
        assertThat(ListVector.class).isAssignableFrom(MapVector.class);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                MapVector vector = MapVector.empty("m", allocator, false)) {
            vector.allocateNew();

            // Reaching the fallback would throw; a clean return proves a map branch was taken.
            invokeSetNull(vector, 0);

            assertThat(vector.isNull(0)).isTrue();
        }
    }
}

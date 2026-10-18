/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.streamfusion.arrow.vectors;

import java.util.Arrays;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.RowColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import tech.streamfusion.arrow.ArrowReader;
import tech.streamfusion.arrow.NestedRepresentation;

/** Arrow column vector for Row. */
@Internal
public final class ArrowRowColumnVector implements RowColumnVector {

    /** Container which is used to store the sequence of row values of a column to read. */
    private final StructVector structVector;

    private final ColumnVector[] fieldColumns;
    private final boolean represented;
    private final ArrowReader reader;
    private final RowDataSerializer serializer;
    private final RowData.FieldGetter[] getters;

    public ArrowRowColumnVector(StructVector structVector, ColumnVector[] fieldColumns) {
        this(structVector, fieldColumns, null);
    }

    public ArrowRowColumnVector(
            StructVector structVector, ColumnVector[] fieldColumns, LogicalType[] types) {
        this.structVector = Preconditions.checkNotNull(structVector);
        this.fieldColumns = Preconditions.checkNotNull(fieldColumns);
        represented =
                NestedRepresentation.kinds(structVector) != null
                        || Arrays.stream(fieldColumns).anyMatch(NestedRepresentation::requiresView);
        reader = represented ? new ArrowReader(fieldColumns) : null;
        serializer = !represented ? null : new RowDataSerializer(types);
        getters = !represented ? null : new RowData.FieldGetter[types.length];
        if (getters != null)
            for (int i = 0; i < types.length; i++) getters[i] = RowData.createFieldGetter(types[i], i);
    }

    @Override
    public ColumnarRowData getRow(int i) {
        VectorizedColumnBatch vectorizedColumnBatch = new VectorizedColumnBatch(fieldColumns);
        return new ColumnarRowData(vectorizedColumnBatch, i);
    }

    public RowData getRowData(int i) {
        if (!preservesRepresentation()) return getRow(i);
        var row = reader.read(i);
        var kinds = NestedRepresentation.kinds(structVector);
        if (kinds != null && !kinds.isNull(i))
            row.setRowKind(RowKind.fromByteValue((byte) (kinds.get(i) & 3)));
        if (kinds != null && !kinds.isNull(i) && (kinds.get(i) & NestedRepresentation.BINARY_ROW) != 0)
            return serializer.toBinaryRow(row).copy();
        var values = new GenericRowData(getters.length);
        values.setRowKind(row.getRowKind());
        for (int field = 0; field < getters.length; field++)
            values.setField(field, row.isNullAt(field) ? null : getters[field].getFieldOrNull(row));
        return serializer.copy(values);
    }

    public boolean preservesRepresentation() {
        return represented;
    }

    public boolean binaryArray(int index) {
        var kinds = NestedRepresentation.kinds(structVector);
        return kinds != null
                && !kinds.isNull(index)
                && (kinds.get(index) & NestedRepresentation.BINARY_ARRAY) != 0;
    }

    @Override
    public boolean isNullAt(int i) {
        return structVector.isNull(i);
    }
}

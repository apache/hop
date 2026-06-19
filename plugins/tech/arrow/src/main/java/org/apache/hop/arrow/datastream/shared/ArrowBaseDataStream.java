/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.arrow.datastream.shared;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datastream.metadata.DataStreamMeta;
import org.apache.hop.datastream.plugin.IDataStream;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

@Getter
@Setter
@GuiPlugin
public abstract class ArrowBaseDataStream implements IDataStream {
  protected String pluginId;
  protected String pluginName;

  protected boolean writing;

  protected IVariables variables;
  protected IHopMetadataProvider metadataProvider;
  protected BufferAllocator rootAllocator;
  protected VectorSchemaRoot vectorSchemaRoot;
  protected IRowMeta rowMeta;
  protected List<Object[]> rowBuffer;

  protected FileOutputStream fileOutputStream;
  protected FileInputStream fileInputStream;
  protected DataStreamMeta dataStreamMeta;

  protected ArrowBaseDataStream() {
    // Empty
  }

  public abstract ArrowBaseDataStream clone();

  @Override
  public void initialize(
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean writing,
      DataStreamMeta dataStreamMeta)
      throws HopException {
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.writing = writing;
    this.dataStreamMeta = dataStreamMeta;

    rowBuffer = new ArrayList<>();
    rootAllocator = new RootAllocator();
  }

  public IRowMeta getRowMeta() throws HopException {
    // To be implemented
    return null;
  }

  public void setRowMeta(IRowMeta rowMeta) throws HopException {
    // To be implemented
  }

  /**
   * Create an Arrow Schema using the given Hop row metadata
   *
   * @param writeRowMeta The row metadata to build a schema for
   * @return The Arrow schema built using Hop row metadata
   * @throws HopException In case something went wrong, usually with unsupported value types.
   */
  public static @NonNull Schema buildSchema(IRowMeta writeRowMeta) throws HopException {
    List<Field> fields = new ArrayList<>();
    for (IValueMeta valueMeta : writeRowMeta.getValueMetaList()) {
      String name = valueMeta.getName();
      FieldType fieldType =
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_STRING -> FieldType.nullable(new ArrowType.Utf8());
            case IValueMeta.TYPE_BOOLEAN -> FieldType.nullable(new ArrowType.Bool());
            case IValueMeta.TYPE_INTEGER -> FieldType.nullable(new ArrowType.Int(64, true));
            case IValueMeta.TYPE_NUMBER ->
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case IValueMeta.TYPE_BIGNUMBER ->
                FieldType.nullable(
                    new ArrowType.Decimal(valueMeta.getLength(), valueMeta.getPrecision(), 64));
            case IValueMeta.TYPE_DATE ->
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"));
            default ->
                throw new HopException(
                    "Data streaming to a file with Apache Arrow isn't yet supported for Hop data type "
                        + valueMeta.getTypeDesc());
          };
      Field field = new Field(name, fieldType, null);
      fields.add(field);
    }
    return new Schema(fields);
  }

  public static @NonNull IRowMeta buildRowMeta(Schema schema) throws HopException {
    IRowMeta readRowMeta = new RowMeta();
    for (Field field : schema.getFields()) {
      int hopType =
          switch (field.getType().getTypeID()) {
            case Utf8 -> IValueMeta.TYPE_STRING;
            case Int -> IValueMeta.TYPE_INTEGER;
            case Bool -> IValueMeta.TYPE_BOOLEAN;
            case Timestamp -> IValueMeta.TYPE_DATE;
            case Decimal -> IValueMeta.TYPE_BIGNUMBER;
            case FloatingPoint -> IValueMeta.TYPE_NUMBER;
            default ->
                throw new HopException(
                    "Unsupported data type ID found in stream for field "
                        + field.getName()
                        + " : "
                        + field.getType().getTypeID().name());
          };
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(field.getName(), hopType);
      readRowMeta.addValueMeta(valueMeta);
    }
    return readRowMeta;
  }

  public static void allocateFieldVectorsSpace(
      VectorSchemaRoot vectorSchemaRoot, IRowMeta rowMeta, int bufferSize) throws HopException {
    for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      FieldVector fieldVector = vectorSchemaRoot.getVector(fieldIndex);

      // Set all values for the field vector
      //
      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_STRING -> ((VarCharVector) fieldVector).allocateNew(bufferSize);
        case IValueMeta.TYPE_INTEGER -> ((BigIntVector) fieldVector).allocateNew(bufferSize);
        case IValueMeta.TYPE_NUMBER -> ((Float8Vector) fieldVector).allocateNew(bufferSize);
        case IValueMeta.TYPE_BIGNUMBER -> ((DecimalVector) fieldVector).allocateNew(bufferSize);
        case IValueMeta.TYPE_BOOLEAN -> ((BitVector) fieldVector).allocateNew(bufferSize);
        case IValueMeta.TYPE_DATE -> ((TimeStampVector) fieldVector).allocateNew(bufferSize);
        default ->
            throw new HopException(
                "Allocating space for field vector isn't supported yet for data type "
                    + valueMeta.getTypeDesc());
      }
    }
  }

  public static void convertHopRowToFieldVectorIndex(
      VectorSchemaRoot vectorSchemaRoot, IRowMeta rowMeta, int rowIndex, Object[] rowData)
      throws HopException {
    for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      FieldVector fieldVector = vectorSchemaRoot.getVector(fieldIndex);

      // Set all values for the field vector
      //
      setFieldVectorValueWithHopValue(valueMeta, rowIndex, fieldVector, rowData[fieldIndex]);
    }
  }

  public static void setFieldVectorValueWithHopValue(
      IValueMeta valueMeta, int rowIndex, FieldVector fieldVector, Object rowData)
      throws HopException {
    Object valueData = valueMeta.getNativeDataType(rowData);
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING -> addString(rowIndex, fieldVector, valueMeta, valueData);
      case IValueMeta.TYPE_INTEGER -> addInteger(rowIndex, fieldVector, valueMeta, valueData);
      case IValueMeta.TYPE_NUMBER -> addNumber(rowIndex, fieldVector, valueMeta, valueData);
      case IValueMeta.TYPE_BIGNUMBER -> addBigNumber(rowIndex, fieldVector, valueMeta, valueData);
      case IValueMeta.TYPE_BOOLEAN -> addBoolean(rowIndex, fieldVector, valueMeta, valueData);
      case IValueMeta.TYPE_DATE -> addDate(rowIndex, fieldVector, valueMeta, valueData);
      default ->
          throw new HopException(
              "Data streaming to an Apache Arrow stream file isn't yet supported for Hop data type "
                  + valueMeta.getTypeDesc());
    }
  }

  public static void addString(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    VarCharVector vector = (VarCharVector) fieldVector;
    // Check the size of the vector
    //
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getString(valueData).getBytes());
    }
  }

  public static void addInteger(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BigIntVector vector = (BigIntVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getInteger(valueData));
    }
  }

  public static void addNumber(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    Float8Vector vector = (Float8Vector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getNumber(valueData));
    }
  }

  public static void addBigNumber(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    DecimalVector vector = (DecimalVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getBigNumber(valueData));
    }
  }

  public static void addBoolean(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    BitVector vector = (BitVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, Boolean.TRUE.equals(valueMeta.getBoolean(valueData)) ? 1 : 0);
    }
  }

  public static void addDate(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, Object valueData)
      throws HopException {
    TimeStampMilliTZVector vector = (TimeStampMilliTZVector) fieldVector;
    if (valueMeta.isNull(valueData)) {
      vector.setNull(rowIndex);
    } else {
      vector.set(rowIndex, valueMeta.getDate(valueData).getTime());
    }
  }

  public static Object[] convertFieldVectorsToHopRow(
      List<FieldVector> readFieldVectors, IRowMeta rowMeta, int rowIndex) throws HopException {
    Object[] rowData = RowDataUtil.allocateRowData(rowMeta.size());

    for (int fieldIndex = 0; fieldIndex < rowMeta.size(); fieldIndex++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      FieldVector fieldVector = readFieldVectors.get(fieldIndex);
      Object valueData = getHopValueFromFieldVector(rowIndex, fieldVector, valueMeta, fieldIndex);
      rowData[fieldIndex] = valueData;
    }
    return rowData;
  }

  public static @Nullable Object getHopValueFromFieldVector(
      int rowIndex, FieldVector fieldVector, IValueMeta valueMeta, int fieldIndex)
      throws HopException {
    Object valueData;
    if (fieldVector.isNull(rowIndex)) {
      valueData = null;
    } else {
      valueData =
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_STRING -> getString(rowIndex, fieldVector);
            case IValueMeta.TYPE_BOOLEAN -> getBoolean(rowIndex, fieldVector);
            case IValueMeta.TYPE_INTEGER -> getInteger(rowIndex, fieldVector);
            case IValueMeta.TYPE_NUMBER -> getNumber(rowIndex, fieldVector);
            case IValueMeta.TYPE_BIGNUMBER -> getBigNumber(rowIndex, fieldVector);
            case IValueMeta.TYPE_DATE -> getDate(rowIndex, fieldVector);
            default ->
                throw new HopException(
                    "Reading value "
                        + fieldIndex
                        + " of type "
                        + valueMeta.getTypeDesc()
                        + " is not yet supported");
          };
    }
    return valueData;
  }

  public static String getString(int rowIndex, FieldVector fieldVector) {
    VarCharVector vector = (VarCharVector) fieldVector;
    return new String(vector.get(rowIndex));
  }

  public static Boolean getBoolean(int rowIndex, FieldVector fieldVector) {
    BitVector vector = (BitVector) fieldVector;
    return vector.get(rowIndex) == 1;
  }

  public static Long getInteger(int rowIndex, FieldVector fieldVector) {
    BigIntVector vector = (BigIntVector) fieldVector;
    return vector.get(rowIndex);
  }

  public static Double getNumber(int rowIndex, FieldVector fieldVector) {
    FloatingPointVector vector = (FloatingPointVector) fieldVector;
    return vector.getValueAsDouble(rowIndex);
  }

  public static Date getDate(int rowIndex, FieldVector fieldVector) {
    TimeStampVector vector = (TimeStampVector) fieldVector;
    return new Date(vector.get(rowIndex));
  }

  public static BigDecimal getBigNumber(int rowIndex, FieldVector fieldVector) {
    DecimalVector vector = (DecimalVector) fieldVector;
    return vector.getObject(rowIndex);
  }
}

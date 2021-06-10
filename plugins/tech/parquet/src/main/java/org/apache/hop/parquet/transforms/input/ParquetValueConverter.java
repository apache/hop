/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.parquet.transforms.input;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IValueMeta;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.math.BigDecimal;
import java.util.Date;

public class ParquetValueConverter extends PrimitiveConverter {

  private final RowMetaAndData group;
  private final IValueMeta valueMeta;
  private final int rowIndex;

  public ParquetValueConverter(RowMetaAndData group, int rowIndex) {
    this.group = group;
    this.valueMeta = group.getValueMeta(rowIndex);
    this.rowIndex = rowIndex;
  }

  @Override
  public void addBinary(Binary value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        object = value.toStringUsingUTF8();
        break;
      case IValueMeta.TYPE_BINARY:
        object = value.getBytes();
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        object = new BigDecimal(value.toStringUsingUTF8());
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Binary source data to type " + valueMeta.getTypeDesc());
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addLong(long value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_INTEGER:
        object = value;
        break;
      case IValueMeta.TYPE_STRING:
        object = Long.toString(value);
        break;
      case IValueMeta.TYPE_DATE:
        object = new Date(value);
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        object = new BigDecimal(value);
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Long source data to type " + valueMeta.getTypeDesc());
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addDouble(double value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_NUMBER:
        object = value;
        break;
      case IValueMeta.TYPE_STRING:
        object = Double.toString(value);
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        object = BigDecimal.valueOf(value);
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Double/Float source data to type " + valueMeta.getTypeDesc());
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addBoolean(boolean value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_BOOLEAN:
        object = value;
        break;
      case IValueMeta.TYPE_STRING:
        object = value ? "true" : "false";
        break;
      case IValueMeta.TYPE_INTEGER:
        object = value ? 1L : 0L;
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Boolean source data to type " + valueMeta.getTypeDesc());
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addFloat(float value) {
    addDouble(value);
  }

  @Override
  public void addInt(int value) {
    addLong(value);
  }
}

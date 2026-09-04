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
 */

package org.apache.hop.pipeline.transforms.janino.function;

import java.math.BigDecimal;
import java.util.Date;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;

/**
 * Gives an expression access to the row it is evaluating by field name, including fields that are
 * not part of the stream. Expressions receive an instance of this class under the name {@code row},
 * which makes it possible to write conditions for streams whose layout is only known at runtime:
 *
 * <pre>
 *   !row.exists("department") || "A".equals(row.getString("department"))
 * </pre>
 *
 * <p>A field that is not in the stream never throws: {@link #exists(String)} returns {@code false},
 * {@link #isNull(String)} returns {@code true} and every getter returns {@code null}. Values are
 * read through their {@link IValueMeta}, so binary-stored (lazily converted) values are converted
 * just like they are everywhere else in Hop.
 *
 * <p>A single instance is reused for every row of a transform copy, {@link #setRow(IRowMeta,
 * Object[])} points it at the row being evaluated.
 */
public class RowAccess {

  /** The name expressions use to reach this object. */
  public static final String PARAMETER_NAME = "row";

  private IRowMeta rowMeta;
  private Object[] rowData;

  /** Points this instance at the row that is being evaluated. */
  public void setRow(IRowMeta rowMeta, Object[] rowData) {
    this.rowMeta = rowMeta;
    this.rowData = rowData;
  }

  @JaninoFunction(
      name = "row.exists",
      category = "Row",
      description = "Checks whether a field is present in the stream.",
      syntax = "row.exists(\"fieldname\")",
      returns = "boolean",
      semantics =
          "Returns true when the stream has a field with this name. Use it to write conditions "
              + "for streams whose layout is only known at runtime: a field that is not "
              + "mentioned outside of row.* is never referenced by the expression.",
      examples =
          """
    [
      {
        "expression": "row.exists(\\"department\\")",
        "result": "true",
        "level": "1",
        "comment": "The stream has a department field."
      },
      {
        "expression": "!row.exists(\\"department\\") || \\"A\\".equals(row.getString(\\"department\\"))",
        "result": "true",
        "level": "1",
        "comment": "Keeps every row when the field is absent, filters on it when it is there."
      }
    ]
    """)
  public boolean exists(String fieldName) {
    return indexOf(fieldName) >= 0;
  }

  @JaninoFunction(
      name = "row.isNull",
      category = "Row",
      description = "Checks whether a field is absent or null.",
      syntax = "row.isNull(\"fieldname\")",
      returns = "boolean",
      semantics = "Returns true when the field is not in the stream or its value is null.",
      examples =
          """
    [
      {
        "expression": "row.isNull(\\"department\\")",
        "result": "true",
        "level": "1",
        "comment": "The field is absent, or present with a null value."
      }
    ]
    """)
  public boolean isNull(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 || rowMeta.getValueMeta(index).isNull(rowData[index]);
  }

  @JaninoFunction(
      name = "row.getString",
      category = "Row",
      description = "Reads a field as a String, null when the field is absent.",
      syntax = "row.getString(\"fieldname\")",
      returns = "String",
      semantics =
          "Converts the value with the metadata of the field, like every other String conversion "
              + "in Hop. Returns null when the field is not in the stream.",
      examples =
          """
    [
      {
        "expression": "\\"A\\".equals(row.getString(\\"department\\"))",
        "result": "true",
        "level": "1",
        "comment": "Null-safe comparison, also when the field does not exist."
      }
    ]
    """)
  public String getString(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 ? null : rowMeta.getString(rowData, index);
  }

  @JaninoFunction(
      name = "row.getInteger",
      category = "Row",
      description = "Reads a field as a Long, null when the field is absent.",
      syntax = "row.getInteger(\"fieldname\")",
      returns = "Long",
      semantics = "Returns null when the field is not in the stream or its value is null.",
      examples =
          """
    [
      {
        "expression": "row.getInteger(\\"id\\") != null && row.getInteger(\\"id\\").longValue() > 100",
        "result": "true",
        "level": "1",
        "comment": "Null-safe numeric comparison."
      }
    ]
    """)
  public Long getInteger(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 ? null : rowMeta.getInteger(rowData, index);
  }

  @JaninoFunction(
      name = "row.getNumber",
      category = "Row",
      description = "Reads a field as a Double, null when the field is absent.",
      syntax = "row.getNumber(\"fieldname\")",
      returns = "Double",
      semantics = "Returns null when the field is not in the stream or its value is null.",
      examples =
          """
    [
      {
        "expression": "row.getNumber(\\"price\\") != null && row.getNumber(\\"price\\").doubleValue() > 9.99",
        "result": "true",
        "level": "1",
        "comment": "Null-safe comparison on a Number field."
      }
    ]
    """)
  public Double getNumber(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 ? null : rowMeta.getNumber(rowData, index);
  }

  @JaninoFunction(
      name = "row.getBigNumber",
      category = "Row",
      description = "Reads a field as a BigDecimal, null when the field is absent.",
      syntax = "row.getBigNumber(\"fieldname\")",
      returns = "BigDecimal",
      semantics = "Returns null when the field is not in the stream or its value is null.",
      examples =
          """
    [
      {
        "expression": "row.getBigNumber(\\"amount\\") != null",
        "result": "true",
        "level": "1",
        "comment": "The field exists and has a value."
      }
    ]
    """)
  public BigDecimal getBigNumber(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 ? null : rowMeta.getBigNumber(rowData, index);
  }

  @JaninoFunction(
      name = "row.getDate",
      category = "Row",
      description = "Reads a field as a Date, null when the field is absent.",
      syntax = "row.getDate(\"fieldname\")",
      returns = "Date",
      semantics = "Returns null when the field is not in the stream or its value is null.",
      examples =
          """
    [
      {
        "expression": "row.getDate(\\"order_date\\") != null && row.getDate(\\"order_date\\").after(new java.util.Date(0))",
        "result": "true",
        "level": "1",
        "comment": "Null-safe date comparison."
      }
    ]
    """)
  public Date getDate(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 ? null : rowMeta.getDate(rowData, index);
  }

  @JaninoFunction(
      name = "row.getBoolean",
      category = "Row",
      description = "Reads a field as a Boolean, null when the field is absent.",
      syntax = "row.getBoolean(\"fieldname\")",
      returns = "Boolean",
      semantics = "Returns null when the field is not in the stream or its value is null.",
      examples =
          """
    [
      {
        "expression": "Boolean.TRUE.equals(row.getBoolean(\\"active\\"))",
        "result": "true",
        "level": "1",
        "comment": "Null-safe boolean check."
      }
    ]
    """)
  public Boolean getBoolean(String fieldName) throws HopValueException {
    int index = indexOf(fieldName);
    return index < 0 ? null : rowMeta.getBoolean(rowData, index);
  }

  /** The index of a field in the current row, -1 when the stream doesn't have it. */
  private int indexOf(String fieldName) {
    if (rowMeta == null || fieldName == null) {
      return -1;
    }
    return rowMeta.indexOfValue(fieldName);
  }
}

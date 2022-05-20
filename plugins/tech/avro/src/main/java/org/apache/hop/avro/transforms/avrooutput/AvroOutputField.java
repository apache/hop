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

package org.apache.hop.avro.transforms.avrooutput;

import org.apache.avro.Schema;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/** Describes a single field in a text file */
public class AvroOutputField implements Cloneable, Comparable<AvroOutputField> {

  @HopMetadataProperty(key = "name", injectionKeyDescription = "AvroOutput.Injection.STREAM_NAME")
  private String name;

  @HopMetadataProperty(key = "avroname", injectionKeyDescription = "AvroOutput.Injection.AVRO_PATH")
  private String avroName;

  // This allows for metadata injection to pass in a string name for the Avro Type, while still
  // maintaining backward compatibility where the Avro Type is stored as a number.
  private String avroTypeDesc;

  @HopMetadataProperty(key = "avrotype", injectionKeyDescription = "AvroOutput.Injection.AVRO_TYPE")
  private int avroType;

  @HopMetadataProperty(key = "nullable", injectionKeyDescription = "AvroOutput.Injection.NULLABLE")
  private boolean nullable;

  public static final int AVRO_TYPE_NONE = 0;
  public static final int AVRO_TYPE_BOOLEAN = 1;
  public static final int AVRO_TYPE_DOUBLE = 2;
  public static final int AVRO_TYPE_FLOAT = 3;
  public static final int AVRO_TYPE_INT = 4;
  public static final int AVRO_TYPE_LONG = 5;
  public static final int AVRO_TYPE_STRING = 6;
  public static final int AVRO_TYPE_ENUM = 7;

  private static String[] avroDescriptions = {
    "", "Boolean", "Double", "Float", "Int", "Long", "String", "Enum"
  };

  public AvroOutputField(String name, String avroName, int avroType, boolean nullable) {
    this.name = name;
    this.avroName = avroName;
    this.avroType = avroType;
    this.nullable = nullable;
  }

  public AvroOutputField(AvroOutputField f) {
    this.name = f.name;
    this.avroName = f.avroName;
    this.avroType = f.avroType;
    this.nullable = f.nullable;
  }

  public AvroOutputField() {}

  public int compare(Object obj) {
    AvroOutputField field = (AvroOutputField) obj;

    return name.compareTo(field.getName());
  }

  public boolean equal(Object obj) {
    AvroOutputField field = (AvroOutputField) obj;

    return name.equals(field.getName());
  }

  /**
   * Get the stream field name.
   *
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the stream field name.
   *
   * @param fieldname
   */
  public void setName(String fieldname) {
    this.name = fieldname;
  }

  /**
   * Get the name of the field in the Avro Schema If the Avro field name is null returns the stream
   * field name
   *
   * @return avroName
   */
  public String getAvroName() {
    return avroName != null ? avroName : name;
  }

  /**
   * Set the name of the field in the Avro Schema
   *
   * @param avroName
   */
  public void setAvroName(String avroName) {
    this.avroName = avroName;
  }

  /**
   * Return the integer value for the Avro datatype
   *
   * @return
   */
  public int getAvroType() {
    if (avroTypeDesc != null) {
      setAvroType(avroTypeDesc);
    }
    return avroType;
  }

  /**
   * Return the description of the Avro datatype
   *
   * @return avroTypeDescription
   */
  public String getAvroTypeDesc() {
    if (avroTypeDesc != null) {
      setAvroType(avroTypeDesc);
    }
    return avroDescriptions[avroType];
  }

  /**
   * Return the description of the Avro datatype
   *
   * @param avroType
   * @return avroTypeDescription
   */
  public static String getAvroTypeDesc(int avroType) {
    return avroDescriptions[avroType];
  }

  /**
   * Set if the field is nullable or not.
   *
   * @param nullable
   */
  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  /**
   * Returns the array of all Avro datatype descriptions sorted alphabetically.
   *
   * @return
   */
  public static String[] getAvroTypeArraySorted() {
    String[] sorted = avroDescriptions;
    Arrays.sort(sorted, 1, avroDescriptions.length - 1);
    return sorted;
  }

  /**
   * Set the Avro datatype
   *
   * @param avroType
   */
  public void setAvroType(int avroType) {
    this.avroType = avroType;
    avroTypeDesc = null;
  }

  /**
   * Set the Avro datatype
   *
   * @param avroTypeDesc
   */
  public void setAvroType(String avroTypeDesc) {
    for (int i = 0; i < avroDescriptions.length; i++) {
      if (avroTypeDesc.equalsIgnoreCase(avroDescriptions[i])) {
        this.avroType = i;
        break;
      }
    }
    this.avroTypeDesc = null;
  }

  public static int getDefaultAvroType(int pentahoType) {
    switch (pentahoType) {
      case IValueMeta.TYPE_NUMBER:
      case IValueMeta.TYPE_BIGNUMBER:
        return AVRO_TYPE_DOUBLE;
      case IValueMeta.TYPE_INTEGER:
        return AVRO_TYPE_LONG;
      case IValueMeta.TYPE_BOOLEAN:
        return AVRO_TYPE_BOOLEAN;
      default:
        return AVRO_TYPE_STRING;
    }
  }

  public Schema.Type getAvroSchemaType() throws HopException {
    switch (avroType) {
      case AVRO_TYPE_BOOLEAN:
        return Schema.Type.BOOLEAN;
      case AVRO_TYPE_DOUBLE:
        return Schema.Type.DOUBLE;
      case AVRO_TYPE_FLOAT:
        return Schema.Type.FLOAT;
      case AVRO_TYPE_INT:
        return Schema.Type.INT;
      case AVRO_TYPE_LONG:
        return Schema.Type.LONG;
      case AVRO_TYPE_STRING:
        return Schema.Type.STRING;
      case AVRO_TYPE_ENUM:
        return Schema.Type.ENUM;
      default:
        throw new HopException("Unsupported Avro Type " + avroDescriptions[avroType]);
    }
  }

  public void setAvroTypeDesc(String avroTypeDesc) {
    this.avroTypeDesc = avroTypeDesc;
  }

  public boolean isNullable() {
    return nullable;
  }

  public static String[] getAvroDescriptions() {
    return avroDescriptions;
  }

  public static void setAvroDescriptions(String[] avroDescriptions) {
    AvroOutputField.avroDescriptions = avroDescriptions;
  }

  public static String[] mapAvroType(Schema schema, Schema.Type type) {
    String[] avroTypeDesc = new String[1];
    switch (type) {
      case BOOLEAN:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_BOOLEAN];
        break;
      case DOUBLE:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_DOUBLE];
        break;
      case FLOAT:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_FLOAT];
        break;
      case INT:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_INT];
        break;
      case LONG:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_LONG];
        break;
      case STRING:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_STRING];
        break;
      case ENUM:
        avroTypeDesc[0] = avroDescriptions[AVRO_TYPE_ENUM];
        break;
      case UNION:
        List<Schema> schemas = schema.getTypes();
        Iterator<Schema> it = schemas.iterator();
        List<String[]> avroTypes = new ArrayList<>();
        int arrayLength = 0;
        while (it.hasNext()) {
          Schema s = it.next();
          if (s.getType() != Schema.Type.NULL) {
            String[] unionAvroType = mapAvroType(s, s.getType());
            if (unionAvroType.length > 0) {
              avroTypes.add(unionAvroType);
              arrayLength++;
            }
          }
        }

        Iterator<String[]> itUnion = avroTypes.iterator();
        avroTypeDesc = new String[arrayLength];
        int i = 0;
        while (itUnion.hasNext()) {
          String[] union = itUnion.next();
          for (int e = 0; e < union.length; e++) {
            avroTypeDesc[i] = union[e];
            i++;
          }
        }
        break;
      default:
        return new String[] {};
    }
    Arrays.sort(avroTypeDesc);
    return avroTypeDesc;
  }

  public boolean validate() throws HopException {
    if (avroType == AVRO_TYPE_NONE || avroType < 0 || avroType >= avroDescriptions.length) {
      throw new HopException("Validation error: Invalid Avro data type " + avroType);
    }

    if (name == null) {
      throw new HopException("Validation error: Stream field name is required.");
    }

    return true;
  }

  public String toString() {
    return name;
  }

  public int compareTo(AvroOutputField compareField) {

    return this.getAvroName() != null
        ? (compareField.getAvroName() != null
            ? this.getAvroName().compareTo(compareField.getAvroName())
            : -1)
        : -1;
  }
}

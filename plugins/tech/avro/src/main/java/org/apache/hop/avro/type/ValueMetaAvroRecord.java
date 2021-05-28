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

package org.apache.hop.avro.type;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.AvroSchema;
import org.apache.hop.core.WriterOutputStream;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;

@ValueMetaPlugin(
    id = "20",
    name = "Avro Record",
    description = "This type wraps around an Avro Record",
    image = "Apache_Avro_Logo.svg")
public class ValueMetaAvroRecord extends ValueMetaBase implements IValueMeta {

  public static final int TYPE_AVRO_RECORD = 20;

  public ValueMetaAvroRecord() {
    super(null, TYPE_AVRO_RECORD);
  }

  public ValueMetaAvroRecord(String name) {
    super(name, TYPE_AVRO_RECORD);
  }

  @Override
  public Object getNativeDataType(Object object) throws HopValueException {
    return getGenericRecord(object);
  }

  @Override
  public String toStringMeta() {
    return "Avro Generic Record";
  }

  public GenericRecord getGenericRecord(Object object) throws HopValueException {
    switch (type) {
      case TYPE_AVRO_RECORD:
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            return (GenericRecord) object;
          default:
            throw new HopValueException(
                "Only normal storage type is supported for the Avro GenericRecord value : "
                    + toString());
        }
      case TYPE_STRING:
        switch (storageType) {
          case STORAGE_TYPE_NORMAL:
            try {
              String jsonString = (String) object;
              return convertStringToGenericRecord(jsonString);
            } catch (Exception e) {
              throw new HopValueException(
                  "Error converting a JSON representation of an Avro GenericRecord to a native representation",
                  e);
            }
          default:
            throw new HopValueException(
                "Only normal storage type is supported for Avro GenericRecord value : "
                    + toString());
        }
      default:
        throw new HopValueException(
            "Unable to convert data type " + toString() + " to an Avro GenericRecord value");
    }
  }

  /**
   * Convert the record to both schema and data in a single JSON block...
   *
   * @param genericRecord The record to convert to JSON
   * @return The JSON representation of a generic Avro record
   * @throws HopValueException
   */
  public String convertGenericRecordToString(GenericRecord genericRecord) throws HopValueException {
    try {
      Schema schema = genericRecord.getSchema();
      String schemaJson = schema.toString();

      StringWriter stringWriter = new StringWriter();
      OutputStream outputStream = new WriterOutputStream(stringWriter);
      JsonEncoder jsonEncoder =
          EncoderFactory.get().jsonEncoder(genericRecord.getSchema(), outputStream);
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(genericRecord.getSchema());
      datumWriter.write(genericRecord, jsonEncoder);
      String dataJson = stringWriter.toString();
      String json = "{ \"schema\" : " + schemaJson + ", \"data\" : " + dataJson + " }";
      return json;

    } catch (IOException e) {
      throw new HopValueException(
          "Unable to convert an Avro record to a JSON String using the provided schema", e);
    }
  }

  public GenericRecord convertStringToGenericRecord(String jsonString) throws HopValueException {
    try {
      // Convert schema AND data to JSON...
      //
      JSONObject json = (JSONObject) new JSONParser().parse(jsonString);
      JSONObject schemaObject = (JSONObject) json.get("schema");
      JSONObject dataObject = (JSONObject) json.get("data");

      Schema schema = new Schema.Parser().parse(schemaObject.toJSONString());

      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, dataObject.toJSONString());
      GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(schema);
      return genericDatumReader.read(null, jsonDecoder);
    } catch (Exception e) {
      throw new HopValueException("Unable to convert a String to an Avro record", e);
    }
  }

  /**
   * Convert Avro Schema value to String...
   *
   * @param object The object to convert to String
   * @return The String representation
   * @throws HopValueException
   */
  @Override
  public String getString(Object object) throws HopValueException {
    try {
      String string;

      switch (type) {
        case TYPE_STRING:
          switch (storageType) {
            case STORAGE_TYPE_NORMAL:
              string = object == null ? null : convertGenericRecordToString((GenericRecord) object);
              break;
            case STORAGE_TYPE_BINARY_STRING:
              string = (String) convertBinaryStringToNativeType((byte[]) object);
              break;
            case STORAGE_TYPE_INDEXED:
              string = object == null ? null : (String) index[(Integer) object];
              break;
            default:
              throw new HopValueException(
                  toString() + " : Unknown storage type " + storageType + " specified.");
          }
          if (string != null) {
            string = trim(string);
          }
          break;

        case TYPE_DATE:
          throw new HopValueException(
              "You can't convert a Date to an Avro GenericRecord data type for : " + toString());

        case TYPE_NUMBER:
          throw new HopValueException(
              "You can't convert a Number to an Avro GenericRecord data type for : " + toString());

        case TYPE_INTEGER:
          throw new HopValueException(
              "You can't convert an Integer to an Avro GenericRecord data type for : "
                  + toString());

        case TYPE_BIGNUMBER:
          throw new HopValueException(
              "You can't convert a BigNumber to an Avro GenericRecord data type for : "
                  + toString());

        case TYPE_BOOLEAN:
          throw new HopValueException(
              "You can't convert a Boolean to an Avro GenericRecord data type for : " + toString());

        case TYPE_BINARY:
          switch (storageType) {
            case STORAGE_TYPE_NORMAL:
              string = convertBinaryStringToString((byte[]) object);
              break;
            case STORAGE_TYPE_BINARY_STRING:
              string = convertBinaryStringToString((byte[]) object);
              break;
            case STORAGE_TYPE_INDEXED:
              string =
                  object == null
                      ? null
                      : convertBinaryStringToString((byte[]) index[(Integer) object]);
              break;
            default:
              throw new HopValueException(
                  toString() + " : Unknown storage type " + storageType + " specified.");
          }
          break;

        case TYPE_SERIALIZABLE:
          switch (storageType) {
            case STORAGE_TYPE_NORMAL:
              string = object == null ? null : object.toString();
              break; // just go for the default toString()
            case STORAGE_TYPE_BINARY_STRING:
              string = convertBinaryStringToString((byte[]) object);
              break;
            case STORAGE_TYPE_INDEXED:
              string = object == null ? null : index[((Integer) object).intValue()].toString();
              break; // just go for the default toString()
            default:
              throw new HopValueException(
                  toString() + " : Unknown storage type " + storageType + " specified.");
          }
          break;

        case TYPE_AVRO_RECORD:
          switch (storageType) {
            case STORAGE_TYPE_NORMAL:
              string = object == null ? null : object.toString();
              break;
            default:
              throw new HopValueException(
                  toString()
                      + " : Unsupported storage type "
                      + getStorageTypeDesc()
                      + " for "
                      + toString());
          }
          break;

        default:
          throw new HopValueException(toString() + " : Unknown type " + type + " specified.");
      }

      if (isOutputPaddingEnabled() && getLength() > 0) {
        string = ValueDataUtil.rightPad(string, getLength());
      }

      return string;
    } catch (ClassCastException e) {
      throw new HopValueException(
          toString()
              + " : There was a data type error: the data type of "
              + object.getClass().getName()
              + " object ["
              + object
              + "] does not correspond to value meta ["
              + toStringMeta()
              + "]");
    }
  }

  public Object cloneValueData(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }

    GenericRecord genericRecord = getGenericRecord(object);
    String jsonString = convertGenericRecordToString(genericRecord);
    return convertStringToGenericRecord(jsonString);
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return GenericRecord.class;
  }
}

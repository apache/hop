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

package org.apache.hop.core.row.value;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueDataUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.server.HttpUtil;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;

@ValueMetaPlugin(
    id = "20",
    name = "Avro Record",
    description = "This type wraps around an Avro Record",
    image = "images/avro.svg")
public class ValueMetaAvroRecord extends ValueMetaBase implements IValueMeta {

  private Schema schema;

  public ValueMetaAvroRecord() {
    super(null, IValueMeta.TYPE_AVRO);
  }

  public ValueMetaAvroRecord(String name) {
    super(name, IValueMeta.TYPE_AVRO);
  }

  public ValueMetaAvroRecord(String name, Schema schema) {
    super(name, IValueMeta.TYPE_AVRO);
    this.schema = schema;
  }

  public ValueMetaAvroRecord(ValueMetaAvroRecord meta) {
    super(meta.name, IValueMeta.TYPE_AVRO);
    if (meta.schema != null) {
      this.schema = new Schema.Parser().parse(meta.schema.toString());
    }
  }

  @Override
  public ValueMetaAvroRecord clone() {
    return new ValueMetaAvroRecord(this);
  }

  @Override
  public Object getNativeDataType(Object object) throws HopValueException {
    return getGenericRecord(object);
  }

  @Override
  public String toStringMeta() {
    if (schema == null) {
      return "Avro Generic Record";
    } else {
      return "Avro Generic Record " + schema.toString(false);
    }
  }

  public GenericRecord getGenericRecord(Object object) throws HopValueException {
    switch (type) {
      case IValueMeta.TYPE_AVRO:
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
  public static String convertGenericRecordToString(GenericRecord genericRecord)
      throws HopValueException {
    try {
      Schema schema = genericRecord.getSchema();
      String schemaJson = schema.toString();
      String dataJson = genericRecord.toString();

      String json = "{ \"schema\" : " + schemaJson + ", \"data\" : " + dataJson + " }";
      return json;
    } catch (Exception e) {
      throw new HopValueException(
          "Unable to convert an Avro record to a JSON String using the provided schema", e);
    }
  }

  public static GenericRecord convertStringToGenericRecord(String jsonString)
      throws HopValueException {
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

        case IValueMeta.TYPE_AVRO:
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

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    if (object == null) {
      return null;
    }

    GenericRecord genericRecord = getGenericRecord(object);
    Schema schema = genericRecord.getSchema();

    // Create a new record and copy over all the values
    //
    GenericRecord copy = new GenericData.Record(schema);

    // We consider all the values to be primitives, not in need of clone
    //
    for (Schema.Field field : schema.getFields()) {
      Object v = genericRecord.get(field.name());
      copy.put(field.name(), v);
    }

    return copy;
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return GenericRecord.class;
  }

  @Override
  public void writeMeta(DataOutputStream outputStream) throws HopFileException {
    try {
      // First write the basic metadata
      //
      super.writeMeta(outputStream);

      // Also output the schema metadata in JSON format...
      //
      if (schema == null) {
        outputStream.writeUTF("");
      } else {
        outputStream.writeUTF(schema.toString(false));
      }
    } catch (Exception e) {
      throw new HopFileException("Error writing Avro Record metadata", e);
    }
  }

  @Override
  public void readMetaData(DataInputStream inputStream) throws HopFileException {
    try {
      // First read the basic type metada data
      //
      super.readMetaData(inputStream);

      // Now read the schema JSON
      //
      String schemaJson = inputStream.readUTF();
      if (StringUtils.isEmpty(schemaJson)) {
        schema = null;
      } else {
        schema = new Schema.Parser().parse(schemaJson);
      }
    } catch (Exception e) {
      throw new HopFileException("Error read Avro Record metadata", e);
    }
  }

  @Override
  public String getMetaXml() throws IOException {
    StringBuilder xml = new StringBuilder();

    xml.append(XmlHandler.openTag(XML_META_TAG));

    xml.append(XmlHandler.addTagValue("type", getTypeDesc()));
    xml.append(XmlHandler.addTagValue("storagetype", getStorageTypeCode(getStorageType())));

    // Just append the schema JSON as a compressed base64 encoded string...
    //
    if (schema != null) {
      xml.append(
          XmlHandler.addTagValue(
              "schema", HttpUtil.encodeBase64ZippedString(schema.toString(false))));
    }
    xml.append(XmlHandler.closeTag(XML_META_TAG));

    return super.getMetaXml();
  }

  @Override
  public void storeMetaInJson(JSONObject jValue) throws HopException {
    // Store the absolute basics (name, type, ...)
    super.storeMetaInJson(jValue);

    // And the schema JSON (if any)
    //
    try {
      if (schema != null) {
        String schemaJson = schema.toString(false);
        Object jSchema = new JSONParser().parse(schemaJson);
        jValue.put("schema", jSchema);
      }
    } catch (Exception e) {
      throw new HopException(
          "Error encoding Avro schema as JSON in value metadata of field " + name, e);
    }
  }

  @Override
  public void loadMetaFromJson(JSONObject jValue) {
    // Load the basic metadata
    //
    super.loadMetaFromJson(jValue);

    // Load the schema (if any)...
    //
    Object jSchema = jValue.get("schema");
    if (jSchema != null) {
      String schemaJson = ((JSONObject) jSchema).toJSONString();
      schema = new Schema.Parser().parse(schemaJson);
    } else {
      schema = null;
    }
  }

  @Override
  public void writeData(DataOutputStream outputStream, Object object) throws HopFileException {
    try {
      // Is the value NULL?
      outputStream.writeBoolean(object == null);

      if (object != null) {
        GenericRecord genericRecord = (GenericRecord) object;

        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> datumWriter =
            new GenericDatumWriter<>(genericRecord.getSchema());
        datumWriter.write(genericRecord, binaryEncoder);
      }
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to write value data to output stream", e);
    }
  }

  @Override
  public Object readData(DataInputStream inputStream)
      throws HopFileException, SocketTimeoutException {
    try {
      // Is the value NULL?
      if (inputStream.readBoolean()) {
        return null; // done
      }

      // De-serialize a GenericRow object
      //
      if (schema == null) {
        throw new HopFileException(
            "An Avro schema is needed to read a GenericRecord from an input stream");
      }

      BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(inputStream, null);
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
      GenericRecord genericRecord = datumReader.read(null, binaryDecoder);

      return genericRecord;
    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException(toString() + " : Unable to read value data from input stream", e);
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  public String getComments() {
    if (StringUtils.isEmpty(super.comments)) {
      if (schema != null) {
        return schema.toString(false);
      }
    }
    return super.getComments();
  }

  /**
   * Try to get an Integer from an Avro value
   *
   * @param object
   * @return
   * @throws HopValueException
   */
  @Override
  public Long getInteger(Object object) throws HopValueException {

    return super.getInteger(object);
  }
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.commons.lang3.StringUtils;
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

@ValueMetaPlugin(
    id = "20",
    name = "Avro Record",
    description = "This type wraps around an Avro Record",
    image = "images/avro.svg")
public class ValueMetaAvroRecord extends ValueMetaBase {

  private Schema schema;
  private static final String CONST_SCHEMA = "schema";
  private static final String CONST_SPECIFIED = " specified.";
  private static final String CONST_UNKNOWN_STORAGE_TYPE = " : Unknown storage type ";
  private static final String CONST_SCHEMA_NEEDED =
      "An Avro schema is needed to read a GenericRecord from an input stream";

  /** Reject a corrupt length instead of allocating it. Sample rows are far smaller than this. */
  private static final int MAX_AVRO_BYTES = 32 * 1024 * 1024;

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
        return switch (storageType) {
          case STORAGE_TYPE_NORMAL -> (GenericRecord) object;
          default ->
              throw new HopValueException(
                  "Only normal storage type is supported for the Avro GenericRecord value : "
                      + this);
        };
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
                "Only normal storage type is supported for Avro GenericRecord value : " + this);
        }
      default:
        throw new HopValueException(
            "Unable to convert data type " + this + " to an Avro GenericRecord value");
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

      return "{ \"schema\" : " + schemaJson + ", \"data\" : " + dataJson + " }";
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
      JSONObject schemaObject = (JSONObject) json.get(CONST_SCHEMA);
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
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL ->
                    object == null ? null : convertGenericRecordToString((GenericRecord) object);
                case STORAGE_TYPE_BINARY_STRING ->
                    (String) convertBinaryStringToNativeType((byte[]) object);
                case STORAGE_TYPE_INDEXED ->
                    object == null ? null : (String) index[(Integer) object];
                default ->
                    throw new HopValueException(
                        this + CONST_UNKNOWN_STORAGE_TYPE + storageType + CONST_SPECIFIED);
              };
          if (string != null) {
            string = trim(string);
          }
          break;

        case TYPE_DATE:
          throw new HopValueException(
              "You can't convert a Date to an Avro GenericRecord data type for : " + this);

        case TYPE_NUMBER:
          throw new HopValueException(
              "You can't convert a Number to an Avro GenericRecord data type for : " + this);

        case TYPE_INTEGER:
          throw new HopValueException(
              "You can't convert an Integer to an Avro GenericRecord data type for : " + this);

        case TYPE_BIGNUMBER:
          throw new HopValueException(
              "You can't convert a BigNumber to an Avro GenericRecord data type for : " + this);

        case TYPE_BOOLEAN:
          throw new HopValueException(
              "You can't convert a Boolean to an Avro GenericRecord data type for : " + this);

        case TYPE_BINARY:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL -> convertBinaryStringToString((byte[]) object);
                case STORAGE_TYPE_BINARY_STRING -> convertBinaryStringToString((byte[]) object);
                case STORAGE_TYPE_INDEXED ->
                    object == null
                        ? null
                        : convertBinaryStringToString((byte[]) index[(Integer) object]);
                default ->
                    throw new HopValueException(
                        this + CONST_UNKNOWN_STORAGE_TYPE + storageType + CONST_SPECIFIED);
              };
          break;

        case TYPE_SERIALIZABLE:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL ->
                    object == null ? null : object.toString(); // just go for the default toString()
                case STORAGE_TYPE_BINARY_STRING -> convertBinaryStringToString((byte[]) object);
                case STORAGE_TYPE_INDEXED ->
                    object == null
                        ? null
                        : index[(Integer) object].toString(); // just go for the default toString()
                default ->
                    throw new HopValueException(
                        this + CONST_UNKNOWN_STORAGE_TYPE + storageType + CONST_SPECIFIED);
              };
          break;

        case IValueMeta.TYPE_AVRO:
          string =
              switch (storageType) {
                case STORAGE_TYPE_NORMAL -> object == null ? null : object.toString();
                default ->
                    throw new HopValueException(
                        this
                            + " : Unsupported storage type "
                            + getStorageTypeDesc()
                            + " for "
                            + this);
              };
          break;

        default:
          throw new HopValueException(this + " : Unknown type " + type + CONST_SPECIFIED);
      }

      if (isOutputPaddingEnabled() && getLength() > 0) {
        string = ValueDataUtil.rightPad(string, getLength());
      }

      return string;
    } catch (ClassCastException e) {
      throw new HopValueException(
          this
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
              CONST_SCHEMA, HttpUtil.encodeBase64ZippedString(schema.toString(false))));
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
        jValue.put(CONST_SCHEMA, jSchema);
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
    Object jSchema = jValue.get(CONST_SCHEMA);
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
        // Schema and datum are length-prefixed so the rest of the row stays aligned, and so a
        // field with no schema on its metadata (Avro File Input, Kafka Consumer) can be read back.
        outputStream.write(encodeRecord(object));
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

      return decodeRecord(inputStream);
    } catch (HopEofException e) {
      throw e;
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (HopFileException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to read value data from input stream", e);
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
    if (StringUtils.isEmpty(super.comments) && schema != null) {
      return schema.toString(false);
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

  /**
   * Minimum and maximum profiling, and row-buffer equality, call this. Avro has no single ordering,
   * so the record text already shown in the execution grid is used.
   */
  @Override
  protected int typeCompare(Object data1, Object data2) throws HopValueException {
    String one = getString(data1);
    String two = getString(data2);
    if (one == null && two == null) {
      return 0;
    }
    if (one == null) {
      return -1;
    }
    if (two == null) {
      return 1;
    }
    return one.compareTo(two);
  }

  @Override
  public boolean requiresRealClone() {
    return true;
  }

  /**
   * Encode a record so it can be stored without a schema on the value metadata. The bytes are the
   * schema JSON (4-byte length, UTF-8) followed by the Avro binary datum (4-byte length).
   */
  public static byte[] encodeRecord(Object object) throws HopFileException {
    GenericRecord genericRecord = toGenericRecord(object);
    Schema recordSchema = genericRecord.getSchema();
    if (recordSchema == null) {
      throw new HopFileException(CONST_SCHEMA_NEEDED);
    }
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      byte[] schemaBytes = recordSchema.toString(false).getBytes(StandardCharsets.UTF_8);
      dos.writeInt(schemaBytes.length);
      dos.write(schemaBytes);

      ByteArrayOutputStream avroBytes = new ByteArrayOutputStream();
      BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(avroBytes, null);
      new GenericDatumWriter<GenericRecord>(recordSchema).write(genericRecord, binaryEncoder);
      binaryEncoder.flush();
      byte[] data = avroBytes.toByteArray();
      dos.writeInt(data.length);
      dos.write(data);
      dos.flush();
      return baos.toByteArray();
    } catch (Exception e) {
      throw new HopFileException("Unable to encode an Avro record", e);
    }
  }

  /** Decode a payload produced by {@link #encodeRecord(Object)}. */
  public static GenericRecord decodeRecord(byte[] payload) throws HopFileException {
    if (payload == null) {
      return null;
    }
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload))) {
      return decodeRecord(dis);
    } catch (HopFileException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException("Unable to decode an Avro record", e);
    }
  }

  private static GenericRecord toGenericRecord(Object object) throws HopFileException {
    if (object instanceof GenericRecord genericRecord) {
      return genericRecord;
    }
    throw new HopFileException(
        "Expected an Avro GenericRecord and got "
            + (object == null ? "null" : object.getClass().getName()));
  }

  private static GenericRecord decodeRecord(DataInputStream inputStream) throws HopFileException {
    try {
      byte[] schemaBytes = readBounded(inputStream);
      String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
      if (StringUtils.isEmpty(schemaJson)) {
        throw new HopFileException(CONST_SCHEMA_NEEDED);
      }
      Schema recordSchema = new Schema.Parser().parse(schemaJson);
      byte[] data = readBounded(inputStream);
      BinaryDecoder binaryDecoder =
          DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
      return new GenericDatumReader<GenericRecord>(recordSchema).read(null, binaryDecoder);
    } catch (HopFileException e) {
      throw e;
    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (Exception e) {
      throw new HopFileException("Unable to decode an Avro record", e);
    }
  }

  private static byte[] readBounded(DataInputStream inputStream)
      throws IOException, HopFileException {
    int length = inputStream.readInt();
    if (length < 0 || length > MAX_AVRO_BYTES) {
      throw new HopFileException("Avro value length " + length + " is not valid");
    }
    byte[] bytes = new byte[length];
    inputStream.readFully(bytes);
    return bytes;
  }
}

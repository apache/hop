package org.apache.hop.core.row.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.ArrowBufferAllocator;
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
    id = "21",
    name = "Arrow RecordBatch Record",
    description = "This type wraps an Arrow RecordBatch",
    image = "images/arrow.svg")
public class ValueMetaArrowRecordBatch extends ValueMetaBase implements IValueMeta {

  private Schema schema;

  public ValueMetaArrowRecordBatch() {
    super(null, IValueMeta.TYPE_ARROW);
  }

  public ValueMetaArrowRecordBatch(String name) {
    super(name, IValueMeta.TYPE_ARROW);
  }

  public ValueMetaArrowRecordBatch(String name, Schema schema) {
    super(name, IValueMeta.TYPE_ARROW);
    this.schema = schema;
  }

  public ValueMetaArrowRecordBatch(ValueMetaArrowRecordBatch meta) {
    this(meta.name, meta.schema);
  }

  @Override
  public String toStringMeta() {
    if (this.schema == null) {
      return "Arrow Record";
    } else {
      return "Arrow Record " + this.schema;
    }
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
        outputStream.writeUTF(schema.toJson());
      }
    } catch (Exception e) {
      throw new HopFileException("Error writing Arrow record metadata", e);
    }
  }

  @Override
  public void readMetaData(DataInputStream inputStream) throws HopFileException {
    try {
      // First read the basic type metadata
      //
      super.readMetaData(inputStream);

      // Now read the schema JSON
      //
      String schemaJson = inputStream.readUTF();
      if (StringUtils.isEmpty(schemaJson)) {
        schema = null;
      } else {
        schema = Schema.fromJSON(schemaJson);
      }
    } catch (Exception e) {
      throw new HopFileException("Error read Arrow record metadata", e);
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
              "schema", HttpUtil.encodeBase64ZippedString(schema.toJson())));
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
        String schemaJson = schema.toJson();
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
      try {
        schema = Schema.fromJSON(schemaJson);
      } catch (IOException e) {
        // XXX log exception
        schema = null;
      }
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
        ArrowRecordBatch batch = (ArrowRecordBatch) object;
        BufferAllocator allocator = ArrowBufferAllocator.rootAllocator();

        try (
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
          VectorLoader loader = new VectorLoader(root);
          loader.load(batch);

          writer.start();
          writer.writeBatch();
        }
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

      // De-serialize a Arrow IPC object
      //
      if (schema == null) {
        throw new HopFileException(
            "An Avro schema is needed to read a GenericRecord from an input stream");
      }

      BufferAllocator allocator = ArrowBufferAllocator.rootAllocator();
      try (ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        VectorUnloader unloader = new VectorUnloader(root);
        return unloader.getRecordBatch();
      }
    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException(toString() + " : Unable to read value data from input stream", e);
    }
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws HopValueException {
    return ArrowRecordBatch.class;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }
}

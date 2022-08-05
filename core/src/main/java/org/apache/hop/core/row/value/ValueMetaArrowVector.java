package org.apache.hop.core.row.value;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@ValueMetaPlugin(
    id = "21",
    name = "Arrow Vector",
    description = "A single Arrow Vector",
    image = "images/arrow.svg")
public class ValueMetaArrowVector extends ValueMetaBase implements IValueMeta {

  private Schema schema;

  public ValueMetaArrowVector() {
    super(null, IValueMeta.TYPE_ARROW);
  }

  public ValueMetaArrowVector(String name) {
    super(name, IValueMeta.TYPE_ARROW);
  }

  public ValueMetaArrowVector(String name, Schema schema) {
    this(name);
    this.schema = schema;
  }

  @Override
  public String toStringMeta() {
    if (schema == null) {
      return "Arrow Vector";
    }
    return "Arrow Vector [" + schema + "]";
  }

  @Override
  public void writeMeta(DataOutputStream outputStream) throws HopFileException {
    try {
      // First write the basic metadata.
      //
      super.writeMeta(outputStream);

      // Serialize the Schema as JSON.
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
      // First read the basic type metadata.
      //
      super.readMetaData(inputStream);

      // Now read the schema JSON.
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

    // And the schema JSON (if any).
    //
    try {
      if (schema != null) {
        Object jSchema = new JSONParser().parse(schema.toJson());
        jValue.put("field", jSchema);
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
      boolean isNull = object == null;
      outputStream.writeBoolean(isNull);
      if (isNull) {
        return;
      }

      if (!(object instanceof FieldVector[])) {
        throw new HopFileException(this + " : expected FieldVector[], got " + object.getClass().getCanonicalName());
      }
      try (VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList((FieldVector[]) object));
           ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outputStream)) {
        writer.writeBatch();
      }
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to write value data to output stream", e);
    }
  }

  @Override
  public Object readData(DataInputStream inputStream)
      throws HopFileException {
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
      try (ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
           VectorSchemaRoot root = reader.getVectorSchemaRoot()) {

        // XXX need to think about how we'd handle multiple batches
        if (reader.loadNextBatch()) {
          return root.getFieldVectors().toArray(FieldVector[]::new);
        }
      }
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to read value data from input stream", e);
    }
    throw new HopFileException(this + " : Unexpected failure reading value data from input stream");
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

  public List<ValueVector> getValueVectors(Object o) {
    // TODO: validate o? what should we do here?
    return (List<ValueVector>) o;
  }
}

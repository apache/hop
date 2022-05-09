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
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/** Converts input rows to avro and then writes this avro to one or more files. */
public class AvroOutput extends BaseTransform<AvroOutputMeta, AvroOutputData> {
  private static final Class<?> PKG = AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private List<AvroOutputField> avroOutputFields;
  private int outputFieldIndex;

  public AvroOutput(
      TransformMeta transformMeta,
      AvroOutputMeta meta,
      AvroOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private GenericRecord getRecord(Object[] r, String parentPath, Schema inputSchema)
      throws HopException {
    String parentName = "";
    if (parentPath != null) {
      parentName = parentPath;
    }

    Schema recordSchema = inputSchema;

    List<Schema> unionSchemas = null;
    if (inputSchema.getType() == Schema.Type.UNION) {
      unionSchemas = inputSchema.getTypes();
      if (unionSchemas != null) {
        for (int i = 0; i < unionSchemas.size(); i++) {
          if (unionSchemas.get(i).getType() == Schema.Type.RECORD) {
            recordSchema = unionSchemas.get(i);
            break;
          }
        }
      }
    }

    GenericRecord result = new GenericData.Record(recordSchema);
    while (outputFieldIndex < avroOutputFields.size()) {

      AvroOutputField aof = avroOutputFields.get(outputFieldIndex);
      String avroName = aof.getAvroName();

      if (avroName.startsWith("$.")) {
        avroName = avroName.substring(2);
      }
      if (parentName == null || parentName.length() == 0 || avroName.startsWith(parentName + ".")) {
        if (parentName != null && parentName.length() > 0) {
          avroName = avroName.substring(parentName.length() + 1);
        }
        if (avroName.contains(".")) {
          String currentAvroPath = avroName.substring(0, avroName.indexOf("."));
          Schema childSchema = recordSchema.getField(currentAvroPath).schema();
          String childPath = parentName + "." + currentAvroPath;
          if (parentName == null || parentName.length() == 0) {
            childPath = currentAvroPath;
          }

          GenericRecord fieldRecord = getRecord(r, childPath, childSchema);
          result.put(currentAvroPath, fieldRecord);
        } else {
          Field avroField = recordSchema.getField(avroName);
          Object value =
              getValue(
                  r,
                  meta.getOutputFields().get(outputFieldIndex),
                  data.fieldnrs[outputFieldIndex],
                  avroField);
          if (value != null) {
            result.put(avroName, value);
          }
          outputFieldIndex++;
        }
      } else {
        break;
      }
    }
    return result;
  }

  public Schema createAvroSchema(List<AvroOutputField> avroFields, String parentPath)
      throws HopException {
    // Get standard schema stuff
    String doc = meta.getDoc();
    String recordName = meta.getRecordName();
    String namespace = meta.getNamespace();

    // do not want to have to deal with $. paths.
    if (parentPath.startsWith("$.")) {
      parentPath = parentPath.substring(2);
    }

    if (parentPath.endsWith(".")) {
      parentPath = parentPath.substring(0, parentPath.length() - 1);
    }

    // If the parent path is not empty the doc and recordname should not be the default
    if (!parentPath.isEmpty()) {
      doc = "Auto generated for path " + parentPath;
      recordName = parentPath.replaceAll("[^A-Za-z0-9\\_]", "_");
    }

    // Create the result schema
    Schema result = Schema.createRecord(recordName, doc, namespace, false);

    List<Schema.Field> resultFields = new ArrayList<>();

    // Can not use an iterator because we will change the list in the middle of this loop
    for (int i = 0; i < avroFields.size(); i++) {
      if (avroFields.get(i) != null) {
        AvroOutputField field = avroFields.get(i);

        String avroName = field.getAvroName();

        // Get rid of the $. stuff
        if (avroName.startsWith("$.")) {
          avroName = avroName.substring(2);
        }

        // The avroName includes the parent path.  We do not want the parent path for our
        // evaluation.
        String finalName = avroName;
        if (!parentPath.isEmpty()) {
          finalName = avroName.substring(parentPath.length() + 1);
        }

        if (finalName.contains(".")) // It has children, perform children processing.
        {
          StringBuilder builder = new StringBuilder();
          if (!parentPath.isEmpty()) {
            builder.append(parentPath).append(".");
          }
          builder.append(finalName.substring(0, finalName.indexOf("."))).append(".");
          String subPath = builder.toString();
          List<AvroOutputField> subFields = new ArrayList<>();
          subFields.add(field);
          boolean nullable = field.isNullable();
          for (int e = i + 1; e < avroFields.size(); e++) {
            if (avroFields.get(e) != null) {
              AvroOutputField subFieldCandidate = avroFields.get(e);

              String candidateName = subFieldCandidate.getAvroName();
              if (candidateName.startsWith("$.")) {
                candidateName = candidateName.substring(2);
              }

              if (candidateName.startsWith(subPath)) {
                if (nullable) {
                  nullable = subFieldCandidate.isNullable();
                }

                subFields.add(subFieldCandidate);
                avroFields.remove(e);
                e--;
              }
            }
          }
          subPath = subPath.substring(0, subPath.length() - 1);

          Schema subSchema = createAvroSchema(subFields, subPath);
          Schema outSchema = subSchema;
          if (nullable) {
            Schema nullSchema = Schema.create(Schema.Type.NULL);
            List<Schema> unionList = new ArrayList<>();
            unionList.add(nullSchema);
            unionList.add(subSchema);
            Schema unionSchema = Schema.createUnion(unionList);
            outSchema = unionSchema;
          }
          Schema.Field schemaField =
              new Schema.Field(
                  finalName.substring(0, finalName.indexOf(".")), outSchema, null, null);
          resultFields.add(schemaField);
        } else { // Is not a sub field create the field.
          Schema fieldSchema = Schema.create(field.getAvroSchemaType());
          Schema outSchema;
          if (field.isNullable()) {
            Schema nullSchema = Schema.create(Schema.Type.NULL);

            List<Schema> unionSchema = new ArrayList<>();
            unionSchema.add(nullSchema);
            unionSchema.add(fieldSchema);
            outSchema = Schema.createUnion(unionSchema);
          } else {
            outSchema = fieldSchema;
          }
          Schema.Field outField = new Schema.Field(finalName, outSchema, null, null);
          resultFields.add(outField);
        }
      }
    }

    result.setFields(resultFields);
    return result;
  }

  public void writeSchemaFile() throws HopException {
    List<AvroOutputField> fields = new ArrayList<>();
    for (AvroOutputField avroField : meta.getOutputFields()) {
      fields.add(avroField);
    }
    data.avroSchema = createAvroSchema(fields, "");
    if (log.isDetailed()) {
      logDetailed("Automatically generated Avro schema.");
    }

    if (meta.isWriteSchemaFile()) {
      if (log.isDetailed()) {
        logDetailed("Writing schema file.");
      }
      try {
        String schemaFileName = buildFilename(resolve(meta.getSchemaFileName()), true);
        if (meta.isCreateParentFolder()) {
          logDetailed("Creating parent folder for schema file");
          createParentFolder(schemaFileName);
        }
        OutputStream outputStream = getOutputStream(schemaFileName, false);

        if (log.isDetailed()) {
          logDetailed("Opening output stream in default encoding");
        }
        OutputStream schemaWriter = new BufferedOutputStream(outputStream, 5000);

        if (log.isDetailed()) {
          logDetailed("Opened new file with name [" + schemaFileName + "]");
        }

        schemaWriter.write(data.avroSchema.toString(true).getBytes());
        schemaWriter.close();
        if (log.isDetailed()) {
          logDetailed("Closed schema file with name [" + schemaFileName + "]");
        }

      } catch (Exception e) {
        throw new HopException("Error opening new file : " + e.toString());
      }
    }
  }

  @Override
  public synchronized boolean processRow() throws HopException {

    boolean result = true;
    Object[] r = getRow(); // This also waits for a row to be finished.

    if (first) {
      first = false;

      avroOutputFields = meta.getOutputFields();

      try {
        if (meta.isCreateSchemaFile()) {
          logDetailed("Generating Avro schema.");
          writeSchemaFile();
        } else {
          logDetailed("Reading Avro schema from file.");
          try {
            data.avroSchema = new Schema.Parser().parse(new File(meta.getSchemaFileName()));
          } catch (Exception e) {
            logError("Error parsing schema file", e);
          }
        }
        data.datumWriter = new GenericDatumWriter<GenericRecord>(data.avroSchema);

        if (meta.getOutputType()
            .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_FIELD])) {
          data.encoderFactory = new EncoderFactory();
          data.byteArrayOutputStream = new ByteArrayOutputStream();
          data.binaryEncoder = data.encoderFactory.binaryEncoder(data.byteArrayOutputStream, null);
        } else if (meta.getOutputType()
            .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE])) {
          data.dataFileWriter = new DataFileWriter<GenericRecord>(data.datumWriter);
          if (!Utils.isEmpty(meta.getCompressionType())
              && !meta.getCompressionType().equalsIgnoreCase("none")) {
            data.dataFileWriter.setCodec(CodecFactory.fromString(meta.getCompressionType()));
          }
          data.dataFileWriter.create(data.avroSchema, data.writer);
        } else if (meta.getOutputType()
            .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_JSON_FIELD])) {
          data.encoderFactory = new EncoderFactory();
          data.byteArrayOutputStream = new ByteArrayOutputStream();
          data.jsonEncoder =
              data.encoderFactory.jsonEncoder(data.avroSchema, data.byteArrayOutputStream);
        } else {
          throw new HopException("Invalid output type " + meta.getOutputType());
        }
      } catch (IOException ex) {
        logError("Could not open Avro writer", ex);
        setErrors(1L);
        stopAll();
        return false;
      }
      if (r != null) {

        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

        data.fieldnrs = new int[avroOutputFields.size()];
        for (int i = 0; i < avroOutputFields.size(); i++) {
          if (avroOutputFields.get(i).validate()) {
            data.fieldnrs[i] = data.outputRowMeta.indexOfValue(avroOutputFields.get(i).getName());
            if (data.fieldnrs[i] < 0) {
              throw new HopException(
                  "Field ["
                      + avroOutputFields.get(i).getName()
                      + "] couldn't be found in the input stream!");
            }
          }
        }
      }
    }

    if (r == null) {
      // no more input to be expected...
      if (meta.getOutputType()
          .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_FIELD])) {
        try {
          data.binaryEncoder = null;
          data.jsonEncoder = null;
          if (data.byteArrayOutputStream != null) {
            data.byteArrayOutputStream.close();
          }
          data.encoderFactory = null;
        } catch (Exception ex) {
          throw new HopException("Error cleaning up transform", ex);
        }
      } else if (meta.getOutputType()
          .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE])) {
        closeFile();
      }
      setOutputDone();
      data.datumWriter = null;
      data.avroSchema = null;
      return false;
    }

    outputFieldIndex = 0;
    GenericRecord row = getRecord(r, null, data.avroSchema);

    try {
      if (meta.getOutputType()
          .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE])) {
        data.dataFileWriter.append(row);
      } else if (meta.getOutputType()
          .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_FIELD])) {
        data.datumWriter.write(row, data.binaryEncoder);
        data.binaryEncoder.flush();
        data.byteArrayOutputStream.flush();
        RowDataUtil.addValueData(
            r, data.outputRowMeta.size() - 1, data.byteArrayOutputStream.toByteArray());
        data.byteArrayOutputStream.close();
        data.byteArrayOutputStream.reset();
      } else if (meta.getOutputType()
          .equals(AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_JSON_FIELD])) {
        data.datumWriter.write(row, data.jsonEncoder);
        data.jsonEncoder.flush();
        data.byteArrayOutputStream.flush();
        RowDataUtil.addValueData(
            r, data.outputRowMeta.size() - 1, data.byteArrayOutputStream.toString());
        data.byteArrayOutputStream.close();
        data.byteArrayOutputStream.reset();
      }
    } catch (IOException i) {
      throw new HopException(i);
    }

    // First handle the file name in field
    // Write a header line as well if needed
    //
    putRow(data.outputRowMeta, r); // in case we want it to go further...

    if (checkFeedback(getLinesOutput())) {
      logBasic("linenr " + getLinesOutput());
    }

    return result;
  }

  public Object getValue(
      Object[] r, AvroOutputField outputField, int inputFieldIndex, Field fieldSchema)
      throws HopException {
    Object value;

    switch (outputField.getAvroType()) {
      case AvroOutputField.AVRO_TYPE_INT:
        value = data.outputRowMeta.getInteger(r, inputFieldIndex).intValue();
        break;
      case AvroOutputField.AVRO_TYPE_STRING:
        value = data.outputRowMeta.getString(r, inputFieldIndex);
        break;
      case AvroOutputField.AVRO_TYPE_LONG:
        value = data.outputRowMeta.getInteger(r, inputFieldIndex);
        break;
      case AvroOutputField.AVRO_TYPE_FLOAT:
        value = data.outputRowMeta.getNumber(r, inputFieldIndex).floatValue();
        break;
      case AvroOutputField.AVRO_TYPE_DOUBLE:
        value = data.outputRowMeta.getNumber(r, inputFieldIndex);
        break;
      case AvroOutputField.AVRO_TYPE_BOOLEAN:
        value = data.outputRowMeta.getBoolean(r, inputFieldIndex);
        break;
      case AvroOutputField.AVRO_TYPE_ENUM:
        Schema schema = getFirstEnumSchema(fieldSchema);
        String fieldValue = data.outputRowMeta.getString(r, inputFieldIndex);
        value = new GenericData.EnumSymbol(schema, fieldValue);
        break;
      default:
        throw new HopException(
            "Avro type "
                + outputField.getAvroTypeDesc()
                + " is not supported for field "
                + outputField.getAvroName()
                + ".");
    }

    return value;
  }

  private Schema getFirstEnumSchema(Field field) {
    Schema fieldSchema = field.schema();
    Type fieldType = fieldSchema.getType();
    if (fieldType == Type.ENUM) {
      return fieldSchema;
    }
    if (fieldType == Type.UNION) {
      List<Schema> subSchemas = fieldSchema.getTypes();
      for (Schema subSchema : subSchemas) {
        if (subSchema.getType() == Type.ENUM) {
          return subSchema;
        }
      }
    }
    return field.schema();
  }

  public String buildFilename(String filename, boolean ziparchive) {
    return meta.buildFilename(
        filename, this, getCopy(), getPartitionId(), data.splitnr, ziparchive, meta);
  }

  public void openNewFile(String baseFilename) throws HopException {
    if (baseFilename == null) {
      throw new HopException(BaseMessages.getString(PKG, "AvroOutput.Exception.FileNameNotSet"));
    }

    data.writer = null;

    String filename = buildFilename(resolve(baseFilename), true);

    try {
      // Check for parent folder creation only if the user asks for it
      //
      if (meta.isCreateParentFolder()) {
        createParentFolder(filename);
      }

      OutputStream outputStream = getOutputStream(filename, false);

      if (log.isDetailed()) {
        logDetailed("Opening output stream in default encoding");
      }
      data.writer = new BufferedOutputStream(outputStream, 5000);

      if (log.isDetailed()) {
        logDetailed("Opened new file with name [" + filename + "]");
      }
    } catch (Exception e) {
      throw new HopException("Error opening new file : " + e.toString());
    }

    data.splitnr++;

    if (meta.isAddToResultFilenames()) {
      // Add this to the result file names...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              getFileObject(filename),
              getPipelineMeta().getName(),
              getTransformName());
      resultFile.setComment(BaseMessages.getString(PKG, "AvroOutput.AddResultFile"));
      addResultFile(resultFile);
    }
  }

  private boolean closeFile() {
    boolean retval = false;

    try {
      if (data.writer != null) {
        data.writer.flush();

        if (log.isDebug()) {
          logDebug("Closing output stream");
        }
        if (data.dataFileWriter != null) {
          data.dataFileWriter.close();
        }

        // Causes exception trying to close file in Java 8.  I believe the flush closes the file
        // also.
        data.writer = null;
        data.dataFileWriter = null;
        if (log.isDebug()) {
          logDebug("Closed output stream");
        }
      }
      data.datumWriter = null;
      data.avroSchema = null;

      retval = true;
    } catch (Exception e) {
      logError("Exception trying to close file: ", e);
      setErrors(1);
      retval = false;
    }

    return retval;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.splitnr = 0;
      try {
        openNewFile(meta.getFileName());
      } catch (Exception e) {
        logError("Couldn't open file " + meta.getFileName(), e);
        setErrors(1L);
        stopAll();
      }

      return true;
    }

    return false;
  }

  @Override
  public void dispose() {

    if (data.writer != null) {
      closeFile();
    }
    data.datumWriter = null;
    data.avroSchema = null;

    super.dispose();
  }

  private void createParentFolder(String filename) throws Exception {
    // Check for parent folder

    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = getFileObject(filename).getParent();
      if (parentfolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "AvroOutput.Log.ParentFolderExist", parentfolder.getName()));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "AvroOutput.Log.ParentFolderNotExist", parentfolder.getName()));
        }
        if (meta.isCreateParentFolder()) {
          parentfolder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "AvroOutput.Log.ParentFolderCreated", parentfolder.getName()));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "AvroOutput.Log.ParentFolderNotExistCreateIt",
                  parentfolder.getName(),
                  filename));
        }
      }

    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
        } catch (Exception ex) {
          // Ignore
        }
      }
    }
  }

  protected FileObject getFileObject(String vfsFilename) throws HopFileException {
    return HopVfs.getFileObject(vfsFilename);
  }

  protected OutputStream getOutputStream(String vfsFilename, boolean append)
      throws HopFileException {
    return HopVfs.getOutputStream(vfsFilename, append);
  }
}

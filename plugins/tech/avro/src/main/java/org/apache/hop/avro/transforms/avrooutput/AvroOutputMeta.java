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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Transform(
    id = "AvroOutput",
    name = "Avro File Output",
    description = "Writes file serialized in the Apache Avro file format",
    image = "avro_output.svg",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "/pipeline/transforms/avro-file-output.html",
    keywords = "i18n::AvroFileOutputMeta.keyword")
public class AvroOutputMeta extends BaseTransformMeta<AvroOutput, AvroOutputData> {

  private static final Class<?> PKG = AvroOutputMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String[] compressionTypes = {"none", "deflate", "snappy" , "bzip2"};

  public static final String[] OUTPUT_TYPES = {"BinaryFile", "BinaryField", "JsonField"};
  public static final int OUTPUT_TYPE_BINARY_FILE = 0;
  public static final int OUTPUT_TYPE_FIELD = 1;
  public static final int OUTPUT_TYPE_JSON_FIELD = 2;

  /** The base name of the output file */
  @HopMetadataProperty(key = "filename", injectionKeyDescription = "AvroOutput.Injection.FILENAME")
  private String fileName;

  /** The base name of the schema file */
  @HopMetadataProperty(
      key = "schemafilename",
      injectionKeyDescription = "AvroOutput.Injection.SCHEMA_FILENAME")
  private String schemaFileName;

  /** Flag: create schema file, default to false */
  @HopMetadataProperty(
      key = "create_schema_file",
      injectionKeyDescription = "AvroOutput.Injection.AUTO_CREATE_SCHEMA")
  private boolean createSchemaFile;

  /** Flag: write schema file, default to true */
  @HopMetadataProperty(
      key = "write_schema_file",
      defaultBoolean = true,
      injectionKeyDescription = "AvroOutput.Injection.WRITE_SCHEMA_TO_FILE")
  private boolean writeSchemaFile;

  /** The namespace for the schema file */
  @HopMetadataProperty(
      key = "namespace",
      injectionKey = "AVRO_NAMESPACE",
      injectionKeyDescription = "AvroOutput.Injection.AVRO_NAMESPACE")
  private String namespace;

  /** The record name for the schema file */
  @HopMetadataProperty(
      key = "recordname",
      injectionKeyDescription = "AvroOutput.Injection.AVRO_RECORD_NAME")
  private String recordName;

  /** The documentation for the schema file */
  @HopMetadataProperty(key = "doc", injectionKeyDescription = "AvroOutput.Injection.AVRO_DOC")
  private String doc;

  /** Flag: create parent folder, default to true */
  @HopMetadataProperty(
      key = "create_parent_folder",
      injectionKeyDescription = "AvroOutput.Injection.CREATE_PARENT_FOLDER")
  private boolean createParentFolder = true;

  /** Flag: add the transformne in the filename */
  @HopMetadataProperty(
      key = "split",
      injectionKeyDescription = "AvroOutput.Injection.INCLUDE_STEPNR")
  private boolean transformNrInFilename;

  /** Flag: add the partition number in the filename */
  @HopMetadataProperty(
      key = "haspartno",
      injectionKeyDescription = "AvroOutput.Injection.INCLUDE_PARTNR")
  private boolean partNrInFilename;

  /** Flag: add the date in the filename */
  @HopMetadataProperty(
      key = "add_date",
      injectionKeyDescription = "AvroOutput.Injection.INCLUDE_DATE")
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  @HopMetadataProperty(
      key = "add_time",
      injectionKeyDescription = "AvroOutput.Injection.INCLUDE_TIME")
  private boolean timeInFilename;

  /** The compression type */
  @HopMetadataProperty(
      key = "compressiontype",
      injectionKeyDescription = "AvroOutput.Injection.COMPRESSION_CODEC")
  private String compressionType;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupDescription = "AvroOutput.Injection.OUTPUT_FIELDS")
  private List<AvroOutputField> outputFields;

  /** Flag: add the filenames to result filenames */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKeyDescription = "AvroOutput.Injection.ADD_TO_RESULT")
  private boolean addToResultFilenames;

  @HopMetadataProperty(
      key = "SpecifyFormat",
      injectionKeyDescription = "AvroOutput.Injection.SPECIFY_FORMAT")
  private boolean specifyingFormat;

  @HopMetadataProperty(
      key = "date_time_format",
      injectionKeyDescription = "AvroOutput.Injection.DATE_FORMAT")
  private String dateTimeFormat;

  @HopMetadataProperty(
      key = "output_type",
      injectionKeyDescription = "AvroOutput.Injection.OUTPUT_TYPE")
  private String outputType;

  @HopMetadataProperty(
      key = "output_field_name",
      injectionKeyDescription = "AvroOutput.Injection.OUTPUT_FIELD_NAME")
  private String outputFieldName;

  public AvroOutputMeta() {
    super(); // allocate BaseTransformMeta
    outputFields = new ArrayList<>();
  }

  /**
   * @param writeSchemaFile whether the schema file should be persisted
   */
  public void setWriteSchemaFile(boolean writeSchemaFile) {
    this.writeSchemaFile = writeSchemaFile;
  }

  /**
   * @return Returns the namespace.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @param namespace The namespace to set.
   */
  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  /**
   * @return Returns the doc
   */
  public String getDoc() {
    return doc;
  }

  /**
   * @param doc The doc to set.
   */
  public void setDoc(String doc) {
    this.doc = doc;
  }

  /**
   * @return Returns the record name
   */
  public String getRecordName() {
    return recordName;
  }

  /**
   * @param recordName The record name to set.
   */
  public void setRecordName(String recordName) {
    this.recordName = recordName;
  }

  /**
   * @param createParentFolder The createParentFolder to set.
   */
  public void setCreateParentFolder(boolean createParentFolder) {
    this.createParentFolder = createParentFolder;
  }

  /**
   * @param dateInFilename The dateInFilename to set.
   */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  /**
   * @return Returns the fileName.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * @param fileName The fileName to set.
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the schema fileName.
   */
  public String getSchemaFileName() {
    return schemaFileName;
  }

  /**
   * @param schemaFileName The schemaFileName to set.
   */
  public void setSchemaFileName(String schemaFileName) {
    this.schemaFileName = schemaFileName;
  }

  /**
   * @param transformNrInFilename The transformNrInFilename to set.
   */
  public void setTransformNrInFilename(boolean transformNrInFilename) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /**
   * @param partNrInFilename The partNrInFilename to set.
   */
  public void setPartNrInFilename(boolean partNrInFilename) {
    this.partNrInFilename = partNrInFilename;
  }

  /**
   * @param timeInFilename The timeInFilename to set.
   */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  public void setSpecifyingFormat(boolean specifyingFormat) {
    this.specifyingFormat = specifyingFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public String getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
  }

  public String getOutputType() {
    return outputType;
  }

  public void setOutputType(String outputType) {
    this.outputType = outputType;
  }

  public int getOutputTypeId() {
    if (outputType != null) {
      for (int i = 0; i < OUTPUT_TYPES.length; i++) {
        if (outputType.equals(OUTPUT_TYPES[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setOutputTypeById(int outputTypeId) {
    if (outputTypeId >= 0 && outputTypeId < OUTPUT_TYPES.length) {
      this.outputType = OUTPUT_TYPES[outputTypeId];
    } else {
      this.outputType = null;
    }
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName(String outputFieldName) {
    this.outputFieldName = outputFieldName;
  }

  /**
   * @return Returns the outputFields.
   */
  public List<AvroOutputField> getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields The outputFields to set.
   */
  public void setOutputFields(List<AvroOutputField> outputFields) {
    this.outputFields = outputFields;
  }

  public boolean isCreateSchemaFile() {
    return createSchemaFile;
  }

  public boolean isWriteSchemaFile() {
    return writeSchemaFile;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public boolean isTransformNrInFilename() {
    return transformNrInFilename;
  }

  public boolean isPartNrInFilename() {
    return partNrInFilename;
  }

  public boolean isDateInFilename() {
    return dateInFilename;
  }

  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  public boolean isAddToResultFilenames() {
    return addToResultFilenames;
  }

  public void setAddToResultFilenames(boolean addToResultFilenames) {
    this.addToResultFilenames = addToResultFilenames;
  }

  public boolean isSpecifyingFormat() {
    return specifyingFormat;
  }

  public void setCreateSchemaFile(boolean createSchemaFile) {
    this.createSchemaFile = createSchemaFile;
  }

  @Override
  public Object clone() {
    AvroOutputMeta retval = new AvroOutputMeta();

    for (AvroOutputField field : outputFields) {
      retval.getOutputFields().add(new AvroOutputField(field));
    }

    return retval;
  }

  @Override
  public void setDefault() {
    createParentFolder = true; // Default createparentfolder to true
    createSchemaFile = false;
    writeSchemaFile = true;
    namespace = "namespace";
    recordName = "recordname";
    specifyingFormat = false;
    dateTimeFormat = null;
    fileName = "file.avro";
    schemaFileName = "schema.avsc";
    transformNrInFilename = false;
    partNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    addToResultFilenames = true;
    compressionType = "none";
    outputType = OUTPUT_TYPES[OUTPUT_TYPE_BINARY_FILE];
    outputFieldName = "avro_record";
  }

  public String buildFilename(
      IVariables space, int transformnr, String partnr, int splitnr, boolean ziparchive) {
    return buildFilename(fileName, space, transformnr, partnr, splitnr, ziparchive, this);
  }

  public String buildFilename(
      String filename,
      IVariables space,
      int transformnr,
      String partnr,
      int splitnr,
      boolean ziparchive,
      AvroOutputMeta meta) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realFileName = space.resolve(filename);
    String extension = "";
    String retval = "";
    if (realFileName.contains(".")) {
      retval = realFileName.substring(0, realFileName.lastIndexOf("."));
      extension = realFileName.substring(realFileName.lastIndexOf(".") + 1);
    } else {
      retval = realFileName;
    }

    Date now = new Date();

    if (meta.isSpecifyingFormat() && !Utils.isEmpty(meta.getDateTimeFormat())) {
      daf.applyPattern(meta.getDateTimeFormat());
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (meta.isDateInFilename()) {
        daf.applyPattern("yyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (meta.isTimeInFilename()) {
        daf.applyPattern("HHmmss");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }
    if (meta.isTransformNrInFilename()) {
      retval += "_" + transformnr;
    }
    if (meta.isPartNrInFilename()) {
      retval += "_" + partnr;
    }

    if (extension != null && extension.length() != 0) {
      retval += "." + extension;
    }
    return retval;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transforminfo,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // Check output fields
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "TextFileOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transforminfo);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < outputFields.size(); i++) {
        int idx = prev.indexOfValue(outputFields.get(i).getName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputFields.get(i).getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "TextFileOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.AllFieldsFound"),
                transforminfo);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.ExpectedInputOk"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.ExpectedInputError"),
              transforminfo);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.FilesNotChecked"),
            transforminfo);
    remarks.add(cr);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // change the case insensitive flag too

    if (outputType.equalsIgnoreCase(OUTPUT_TYPES[OUTPUT_TYPE_FIELD])) {
      IValueMeta v = new ValueMetaBinary(variables.resolve(outputFieldName));
      v.setOrigin(origin);
      row.addValueMeta(v);
    } else if (outputType.equalsIgnoreCase(OUTPUT_TYPES[OUTPUT_TYPE_JSON_FIELD])) {
      IValueMeta valueMetaInterface = new ValueMetaString(variables.resolve(outputFieldName));
      valueMetaInterface.setOrigin(origin);
      row.addValueMeta(valueMetaInterface);
    }
  }
}

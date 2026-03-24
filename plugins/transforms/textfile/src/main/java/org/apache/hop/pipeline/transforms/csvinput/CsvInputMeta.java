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

package org.apache.hop.pipeline.transforms.csvinput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;

@Transform(
    id = "CSVInput",
    image = "textfileinput.svg",
    name = "i18n::CsvInput.Name",
    description = "i18n::CsvInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::CsvInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/csvinput.html")
@Getter
@Setter
public class CsvInputMeta extends BaseTransformMeta<CsvInput, CsvInputData> {

  private static final Class<?> PKG = CsvInput.class;

  @HopMetadataProperty(
      key = "filename",
      injectionKey = "FILENAME",
      injectionKeyDescription = "CsvInputMeta.Injection.FILENAME")
  private String filename;

  @HopMetadataProperty(
      key = "filename_field",
      injectionKey = "FILENAME_FIELD",
      injectionKeyDescription = "CsvInputMeta.Injection.FILENAME_FIELD")
  private String filenameField;

  @HopMetadataProperty(
      key = "include_filename",
      injectionKey = "INCLUDE_FILENAME",
      injectionKeyDescription = "CsvInputMeta.Injection.INCLUDE_FILENAME")
  private boolean includingFilename;

  @HopMetadataProperty(
      key = "rownum_field",
      injectionKey = "ROW_NUMBER_FIELDNAME",
      injectionKeyDescription = "CsvInputMeta.Injection.ROW_NUMBER_FIELDNAME")
  private String rowNumField;

  @HopMetadataProperty(
      key = "header",
      injectionKey = "HEADER_PRESENT",
      injectionKeyDescription = "CsvInputMeta.Injection.HEADER_PRESENT")
  private boolean headerPresent;

  @HopMetadataProperty(
      key = "separator",
      injectionKey = "DELIMITER",
      injectionKeyDescription = "CsvInputMeta.Injection.DELIMITER")
  private String delimiter;

  @HopMetadataProperty(
      key = "enclosure",
      injectionKey = "ENCLOSURE",
      injectionKeyDescription = "CsvInputMeta.Injection.DELIMITER")
  private String enclosure;

  @Injection(name = "BUFFER_SIZE")
  @HopMetadataProperty(
      key = "buffer_size",
      injectionKey = "BUFFER_SIZE",
      injectionKeyDescription = "CsvInputMeta.Injection.BUFFER_SIZE")
  private String bufferSize;

  @Injection(name = "LAZY_CONVERSION")
  @HopMetadataProperty(
      key = "lazy_conversion",
      injectionKey = "LAZY_CONVERSION",
      injectionKeyDescription = "CsvInputMeta.Injection.LAZY_CONVERSION")
  private boolean lazyConversionActive;

  @Injection(name = "ADD_RESULT")
  @HopMetadataProperty(
      key = "add_filename_result",
      injectionKey = "ADD_RESULT",
      injectionKeyDescription = "CsvInputMeta.Injection.ADD_RESULT")
  private boolean addResult;

  @Injection(name = "RUNNING_IN_PARALLEL")
  @HopMetadataProperty(
      key = "parallel",
      injectionKey = "RUNNING_IN_PARALLEL",
      injectionKeyDescription = "CsvInputMeta.Injection.RUNNING_IN_PARALLEL")
  private boolean runningInParallel;

  @Injection(name = "FILE_ENCODING")
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "FILE_ENCODING",
      injectionKeyDescription = "CsvInputMeta.Injection.FILE_ENCODING")
  private String encoding;

  @Injection(name = "NEWLINES_IN_FIELDS")
  @HopMetadataProperty(
      key = "newline_possible",
      injectionKey = "NEWLINES_IN_FIELDS",
      injectionKeyDescription = "CsvInputMeta.Injection.NEWLINES_IN_FIELDS")
  private boolean newlinePossibleInFields;

  @Injection(name = "SCHEMA_DEFINITION")
  @HopMetadataProperty(
      key = "schemaDefinition",
      injectionKey = "SCHEMA_DEFINITION",
      injectionKeyDescription = "CsvInputMeta.Injection.SCHEMA_DEFINITION",
      hopMetadataPropertyType = HopMetadataPropertyType.STATIC_SCHEMA_DEFINITION)
  private String schemaDefinition;

  /** Reference to ignore fields tab */
  @Injection(name = "IGNORE_FIELDS")
  @HopMetadataProperty(
      key = "ignoreFields",
      injectionKey = "IGNORE_FIELDS",
      injectionKeyDescription = "CsvInputMeta.Injection.IGNORE_FIELDS")
  public boolean ignoreFields;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "INPUT_FIELDS",
      injectionGroupDescription = "CsvInputMeta.Injection.INPUT_FIELDS")
  private List<CsvInputField> inputFields;

  public CsvInputMeta() {
    super();
    this.inputFields = new ArrayList<>();
    this.delimiter = ",";
    this.enclosure = "\"";
    this.headerPresent = true;
    this.lazyConversionActive = false;
    this.addResult = false;
    this.bufferSize = "50000";
  }

  public CsvInputMeta(CsvInputMeta m) {
    this();
    this.addResult = m.addResult;
    this.bufferSize = m.bufferSize;
    this.delimiter = m.delimiter;
    this.enclosure = m.enclosure;
    this.encoding = m.encoding;
    this.filename = m.filename;
    this.filenameField = m.filenameField;
    this.headerPresent = m.headerPresent;
    this.ignoreFields = m.ignoreFields;
    this.includingFilename = m.includingFilename;
    this.lazyConversionActive = m.lazyConversionActive;
    this.newlinePossibleInFields = m.newlinePossibleInFields;
    this.rowNumField = m.rowNumField;
    this.runningInParallel = m.runningInParallel;
    this.schemaDefinition = m.schemaDefinition;
    m.inputFields.forEach(field -> this.inputFields.add(new CsvInputField(field)));
  }

  @Override
  public CsvInputMeta clone() {
    return new CsvInputMeta(this);
  }

  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    try {
      rowMeta.clear(); // Start with a clean slate, eats the input

      if (ignoreFields) {

        try {
          SchemaDefinition loadedSchemaDefinition =
              (new SchemaDefinitionUtil())
                  .loadSchemaDefinition(metadataProvider, getSchemaDefinition());
          if (loadedSchemaDefinition != null) {
            IRowMeta r = loadedSchemaDefinition.getRowMeta();
            if (r != null) {
              for (int i = 0; i < r.size(); i++) {
                IValueMeta v = r.getValueMeta(i);
                if (lazyConversionActive) {
                  v.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
                }
                v.setStringEncoding(variables.resolve(encoding));

                // In case we want to convert Strings...
                // Using a copy of the valueMeta object means that the inner and outer
                // representation
                // format
                // is the same.
                // Preview will show the data the same way as we read it.
                // This layout is then taken further down the road by the metadata through the
                // pipeline.
                //
                IValueMeta storageMetadata =
                    ValueMetaFactory.cloneValueMeta(v, IValueMeta.TYPE_STRING);
                storageMetadata.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
                storageMetadata.setLength(
                    -1, -1); // we don't really know the lengths of the strings read in advance.
                v.setStorageMetadata(storageMetadata);

                v.setOrigin(origin);

                rowMeta.addValueMeta(v);
              }
            }
          }
        } catch (HopTransformException | HopPluginException e) {
          // ignore any errors here.
        }
      } else {
        for (CsvInputField inputField : inputFields) {
          IValueMeta valueMeta =
              ValueMetaFactory.createValueMeta(inputField.getName(), inputField.getType());
          valueMeta.setConversionMask(inputField.getFormat());
          valueMeta.setLength(inputField.getLength());
          valueMeta.setPrecision(inputField.getPrecision());
          valueMeta.setConversionMask(inputField.getFormat());
          valueMeta.setDecimalSymbol(inputField.getDecimalSymbol());
          valueMeta.setGroupingSymbol(inputField.getGroupSymbol());
          valueMeta.setCurrencySymbol(inputField.getCurrencySymbol());
          valueMeta.setTrimType(inputField.getTrimType());
          if (lazyConversionActive) {
            valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
          }
          valueMeta.setStringEncoding(variables.resolve(encoding));

          // In case we want to convert Strings...
          // Using a copy of the valueMeta object means that the inner and outer representation
          // format
          // is the same.
          // Preview will show the data the same way as we read it.
          // This layout is then taken further down the road by the metadata through the pipeline.
          //
          IValueMeta storageMetadata =
              ValueMetaFactory.cloneValueMeta(valueMeta, IValueMeta.TYPE_STRING);
          storageMetadata.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
          storageMetadata.setLength(
              -1, -1); // we don't really know the lengths of the strings read in advance.
          valueMeta.setStorageMetadata(storageMetadata);

          valueMeta.setOrigin(origin);

          rowMeta.addValueMeta(valueMeta);
        }
      }

      if (!Utils.isEmpty(filenameField) && includingFilename) {
        IValueMeta filenameMeta = new ValueMetaString(filenameField);
        filenameMeta.setOrigin(origin);
        if (lazyConversionActive) {
          filenameMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
          filenameMeta.setStorageMetadata(new ValueMetaString(filenameField));
        }
        rowMeta.addValueMeta(filenameMeta);
      }

      if (!Utils.isEmpty(rowNumField)) {
        IValueMeta rowNumMeta = new ValueMetaInteger(rowNumField);
        rowNumMeta.setLength(10);
        rowNumMeta.setOrigin(origin);
        rowMeta.addValueMeta(rowNumMeta);
      }
    } catch (Exception e) {
      throw new HopTransformException(e);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "CsvInputMeta.CheckResult.NotReceivingFields"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "CsvInputMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "CsvInputMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CsvInputMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {

    List<ResourceReference> references = new ArrayList<>(5);

    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);
    if (!Utils.isEmpty(filename)) {
      // Add the filename to the references, including a reference to this
      // transform meta data.
      //
      reference.getEntries().add(new ResourceEntry(variables.resolve(filename), ResourceType.FILE));
    }
    return references;
  }

  public void setFields(CsvInputField[] fields) {
    this.inputFields.clear();
    this.inputFields.addAll(List.of(fields));
  }

  /**
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if (Utils.isEmpty(filenameField) && !Utils.isEmpty(filename)) {
        // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.csv
        // To : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(filename), variables);

        // If the file doesn't exist, forget about this effort too!
        //
        if (fileObject.exists()) {
          // Convert to an absolute path...
          //
          filename = iResourceNaming.nameResource(fileObject, variables, true);

          return filename;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}

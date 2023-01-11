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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.IInputFileMeta;
import org.apache.hop.core.file.TextFileInputField;
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.common.ICsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
    id = "CSVInput",
    image = "textfileinput.svg",
    name = "i18n::CsvInput.Name",
    description = "i18n::CsvInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::CsvInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/csvinput.html")
public class CsvInputMeta extends BaseTransformMeta<CsvInput, CsvInputData> implements
        IInputFileMeta,
        ICsvInputAwareMeta {

  private static final Class<?> PKG = CsvInput.class; // For Translator

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
      injectionKeyDescription = "CsvInputMeta.Injection.ENCLOSURE")
  private String enclosure;

  @HopMetadataProperty(
      key = "buffer_size",
      injectionKey = "BUFFER_SIZE",
      injectionKeyDescription = "CsvInputMeta.Injection.BUFFER_SIZE")
  private String bufferSize;

  @HopMetadataProperty(
      key = "lazy_conversion",
      injectionKey = "LAZY_CONVERSION",
      injectionKeyDescription = "CsvInputMeta.Injection.LAZY_CONVERSION")
  private boolean lazyConversionActive;

  @HopMetadataProperty(
      key = "fields",
      injectionKey = "INPUT_FIELDS",
      injectionKeyDescription = "CsvInputMeta.Injection.INPUT_FIELDS")
  private List<TextFileInputField> inputFields;

  @HopMetadataProperty(
      key = "add_filename_result",
      injectionKey = "ADD_RESULT",
      injectionKeyDescription = "CsvInputMeta.Injection.ADD_RESULT")
  private boolean isaddresult;

  @HopMetadataProperty(
      key = "parallel",
      injectionKey = "RUNNING_IN_PARALLEL",
      injectionKeyDescription = "CsvInputMeta.Injection.RUNNING_IN_PARALLEL")
  private boolean runningInParallel;

  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "FILE_ENCODING",
      injectionKeyDescription = "CsvInputMeta.Injection.FILE_ENCODING")
  private String encoding;

  @HopMetadataProperty(
      key = "newline_possible",
      injectionKey = "NEWLINES_IN_FIELDS",
      injectionKeyDescription = "CsvInputMeta.Injection.NEWLINES_IN_FIELDS")
  private boolean newlinePossibleInFields;

  public CsvInputMeta() {
    super();
    this.inputFields = new ArrayList<>();
  }

  @Override
  public Object clone() {
    final CsvInputMeta retval = (CsvInputMeta) super.clone();
    retval.inputFields = new ArrayList<>();
    for (TextFileInputField textFileInputField : inputFields) {
      retval.inputFields.add((TextFileInputField) textFileInputField.clone());
    }
    return retval;
  }

  @Override
  public void setDefault() {
    delimiter = ",";
    enclosure = "\"";
    headerPresent = true;
    lazyConversionActive = true;
    isaddresult = false;
    bufferSize = "50000";
  }

  public void allocate() {
    inputFields = new ArrayList<>();
  }

  @Override
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

      for (TextFileInputField field : inputFields) {
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta(field.getName(), field.getType());
        valueMeta.setConversionMask(field.getFormat());
        valueMeta.setLength(field.getLength());
        valueMeta.setPrecision(field.getPrecision());
        valueMeta.setConversionMask(field.getFormat());
        valueMeta.setDecimalSymbol(field.getDecimalSymbol());
        valueMeta.setGroupingSymbol(field.getGroupSymbol());
        valueMeta.setCurrencySymbol(field.getCurrencySymbol());
        valueMeta.setTrimType(field.getTrimType());
        if (lazyConversionActive) {
          valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
        }
        valueMeta.setStringEncoding(variables.resolve(encoding));

        // In case we want to convert Strings...
        // Using a copy of the valueMeta object means that the inner and outer representation format
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
    if (prev == null || prev.size() == 0) {
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

  /** @return the delimiter */
  @Override
  public String getDelimiter() {
    return delimiter;
  }

  /** @param delimiter the delimiter to set */
  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  /** @return the filename */
  public String getFilename() {
    return filename;
  }

  /** @param filename the filename to set */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /** @return the bufferSize */
  public String getBufferSize() {
    return bufferSize;
  }

  /** @param bufferSize the bufferSize to set */
  public void setBufferSize(String bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * @return true if lazy conversion is turned on: conversions are delayed as long as possible,
   *     perhaps to never occur at all.
   */
  public boolean isLazyConversionActive() {
    return lazyConversionActive;
  }

  /**
   * @param lazyConversionActive true if lazy conversion is to be turned on: conversions are delayed
   *     as long as possible, perhaps to never occur at all.
   */
  public void setLazyConversionActive(boolean lazyConversionActive) {
    this.lazyConversionActive = lazyConversionActive;
  }

  /** @return the headerPresent */
  public boolean isHeaderPresent() {
    return headerPresent;
  }

  /** @param headerPresent the headerPresent to set */
  public void setHeaderPresent(boolean headerPresent) {
    this.headerPresent = headerPresent;
  }

  /** @return the enclosure */
  @Override
  public String getEnclosure() {
    return enclosure;
  }

  /** @param enclosure the enclosure to set */
  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
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

  /** @return the inputFields */
  @Override
  public List<TextFileInputField> getInputFields() {
    return inputFields;
  }

  /** @param inputFields the inputFields to set */
  public void setInputFields(List<TextFileInputField> inputFields) {
    this.inputFields = inputFields;
  }

  @Override
  public int getFileFormatTypeNr() {
    return TextFileInputMeta.FILE_FORMAT_MIXED; // TODO: check this
  }

  @Override
  public String[] getFilePaths(IVariables variables) {
    return new String[] {
      variables.resolve(filename),
    };
  }

  @Override
  public int getNrHeaderLines() {
    return 1;
  }

  @Override
  public boolean hasHeader() {
    return isHeaderPresent();
  }

  @Override
  public String getErrorCountField() {
    return null;
  }

  @Override
  public String getErrorFieldsField() {
    return null;
  }

  @Override
  public String getErrorTextField() {
    return null;
  }

  @Override
  public String getEscapeCharacter() {
    return null;
  }

  @Override
  public String getFileType() {
    return "CSV";
  }

  @Override
  public String getSeparator() {
    return delimiter;
  }

  @Override
  public boolean includeFilename() {
    return false;
  }

  @Override
  public boolean includeRowNumber() {
    return false;
  }

  @Override
  public boolean isErrorIgnored() {
    return false;
  }

  @Override
  public boolean isErrorLineSkipped() {
    return false;
  }

  /** @return the filenameField */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField the filenameField to set */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /** @return the includingFilename */
  public boolean isIncludingFilename() {
    return includingFilename;
  }

  /** @param includingFilename the includingFilename to set */
  public void setIncludingFilename(boolean includingFilename) {
    this.includingFilename = includingFilename;
  }

  /** @return the rowNumField */
  public String getRowNumField() {
    return rowNumField;
  }

  /** @param rowNumField the rowNumField to set */
  public void setRowNumField(String rowNumField) {
    this.rowNumField = rowNumField;
  }

  /** @param isaddresult The isaddresult to set. */
  public void setIsaddresult(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /** @return Returns isaddresult. */
  public boolean isIsaddresult() {
    return isaddresult;
  }

  /** @return the runningInParallel */
  public boolean isRunningInParallel() {
    return runningInParallel;
  }

  /** @param runningInParallel the runningInParallel to set */
  public void setRunningInParallel(boolean runningInParallel) {
    this.runningInParallel = runningInParallel;
  }

  /** @return the encoding */
  @Override
  public String getEncoding() {
    return encoding;
  }

  /** @param encoding the encoding to set */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
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
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(filename));

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

  /** @return the newlinePossibleInFields */
  public boolean isNewlinePossibleInFields() {
    return newlinePossibleInFields;
  }

  /** @param newlinePossibleInFields the newlinePossibleInFields to set */
  public void setNewlinePossibleInFields(boolean newlinePossibleInFields) {
    this.newlinePossibleInFields = newlinePossibleInFields;
  }

  @Override
  public FileObject getHeaderFileObject(final IVariables variables) {
    final String filename = variables.resolve(getFilename());
    try {
      return HopVfs.getFileObject(filename);
    } catch (final HopFileException e) {
      return null;
    }
  }
}

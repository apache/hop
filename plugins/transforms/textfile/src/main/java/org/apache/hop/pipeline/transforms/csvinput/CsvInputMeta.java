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
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IInputFileMeta;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.common.ICsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@InjectionSupported(
    localizationPrefix = "CsvInputMeta.Injection.",
    groups = {"INPUT_FIELDS"})
@Transform(
    id = "CSVInput",
    image = "textfileinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.CsvInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.CsvInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = {
      "csv",
      "text",
      "tsv",
      "csv read",
      "tsv read",
      "text read",
    },
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/csvinput.html")
public class CsvInputMeta extends BaseTransformMeta
    implements ITransformMeta<CsvInput, CsvInputData>,
        IInputFileMeta<CsvInput, CsvInputData>,
        ICsvInputAwareMeta {

  private static final Class<?> PKG = CsvInput.class; // For Translator

  @Injection(name = "FILENAME")
  private String filename;

  @Injection(name = "FILENAME_FIELD")
  private String filenameField;

  @Injection(name = "INCLUDE_FILENAME")
  private boolean includingFilename;

  @Injection(name = "ROW_NUMBER_FIELDNAME")
  private String rowNumField;

  @Injection(name = "HEADER_PRESENT")
  private boolean headerPresent;

  @Injection(name = "DELIMITER")
  private String delimiter;

  @Injection(name = "ENCLOSURE")
  private String enclosure;

  @Injection(name = "BUFFER_SIZE")
  private String bufferSize;

  @Injection(name = "LAZY_CONVERSION")
  private boolean lazyConversionActive;

  @InjectionDeep private TextFileInputField[] inputFields;

  @Injection(name = "ADD_RESULT")
  private boolean isaddresult;

  @Injection(name = "RUNNING_IN_PARALLEL")
  private boolean runningInParallel;

  @Injection(name = "FILE_ENCODING")
  private String encoding;

  @Injection(name = "NEWLINES_IN_FIELDS")
  private boolean newlinePossibleInFields;

  public CsvInputMeta() {
    super();
    allocate(0);
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    final CsvInputMeta retval = (CsvInputMeta) super.clone();
    retval.inputFields = new TextFileInputField[inputFields.length];
    for (int i = 0; i < inputFields.length; i++) {
      retval.inputFields[i] = (TextFileInputField) inputFields[i].clone();
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

  private void readData(Node transformNode) throws HopXmlException {
    try {
      filename = XmlHandler.getTagValue(transformNode, "filename");
      filenameField = XmlHandler.getTagValue(transformNode, "filename_field");
      rowNumField = XmlHandler.getTagValue(transformNode, "rownum_field");
      includingFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include_filename"));
      delimiter = XmlHandler.getTagValue(transformNode, "separator");
      enclosure = XmlHandler.getTagValue(transformNode, "enclosure");
      bufferSize = XmlHandler.getTagValue(transformNode, "buffer_size");
      headerPresent = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      lazyConversionActive =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "lazy_conversion"));
      isaddresult =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "add_filename_result"));
      runningInParallel = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "parallel"));
      String nlp = XmlHandler.getTagValue(transformNode, "newline_possible");
      if (Utils.isEmpty(nlp)) {
        if (runningInParallel) {
          newlinePossibleInFields = false;
        } else {
          newlinePossibleInFields = true;
        }
      } else {
        newlinePossibleInFields = "Y".equalsIgnoreCase(nlp);
      }
      encoding = XmlHandler.getTagValue(transformNode, "encoding");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        inputFields[i] = new TextFileInputField();

        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        inputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
        inputFields[i].setType(
            ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fnode, "type")));
        inputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
        inputFields[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        inputFields[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        inputFields[i].setGroupSymbol(XmlHandler.getTagValue(fnode, "group"));
        inputFields[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        inputFields[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
        inputFields[i].setTrimType(
            ValueMetaString.getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void allocate(int nrFields) {
    inputFields = new TextFileInputField[nrFields];
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(500);

    retval.append("    ").append(XmlHandler.addTagValue("filename", filename));
    retval.append("    ").append(XmlHandler.addTagValue("filename_field", filenameField));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumField));
    retval.append("    ").append(XmlHandler.addTagValue("include_filename", includingFilename));
    retval.append("    ").append(XmlHandler.addTagValue("separator", delimiter));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure", enclosure));
    retval.append("    ").append(XmlHandler.addTagValue("header", headerPresent));
    retval.append("    ").append(XmlHandler.addTagValue("buffer_size", bufferSize));
    retval.append("    ").append(XmlHandler.addTagValue("lazy_conversion", lazyConversionActive));
    retval.append("    ").append(XmlHandler.addTagValue("add_filename_result", isaddresult));
    retval.append("    ").append(XmlHandler.addTagValue("parallel", runningInParallel));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("newline_possible", newlinePossibleInFields));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));

    retval.append("    ").append(XmlHandler.openTag("fields")).append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      TextFileInputField field = inputFields[i];

      retval.append("      ").append(XmlHandler.openTag("field")).append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", field.getName()));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue("type", ValueMetaFactory.getValueMetaName(field.getType())));
      retval.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("currency", field.getCurrencySymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("decimal", field.getDecimalSymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("group", field.getGroupSymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("length", field.getLength()));
      retval.append("        ").append(XmlHandler.addTagValue("precision", field.getPrecision()));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue(
                  "trim_type", ValueMetaString.getTrimTypeCode(field.getTrimType())));
      retval.append("      ").append(XmlHandler.closeTag("field")).append(Const.CR);
    }
    retval.append("    ").append(XmlHandler.closeTag("fields")).append(Const.CR);

    return retval.toString();
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

      for (int i = 0; i < inputFields.length; i++) {
        TextFileInputField field = inputFields[i];

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

  @Override
  public CsvInput createTransform(
      TransformMeta transformMeta, CsvInputData data, int cnr, PipelineMeta tr, Pipeline pipeline) {
    return new CsvInput(transformMeta, this, data, cnr, tr, pipeline);
  }

  @Override
  public CsvInputData getTransformData() {
    return new CsvInputData();
  }

  /** @return the delimiter */
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
  public TextFileInputField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields the inputFields to set */
  public void setInputFields(TextFileInputField[] inputFields) {
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
  public void setAddResultFile(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /** @return Returns isaddresult. */
  public boolean isAddResultFile() {
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

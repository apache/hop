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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "XMLOutput",
    image = "XOU.svg",
    name = "i18n::XMLOutput.name",
    description = "i18n::XMLOutput.description",
    categoryDescription = "i18n::XMLOutput.category",
    keywords = "i18n::XmlOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/xmloutput.html")
@Getter
@Setter
public class XmlOutputMeta extends BaseTransformMeta<XmlOutput, XmlOutputData> {
  private static final Class<?> PKG = XmlOutputMeta.class;
  public static final String CONST_SPACES_LONG = "        ";
  public static final String CONST_SPACES = "      ";

  @Getter
  @Setter
  public static class FileDetails {
    /** The base name of the output file */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FILENAME",
        injectionKeyDescription = "XMLOutput.Injection.FILENAME")
    private String fileName;

    /** The file extention in case of a generated filename */
    @HopMetadataProperty(
        key = "extension",
        injectionKey = "EXTENSION",
        injectionKeyDescription = "XMLOutput.Injection.EXTENSION")
    private String extension;

    /** if this value is larger than 0, the file is split up into parts of this number of lines */
    @HopMetadataProperty(
        key = "splitevery",
        injectionKey = "SPLIT_EVERY",
        injectionKeyDescription = "XMLOutput.Injection.SPLIT_EVERY")
    private int splitEvery;

    /** Flag: add the transformnr in the filename */
    @HopMetadataProperty(
        key = "split",
        injectionKey = "INC_TRANSFORMNR_IN_FILENAME",
        injectionKeyDescription = "XMLOutput.Injection.INC_TRANSFORMNR_IN_FILENAME")
    private boolean transformNrInFilename;

    /** Flag: add the date in the filename */
    @HopMetadataProperty(
        key = "add_date",
        injectionKey = "INC_DATE_IN_FILENAME",
        injectionKeyDescription = "XMLOutput.Injection.INC_DATE_IN_FILENAME")
    private boolean dateInFilename;

    /** Flag: add the time in the filename */
    @HopMetadataProperty(
        key = "add_time",
        injectionKey = "INC_TIME_IN_FILENAME",
        injectionKeyDescription = "XMLOutput.Injection.INC_TIME_IN_FILENAME")
    private boolean timeInFilename;

    /** Flag: put the destination file in a zip archive */
    @HopMetadataProperty(
        key = "zipped",
        injectionKey = "ZIPPED",
        injectionKeyDescription = "XMLOutput.Injection.ZIPPED")
    private boolean zipped;

    /** Flag: add the filenames to result filenames */
    @HopMetadataProperty(
        key = "add_to_result_filenames",
        injectionKey = "ADD_TO_RESULT",
        injectionKeyDescription = "XMLOutput.Injection.ADD_TO_RESULT")
    private boolean addToResultFilenames;

    /** Flag : Do not open new file when transformation start */
    @HopMetadataProperty(
        key = "do_not_open_newfile_init",
        injectionKey = "DO_NOT_CREATE_FILE_AT_STARTUP",
        injectionKeyDescription = "XMLOutput.Injection.DO_NOT_CREATE_FILE_AT_STARTUP")
    private boolean doNotOpenNewFileInit;

    /** Omit null elements from xml output */
    @HopMetadataProperty(
        key = "omit_null_values",
        injectionKey = "OMIT_NULL_VALUES",
        injectionKeyDescription = "XMLOutput.Injection.OMIT_NULL_VALUES")
    private boolean omitNullValues;

    @HopMetadataProperty(
        key = "SpecifyFormat",
        injectionKey = "SPEFICY_FORMAT",
        injectionKeyDescription = "XMLOutput.Injection.SPEFICY_FORMAT")
    private boolean specifyFormat;

    @HopMetadataProperty(
        key = "date_time_format",
        injectionKey = "DATE_FORMAT",
        injectionKeyDescription = "XMLOutput.Injection.DATE_FORMAT")
    private String dateTimeFormat;

    public FileDetails() {
      fileName = "";
      extension = "";
      splitEvery = 0;
    }

    public FileDetails(FileDetails d) {
      this();
      this.addToResultFilenames = d.addToResultFilenames;
      this.dateInFilename = d.dateInFilename;
      this.dateTimeFormat = d.dateTimeFormat;
      this.doNotOpenNewFileInit = d.doNotOpenNewFileInit;
      this.extension = d.extension;
      this.fileName = d.fileName;
      this.omitNullValues = d.omitNullValues;
      this.specifyFormat = d.specifyFormat;
      this.splitEvery = d.splitEvery;
      this.timeInFilename = d.timeInFilename;
      this.transformNrInFilename = d.transformNrInFilename;
      this.zipped = d.zipped;
    }
  }

  @HopMetadataProperty(key = "file")
  private FileDetails fileDetails;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "XMLOutput.Injection.ENCODING")
  private String encoding;

  /** The name variables for the XML document: null or empty string means no xmlns is written */
  @HopMetadataProperty(
      key = "name_space",
      injectionKey = "NAMESPACE",
      injectionKeyDescription = "XMLOutput.Injection.NAMESPACE")
  private String nameSpace;

  /** The name of the parent XML element */
  @HopMetadataProperty(
      key = "xml_main_element",
      injectionKey = "MAIN_ELEMENT",
      injectionKeyDescription = "XMLOutput.Injection.MAIN_ELEMENT")
  private String mainElement;

  /** The name of the repeating row XML element */
  @HopMetadataProperty(
      key = "xml_repeat_element",
      injectionKey = "REPEAT_ELEMENT",
      injectionKeyDescription = "XMLOutput.Injection.REPEAT_ELEMENT")
  private String repeatElement;

  /** The output fields */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "OUTPUT_FIELDS",
      injectionGroupDescription = "XMLOutput.Injection.OUTPUT_FIELDS")
  private List<XmlField> outputFields;

  public XmlOutputMeta() {
    super();
    fileDetails = new FileDetails();
    outputFields = new ArrayList<>();
    encoding = Const.XML_ENCODING;
    nameSpace = "";
    mainElement = "Rows";
    repeatElement = "Row";
  }

  public XmlOutputMeta(XmlOutputMeta m) {
    this();
    this.encoding = m.encoding;
    this.fileDetails = new FileDetails(m.fileDetails);
    this.mainElement = m.mainElement;
    this.nameSpace = m.nameSpace;
    this.repeatElement = m.repeatElement;
    m.outputFields.forEach(field -> outputFields.add(new XmlField(field)));
  }

  @Override
  public Object clone() {
    return new XmlOutputMeta(this);
  }

  public String getNewLine(String formatCode) {
    String nl = System.lineSeparator();
    if (formatCode != null) {
      if (formatCode.equalsIgnoreCase("DOS")) {
        nl = "\r\n";
      } else if (formatCode.equalsIgnoreCase("UNIX")) {
        nl = "\n";
      }
    }

    return nl;
  }

  public String[] getFiles(IVariables variables) {
    int copies = 1;
    int splits = 1;

    if (fileDetails.transformNrInFilename) {
      copies = 3;
    }

    if (fileDetails.splitEvery != 0) {
      splits = 3;
    }

    int nr = copies * splits;
    if (nr > 1) {
      nr++;
    }

    String[] fileNames = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int split = 0; split < splits; split++) {
        fileNames[i] = buildFilename(variables, copy, split, false);
        i++;
      }
    }
    if (i < nr) {
      fileNames[i] = "...";
    }

    return fileNames;
  }

  public String buildFilename(IVariables variables, int copyNr, int splitNr, boolean zipArchive) {
    SimpleDateFormat daf = new SimpleDateFormat();
    DecimalFormat df = new DecimalFormat("00000");

    // Replace possible environment variables...
    String filename = variables.resolve(fileDetails.fileName);
    String realExtension = variables.resolve(fileDetails.extension);

    Date now = new Date();

    if (fileDetails.specifyFormat && !Utils.isEmpty(fileDetails.dateTimeFormat)) {
      daf.applyPattern(fileDetails.dateTimeFormat);
      String dt = daf.format(now);
      filename += dt;
    } else {
      if (fileDetails.dateInFilename) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        filename += "_" + d;
      }
      if (fileDetails.timeInFilename) {
        daf.applyPattern("HHmmss");
        String t = daf.format(now);
        filename += "_" + t;
      }
    }

    if (fileDetails.transformNrInFilename) {
      filename += "_" + copyNr;
    }
    if (fileDetails.splitEvery > 0) {
      filename += "_" + df.format(splitNr + 1);
    }

    if (fileDetails.zipped) {
      if (zipArchive) {
        filename += ".zip";
      } else {
        if (!Utils.isEmpty(realExtension)) {
          filename += "." + realExtension;
        }
      }
    } else {
      if (!Utils.isEmpty(realExtension)) {
        filename += "." + realExtension;
      }
    }
    return filename;
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // No values are added to the row in this type of transform
    // However, in case of Fixed length records,
    // the field precisions and lengths are altered!

    for (XmlField field : outputFields) {
      IValueMeta v = row.searchValueMeta(field.getFieldName());
      if (v != null) {
        v.setLength(field.getLength(), field.getPrecision());
      }
    }
  }

  @Override
  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    RowMeta row = new RowMeta();
    for (XmlField field : outputFields) {
      row.addValueMeta(
          new ValueMetaBase(
              field.getFieldName(), field.getType(), field.getLength(), field.getPrecision()));
    }
    return row;
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

    // Check output fields
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "XMLOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (XmlField outputField : outputFields) {
        int idx = prev.indexOfValue(outputField.getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputField.getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  /**
   * Since the exported transformation that runs this will reside in a ZIP file, we can't reference
   * files relatively. So what this does is turn the name of the base path into an absolute path.
   *
   * @param variables the variable variables to use
   * @param definitions The definitions to use
   * @param resourceNamingInterface The repository to optionally load other resources from (to be
   *     converted to XML)
   * @param metadataProvider the metadataProvider in which non-Hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      if (!Utils.isEmpty(fileDetails.fileName)) {
        FileObject fileObject =
            HopVfs.getFileObject(variables.resolve(fileDetails.fileName), variables);
        fileDetails.fileName = resourceNamingInterface.nameResource(fileObject, variables, true);
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public void convertLegacyXml(Node node) throws HopException {
    String oldExtension = XmlHandler.getTagValue(node, "file", "extention");
    fileDetails.extension = Const.NVL(fileDetails.extension, oldExtension);
  }
}

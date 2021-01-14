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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.xml.xmloutput.XmlField.ContentType;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Transform(
    id = "XMLOutput",
    image = "XOU.svg",
    name = "i18n::XMLOutput.name",
    description = "i18n::XMLOutput.description",
    categoryDescription = "i18n::XMLOutput.category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/xmloutput.html")
@InjectionSupported(localizationPrefix = "XMLOutput.Injection.", groups = "OUTPUT_FIELDS")
public class XmlOutputMeta extends BaseTransformMeta
    implements ITransformMeta<XmlOutput, XmlOutputData> {
  private static final Class<?> PKG = XmlOutputMeta.class; // For Translator

  /** The base name of the output file */
  @Injection(name = "FILENAME")
  private String fileName;

  /** The file extention in case of a generated filename */
  @Injection(name = "EXTENSION")
  private String extension;

  /**
   * Whether to push the output into the output of a servlet with the executeTrans Carte/DI-Server
   * servlet
   */
  @Injection(name = "PASS_TO_SERVLET")
  private boolean servletOutput;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  @Injection(name = "SPLIT_EVERY")
  private int splitEvery;

  /** Flag: add the transformnr in the filename */
  @Injection(name = "INC_TRANSFORMNR_IN_FILENAME")
  private boolean transformNrInFilename;

  /** Flag: add the date in the filename */
  @Injection(name = "INC_DATE_IN_FILENAME")
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  @Injection(name = "INC_TIME_IN_FILENAME")
  private boolean timeInFilename;

  /** Flag: put the destination file in a zip archive */
  @Injection(name = "ZIPPED")
  private boolean zipped;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING")
  private String encoding;

  /** The name variables for the XML document: null or empty string means no xmlns is written */
  @Injection(name = "NAMESPACE")
  private String nameSpace;

  /** The name of the parent XML element */
  @Injection(name = "MAIN_ELEMENT")
  private String mainElement;

  /** The name of the repeating row XML element */
  @Injection(name = "REPEAT_ELEMENT")
  private String repeatElement;

  /** Flag: add the filenames to result filenames */
  @Injection(name = "ADD_TO_RESULT")
  private boolean addToResultFilenames;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @InjectionDeep private XmlField[] outputFields;

  /** Flag : Do not open new file when transformation start */
  @Injection(name = "DO_NOT_CREATE_FILE_AT_STARTUP")
  private boolean doNotOpenNewFileInit;

  /** Omit null elements from xml output */
  @Injection(name = "OMIT_NULL_VALUES")
  private boolean omitNullValues;

  @Injection(name = "SPEFICY_FORMAT")
  private boolean specifyFormat;

  @Injection(name = "DATE_FORMAT")
  private String dateTimeFormat;

  public XmlOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the dateInFilename. */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /** @param dateInFilename The dateInFilename to set. */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  /** @return Returns the extension. */
  public String getExtension() {
    return extension;
  }

  /** @param extension The extension to set. */
  public void setExtension(String extension) {
    this.extension = extension;
  }

  /** @return Returns the "do not open new file at init" flag. */
  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  /** @param doNotOpenNewFileInit The "do not open new file at init" flag to set. */
  public void setDoNotOpenNewFileInit(boolean doNotOpenNewFileInit) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }

  /** @return Returns the fileName. */
  public String getFileName() {
    return fileName;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /** @return Returns the splitEvery. */
  public int getSplitEvery() {
    return splitEvery;
  }

  /** @param splitEvery The splitEvery to set. */
  public void setSplitEvery(int splitEvery) {
    this.splitEvery = splitEvery;
  }

  /** @return Returns the transformNrInFilename. */
  public boolean isTransformNrInFilename() {
    return transformNrInFilename;
  }

  /** @param transformNrInFilename The transformNrInFilename to set. */
  public void setTransformNrInFilename(boolean transformNrInFilename) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /** @return Returns the timeInFilename. */
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /** @param timeInFilename The timeInFilename to set. */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  /** @return Returns the add to result filesname. */
  public boolean isAddToResultFiles() {
    return addToResultFilenames;
  }

  /** @param addtoresultfilenamesin The addtoresultfilenames to set. */
  public void setAddToResultFiles(boolean addtoresultfilenamesin) {
    this.addToResultFilenames = addtoresultfilenamesin;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat(boolean specifyFormat) {
    this.specifyFormat = specifyFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  /** @return Returns the zipped. */
  public boolean isZipped() {
    return zipped;
  }

  /** @param zipped The zipped to set. */
  public void setZipped(boolean zipped) {
    this.zipped = zipped;
  }

  /** @return Returns the outputFields. */
  public XmlField[] getOutputFields() {
    return outputFields;
  }

  /** @param outputFields The outputFields to set. */
  public void setOutputFields(XmlField[] outputFields) {
    this.outputFields = outputFields;
  }

  public void allocate(int nrFields) {
    outputFields = new XmlField[nrFields];
  }

  public Object clone() {
    XmlOutputMeta retval = (XmlOutputMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.outputFields[i] = (XmlField) outputFields[i].clone();
    }

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      XmlOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new XmlOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public XmlOutputData getTransformData() {
    return new XmlOutputData();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      setEncoding(XmlHandler.getTagValue(transformNode, "encoding"));
      setNameSpace(XmlHandler.getTagValue(transformNode, "name_space"));
      setMainElement(XmlHandler.getTagValue(transformNode, "xml_main_element"));
      setRepeatElement(XmlHandler.getTagValue(transformNode, "xml_repeat_element"));

      setFileName(XmlHandler.getTagValue(transformNode, "file", "name"));
      setExtension(XmlHandler.getTagValue(transformNode, "file", "extention"));
      setServletOutput(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "servlet_output")));

      setDoNotOpenNewFileInit(
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "do_not_open_newfile_init")));
      setTransformNrInFilename(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "split")));
      setDateInFilename(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_date")));
      setTimeInFilename(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_time")));
      setSpecifyFormat(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "SpecifyFormat")));
      setOmitNullValues(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "omit_null_values")));
      setDateTimeFormat(XmlHandler.getTagValue(transformNode, "file", "date_time_format"));

      setAddToResultFiles(
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "add_to_result_filenames")));

      setZipped("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "zipped")));
      setSplitEvery(Const.toInt(XmlHandler.getTagValue(transformNode, "file", "splitevery"), 0));

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        outputFields[i] = new XmlField();
        String contentTypeString =
            Const.NVL(XmlHandler.getTagValue(fnode, "content_type"), ContentType.Element.name());
        outputFields[i].setContentType(ContentType.valueOf(contentTypeString));
        String fieldName = XmlHandler.getTagValue(fnode, "name");
        outputFields[i].setFieldName(fieldName);
        String elementName = XmlHandler.getTagValue(fnode, "element");
        outputFields[i].setElementName(elementName == null ? "" : elementName);
        outputFields[i].setType(XmlHandler.getTagValue(fnode, "type"));
        outputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
        outputFields[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        outputFields[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        outputFields[i].setGroupingSymbol(XmlHandler.getTagValue(fnode, "group"));
        outputFields[i].setNullString(XmlHandler.getTagValue(fnode, "nullif"));
        outputFields[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        outputFields[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public String getNewLine(String fformat) {
    String nl = System.getProperty("line.separator");

    if (fformat != null) {
      if (fformat.equalsIgnoreCase("DOS")) {
        nl = "\r\n";
      } else if (fformat.equalsIgnoreCase("UNIX")) {
        nl = "\n";
      }
    }

    return nl;
  }

  public void setDefault() {
    fileName = "file";
    extension = "xml";
    transformNrInFilename = false;
    doNotOpenNewFileInit = false;
    dateInFilename = false;
    timeInFilename = false;
    addToResultFilenames = false;
    zipped = false;
    splitEvery = 0;
    encoding = Const.XML_ENCODING;
    nameSpace = "";
    dateTimeFormat = null;
    specifyFormat = false;
    omitNullValues = false;
    mainElement = "Rows";
    repeatElement = "Row";

    int nrFields = 0;

    allocate(nrFields);
  }

  public String[] getFiles(IVariables variables) {
    int copies = 1;
    int splits = 1;

    if (transformNrInFilename) {
      copies = 3;
    }

    if (splitEvery != 0) {
      splits = 3;
    }

    int nr = copies * splits;
    if (nr > 1) {
      nr++;
    }

    String[] retval = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int split = 0; split < splits; split++) {
        retval[i] = buildFilename(variables, copy, split, false);
        i++;
      }
    }
    if (i < nr) {
      retval[i] = "...";
    }

    return retval;
  }

  public String buildFilename(
      IVariables variables, int transformnr, int splitnr, boolean ziparchive) {
    SimpleDateFormat daf = new SimpleDateFormat();
    DecimalFormat df = new DecimalFormat("00000");

    // Replace possible environment variables...
    String retval = variables.resolve(fileName);
    String realextension = variables.resolve(extension);

    Date now = new Date();

    if (specifyFormat && !Utils.isEmpty(dateTimeFormat)) {
      daf.applyPattern(dateTimeFormat);
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (dateInFilename) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (timeInFilename) {
        daf.applyPattern("HHmmss");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }

    if (transformNrInFilename) {
      retval += "_" + transformnr;
    }
    if (splitEvery > 0) {
      retval += "_" + df.format(splitnr + 1);
    }

    if (zipped) {
      if (ziparchive) {
        retval += ".zip";
      } else {
        if (realextension != null && realextension.length() != 0) {
          retval += "." + realextension;
        }
      }
    } else {
      if (realextension != null && realextension.length() != 0) {
        retval += "." + realextension;
      }
    }
    return retval;
  }

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

    for (int i = 0; i < outputFields.length; i++) {
      XmlField field = outputFields[i];
      IValueMeta v = row.searchValueMeta(field.getFieldName());
      if (v != null) {
        v.setLength(field.getLength(), field.getPrecision());
      }
    }
  }

  public IRowMeta getRequiredFields(IVariables variables) throws HopException {
    RowMeta row = new RowMeta();
    for (int i = 0; i < outputFields.length; i++) {
      XmlField field = outputFields[i];
      row.addValueMeta(
          new ValueMetaBase(
              field.getFieldName(), field.getType(), field.getLength(), field.getPrecision()));
    }
    return row;
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer(600);

    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    ").append(XmlHandler.addTagValue("name_space", nameSpace));
    retval.append("    ").append(XmlHandler.addTagValue("xml_main_element", mainElement));
    retval.append("    ").append(XmlHandler.addTagValue("xml_repeat_element", repeatElement));

    retval.append("    <file>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", fileName));
    retval.append("      ").append(XmlHandler.addTagValue("extention", extension));
    retval.append("      ").append(XmlHandler.addTagValue("servlet_output", servletOutput));

    retval
        .append("      ")
        .append(XmlHandler.addTagValue("do_not_open_newfile_init", doNotOpenNewFileInit));
    retval.append("      ").append(XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_time", timeInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    retval.append("      ").append(XmlHandler.addTagValue("omit_null_values", omitNullValues));
    retval.append("      ").append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("add_to_result_filenames", addToResultFilenames));
    retval.append("      ").append(XmlHandler.addTagValue("zipped", zipped));
    retval.append("      ").append(XmlHandler.addTagValue("splitevery", splitEvery));
    retval.append("    </file>").append(Const.CR);
    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      XmlField field = outputFields[i];

      if (field.getFieldName() != null && field.getFieldName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("content_type", field.getContentType().name()));
        retval.append("        ").append(XmlHandler.addTagValue("name", field.getFieldName()));
        retval.append("        ").append(XmlHandler.addTagValue("element", field.getElementName()));
        retval.append("        ").append(XmlHandler.addTagValue("type", field.getTypeDesc()));
        retval.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("currency", field.getCurrencySymbol()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("decimal", field.getDecimalSymbol()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("group", field.getGroupingSymbol()));
        retval.append("        ").append(XmlHandler.addTagValue("nullif", field.getNullString()));
        retval.append("        ").append(XmlHandler.addTagValue("length", field.getLength()));
        retval.append("        ").append(XmlHandler.addTagValue("precision", field.getPrecision()));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

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
                  PKG, "XMLOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transforminfo);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < outputFields.length; i++) {
        int idx = prev.indexOfValue(outputFields[i].getFieldName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputFields[i].getFieldName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transforminfo);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.AllFieldsFound"),
                transforminfo);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.ExpectedInputOk"),
              transforminfo);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.ExpectedInputError"),
              transforminfo);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "XMLOutputMeta.CheckResult.FilesNotChecked"),
            transforminfo);
    remarks.add(cr);
  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @return Returns the mainElement. */
  public String getMainElement() {
    return mainElement;
  }

  /** @param mainElement The mainElement to set. */
  public void setMainElement(String mainElement) {
    this.mainElement = mainElement;
  }

  /** @return Returns the repeatElement. */
  public String getRepeatElement() {
    return repeatElement;
  }

  /** @param repeatElement The repeatElement to set. */
  public void setRepeatElement(String repeatElement) {
    this.repeatElement = repeatElement;
  }

  /** @return Returns the nameSpace. */
  public String getNameSpace() {
    return nameSpace;
  }

  /** @param nameSpace The nameSpace to set. */
  public void setNameSpace(String nameSpace) {
    this.nameSpace = nameSpace;
  }

  public void setOmitNullValues(boolean omitNullValues) {

    this.omitNullValues = omitNullValues;
  }

  public boolean isOmitNullValues() {

    return omitNullValues;
  }

  public boolean isServletOutput() {
    return servletOutput;
  }

  public void setServletOutput(boolean servletOutput) {
    this.servletOutput = servletOutput;
  }

  /**
   * Since the exported transformation that runs this will reside in a ZIP file, we can't reference
   * files relatively. So what this does is turn the name of the base path into an absolute path.
   *
   * @param variables the variable variables to use
   * @param definitions
   * @param resourceNamingInterface The repository to optionally load other resources from (to be
   *     converted to XML)
   * @param metadataProvider the metadataProvider in which non-kettle metadata could reside.
   * @return the filename of the exported resource
   */
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
      if (!Utils.isEmpty(fileName)) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName));
        fileName = resourceNamingInterface.nameResource(fileObject, variables, true);
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /** {@inheritDoc} */
  public boolean passDataToServletOutput() {
    return servletOutput;
  }
}

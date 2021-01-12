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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
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
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Transform(
    id = "TypeExitExcelWriterTransform",
    image = "excelwriter.svg",
    name = "i18n::BaseTransform.TypeLongDesc.TypeExitExcelWriterTransform",
    description = "i18n::BaseTransform.TypeTooltipDesc.TypeExitExcelWriterTransform",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/excelwriter.html")
public class ExcelWriterTransformMeta extends BaseTransformMeta
    implements ITransformMeta<ExcelWriterTransform, ExcelWriterTransformData> {
  private static final Class<?> PKG = ExcelWriterTransformMeta.class; // For Translator

  public static final String IF_FILE_EXISTS_REUSE = "reuse";
  public static final String IF_FILE_EXISTS_CREATE_NEW = "new";

  public static final String IF_SHEET_EXISTS_REUSE = "reuse";
  public static final String IF_SHEET_EXISTS_CREATE_NEW = "new";

  public static final String ROW_WRITE_OVERWRITE = "overwrite";
  public static final String ROW_WRITE_PUSH_DOWN = "push";

  /** The base name of the output file */
  private String fileName;
  /** what to do if file exists */
  private String ifFileExists;

  private String ifSheetExists;

  private boolean makeSheetActive;
  private boolean forceFormulaRecalculation = false;
  private boolean leaveExistingStylesUnchanged = false;

  /** advanced line append options */
  private int appendOffset = 0;

  private int appendEmpty = 0;
  private boolean appendOmitHeader = false;

  /** how to write rows */
  private String rowWritingMethod;

  /** where to start writing */
  private String startingCell;

  /** The file extension in case of a generated filename */
  private String extension;

  /** The password to protect the sheet */
  private String password;

  private String protectedBy;

  /** Add a header at the top of the file? */
  private boolean headerEnabled;

  /** Add a footer at the bottom of the file? */
  private boolean footerEnabled;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  private int splitEvery;

  /** Flag: add the transformnr in the filename */
  private boolean transformNrInFilename;

  /** Flag: add the date in the filename */
  private boolean dateInFilename;

  /** Flag: add the filenames to result filenames */
  private boolean addToResultFilenames;

  /** Flag: protect the sheet */
  private boolean protectsheet;

  /** Flag: add the time in the filename */
  private boolean timeInFilename;

  /** Flag: use a template */
  private boolean templateEnabled;

  private boolean templateSheetEnabled;
  private boolean templateSheetHidden;

  /** the excel template */
  private String templateFileName;

  private String templateSheetName;

  /** the excel sheet name */
  private String sheetname;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  private ExcelWriterTransformField[] outputFields;

  /** Flag : appendLines lines? */
  private boolean appendLines;

  /** Flag : Do not open new file when pipeline start */
  private boolean doNotOpenNewFileInit;

  private boolean specifyFormat;

  private String dateTimeFormat;

  /** Flag : auto size columns? */
  private boolean autosizecolums;

  /** Do we need to stream data to handle very large files? */
  private boolean streamingData;

  public ExcelWriterTransformMeta() {
    super();
  }

  public int getAppendOffset() {
    return appendOffset;
  }

  public void setAppendOffset(int appendOffset) {
    this.appendOffset = appendOffset;
  }

  public int getAppendEmpty() {
    return appendEmpty;
  }

  public void setAppendEmpty(int appendEmpty) {
    this.appendEmpty = appendEmpty >= 0 ? appendEmpty : 0;
  }

  /** @return Returns the dateInFilename. */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /** @param dateInFilename The dateInFilename to set. */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  public boolean isAppendOmitHeader() {
    return appendOmitHeader;
  }

  public void setAppendOmitHeader(boolean appendOmitHeader) {
    this.appendOmitHeader = appendOmitHeader;
  }

  public String getStartingCell() {
    return startingCell;
  }

  public void setStartingCell(String startingCell) {
    this.startingCell = startingCell;
  }

  public String getRowWritingMethod() {
    return rowWritingMethod;
  }

  public void setRowWritingMethod(String rowWritingMethod) {
    this.rowWritingMethod = rowWritingMethod;
  }

  public String getIfFileExists() {
    return ifFileExists;
  }

  public void setIfFileExists(String ifFileExists) {
    this.ifFileExists = ifFileExists;
  }

  public String getIfSheetExists() {
    return ifSheetExists;
  }

  public void setIfSheetExists(String ifSheetExists) {
    this.ifSheetExists = ifSheetExists;
  }

  public String getProtectedBy() {
    return protectedBy;
  }

  public void setProtectedBy(String protectedBy) {
    this.protectedBy = protectedBy;
  }

  /** @return Returns the extension. */
  public String getExtension() {
    return extension;
  }

  /** @param extension The extension to set. */
  public void setExtension(String extension) {
    this.extension = extension;
  }

  /** @return Returns the fileName. */
  public String getFileName() {
    return fileName;
  }

  /** @return Returns the password. */
  public String getPassword() {
    return password;
  }

  /** @return Returns the sheet name. */
  public String getSheetname() {
    return sheetname;
  }

  /** @param sheetname The sheet name. */
  public void setSheetname(String sheetname) {
    this.sheetname = sheetname;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /** @param password teh passwoed to set. */
  public void setPassword(String password) {
    this.password = password;
  }

  /** @return Returns the footer. */
  public boolean isFooterEnabled() {
    return footerEnabled;
  }

  /** @param footer The footer to set. */
  public void setFooterEnabled(boolean footer) {
    this.footerEnabled = footer;
  }

  /** @return Returns the autosizecolums. */
  public boolean isAutoSizeColums() {
    return autosizecolums;
  }

  /** @param autosizecolums The autosizecolums to set. */
  public void setAutoSizeColums(boolean autosizecolums) {
    this.autosizecolums = autosizecolums;
  }

  /** @return Returns the header. */
  public boolean isHeaderEnabled() {
    return headerEnabled;
  }

  /** @param header The header to set. */
  public void setHeaderEnabled(boolean header) {
    this.headerEnabled = header;
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

  /** @return Returns the splitEvery. */
  public int getSplitEvery() {
    return splitEvery;
  }

  /** @return Returns the add to result filesname. */
  public boolean isAddToResultFiles() {
    return addToResultFilenames;
  }

  /** @param addtoresultfilenamesin The addtoresultfilenames to set. */
  public void setAddToResultFiles(boolean addtoresultfilenamesin) {
    this.addToResultFilenames = addtoresultfilenamesin;
  }

  /** @param splitEvery The splitEvery to set. */
  public void setSplitEvery(int splitEvery) {
    this.splitEvery = splitEvery >= 0 ? splitEvery : 0;
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

  /** @return Returns the protectsheet. */
  public boolean isSheetProtected() {
    return protectsheet;
  }

  /** @param timeInFilename The timeInFilename to set. */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  /** @param protectsheet the value to set. */
  public void setProtectSheet(boolean protectsheet) {
    this.protectsheet = protectsheet;
  }

  /** @return Returns the outputFields. */
  public ExcelWriterTransformField[] getOutputFields() {
    return outputFields;
  }

  /** @param outputFields The outputFields to set. */
  public void setOutputFields(ExcelWriterTransformField[] outputFields) {
    this.outputFields = outputFields;
  }

  /** @return Returns the template. */
  public boolean isTemplateEnabled() {
    return templateEnabled;
  }

  /** @param template The template to set. */
  public void setTemplateEnabled(boolean template) {
    this.templateEnabled = template;
  }

  public boolean isTemplateSheetEnabled() {
    return templateSheetEnabled;
  }

  public void setTemplateSheetEnabled(boolean templateSheetEnabled) {
    this.templateSheetEnabled = templateSheetEnabled;
  }

  /** @return Returns the templateFileName. */
  public String getTemplateFileName() {
    return templateFileName;
  }

  /** @param templateFileName The templateFileName to set. */
  public void setTemplateFileName(String templateFileName) {
    this.templateFileName = templateFileName;
  }

  public String getTemplateSheetName() {
    return templateSheetName;
  }

  public void setTemplateSheetName(String templateSheetName) {
    this.templateSheetName = templateSheetName;
  }

  /** @return Returns the "do not open new file at init" flag. */
  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  /** @param doNotOpenNewFileInit The "do not open new file at init" flag to set. */
  public void setDoNotOpenNewFileInit(boolean doNotOpenNewFileInit) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }

  /** @return Returns the appendLines. */
  public boolean isAppendLines() {
    return appendLines;
  }

  /** @param append The appendLines to set. */
  public void setAppendLines(boolean append) {
    this.appendLines = append;
  }

  public void setMakeSheetActive(boolean makeSheetActive) {
    this.makeSheetActive = makeSheetActive;
  }

  public boolean isMakeSheetActive() {
    return makeSheetActive;
  }

  public boolean isForceFormulaRecalculation() {
    return forceFormulaRecalculation;
  }

  public void setForceFormulaRecalculation(boolean forceFormulaRecalculation) {
    this.forceFormulaRecalculation = forceFormulaRecalculation;
  }

  public boolean isLeaveExistingStylesUnchanged() {
    return leaveExistingStylesUnchanged;
  }

  public void setLeaveExistingStylesUnchanged(boolean leaveExistingStylesUnchanged) {
    this.leaveExistingStylesUnchanged = leaveExistingStylesUnchanged;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    outputFields = new ExcelWriterTransformField[nrFields];
  }

  @Override
  public Object clone() {
    ExcelWriterTransformMeta retval = (ExcelWriterTransformMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.outputFields[i] = (ExcelWriterTransformField) outputFields[i].clone();
    }

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      headerEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      footerEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "footer"));
      appendOmitHeader =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "appendOmitHeader"));
      appendLines = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "appendLines"));
      makeSheetActive =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "makeSheetActive"));
      appendOffset = Const.toInt(XmlHandler.getTagValue(transformNode, "appendOffset"), 0);
      appendEmpty = Const.toInt(XmlHandler.getTagValue(transformNode, "appendEmpty"), 0);

      startingCell = XmlHandler.getTagValue(transformNode, "startingCell");
      rowWritingMethod = XmlHandler.getTagValue(transformNode, "rowWritingMethod");
      forceFormulaRecalculation =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "forceFormulaRecalculation"));
      leaveExistingStylesUnchanged =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "leaveExistingStylesUnchanged"));

      String addToResult = XmlHandler.getTagValue(transformNode, "add_to_result_filenames");
      if (Utils.isEmpty(addToResult)) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = "Y".equalsIgnoreCase(addToResult);
      }

      fileName = XmlHandler.getTagValue(transformNode, "file", "name");
      extension = XmlHandler.getTagValue(transformNode, "file", "extention");

      doNotOpenNewFileInit =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "do_not_open_newfile_init"));
      transformNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "split"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_time"));
      specifyFormat =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "SpecifyFormat"));
      dateTimeFormat = XmlHandler.getTagValue(transformNode, "file", "date_time_format");

      autosizecolums =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "autosizecolums"));
      streamingData =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "stream_data"));
      protectsheet =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "protect_sheet"));
      password =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "file", "password"));
      protectedBy = XmlHandler.getTagValue(transformNode, "file", "protected_by");
      splitEvery = Const.toInt(XmlHandler.getTagValue(transformNode, "file", "splitevery"), 0);

      templateEnabled =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "template", "enabled"));
      templateSheetEnabled =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "template", "sheet_enabled"));
      templateSheetHidden =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "template", "hidden"));
      templateFileName = XmlHandler.getTagValue(transformNode, "template", "filename");
      templateSheetName = XmlHandler.getTagValue(transformNode, "template", "sheetname");
      sheetname = XmlHandler.getTagValue(transformNode, "file", "sheetname");
      ifFileExists = XmlHandler.getTagValue(transformNode, "file", "if_file_exists");
      ifSheetExists = XmlHandler.getTagValue(transformNode, "file", "if_sheet_exists");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        outputFields[i] = new ExcelWriterTransformField();
        outputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
        outputFields[i].setType(XmlHandler.getTagValue(fnode, "type"));
        outputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
        outputFields[i].setTitle(XmlHandler.getTagValue(fnode, "title"));
        outputFields[i].setTitleStyleCell(XmlHandler.getTagValue(fnode, "titleStyleCell"));
        outputFields[i].setStyleCell(XmlHandler.getTagValue(fnode, "styleCell"));
        outputFields[i].setCommentField(XmlHandler.getTagValue(fnode, "commentField"));
        outputFields[i].setCommentAuthorField(XmlHandler.getTagValue(fnode, "commentAuthorField"));
        outputFields[i].setFormula(
            XmlHandler.getTagValue(fnode, "formula") != null
                && XmlHandler.getTagValue(fnode, "formula").equalsIgnoreCase("Y"));
        outputFields[i].setHyperlinkField(XmlHandler.getTagValue(fnode, "hyperlinkField"));
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

  @Override
  public void setDefault() {

    autosizecolums = false;
    streamingData = false;
    headerEnabled = true;
    footerEnabled = false;
    fileName = "file";
    extension = "xls";
    doNotOpenNewFileInit = false;
    transformNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    dateTimeFormat = null;
    specifyFormat = false;
    addToResultFilenames = true;
    protectsheet = false;
    splitEvery = 0;
    templateEnabled = false;
    templateFileName = "template.xls";
    templateSheetHidden = false;
    sheetname = "Sheet1";
    appendLines = false;
    ifFileExists = IF_FILE_EXISTS_CREATE_NEW;
    ifSheetExists = IF_SHEET_EXISTS_CREATE_NEW;
    startingCell = "A1";
    rowWritingMethod = ROW_WRITE_OVERWRITE;
    appendEmpty = 0;
    appendOffset = 0;
    appendOmitHeader = false;
    makeSheetActive = true;
    forceFormulaRecalculation = false;

    allocate(0);
  }

  public String[] getFiles(IVariables variables) {
    int copies = 1;
    int splits = 1;

    if (transformNrInFilename) {
      copies = 3;
    }

    if (splitEvery != 0) {
      splits = 4;
    }

    int nr = copies * splits;
    if (nr > 1) {
      nr++;
    }

    String[] retval = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int split = 0; split < splits; split++) {
        retval[i] = buildFilename(variables, copy, split);
        i++;
      }
    }
    if (i < nr) {
      retval[i] = "...";
    }

    return retval;
  }

  public String buildFilename(IVariables variables, int transformnr, int splitnr) {
    SimpleDateFormat daf = new SimpleDateFormat();

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
        daf.applyPattern("yyyMMdd");
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
      retval += "_" + splitnr;
    }

    if (realextension != null && realextension.length() != 0) {
      retval += "." + realextension;
    }

    return retval;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (r == null) {
      r = new RowMeta(); // give back values
    }

    // No values are added to the row in this type of transform
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(800);

    retval.append("    ").append(XmlHandler.addTagValue("header", headerEnabled));
    retval.append("    ").append(XmlHandler.addTagValue("footer", footerEnabled));
    retval.append("    ").append(XmlHandler.addTagValue("makeSheetActive", makeSheetActive));
    retval.append("    ").append(XmlHandler.addTagValue("rowWritingMethod", rowWritingMethod));
    retval.append("    ").append(XmlHandler.addTagValue("startingCell", startingCell));
    retval.append("    ").append(XmlHandler.addTagValue("appendOmitHeader", appendOmitHeader));
    retval.append("    ").append(XmlHandler.addTagValue("appendOffset", appendOffset));
    retval.append("    ").append(XmlHandler.addTagValue("appendEmpty", appendEmpty));
    retval.append("    ").append(XmlHandler.addTagValue("rowWritingMethod", rowWritingMethod));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("forceFormulaRecalculation", forceFormulaRecalculation));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("leaveExistingStylesUnchanged", leaveExistingStylesUnchanged));
    retval.append("    " + XmlHandler.addTagValue("appendLines", appendLines));
    retval.append("    " + XmlHandler.addTagValue("add_to_result_filenames", addToResultFilenames));

    retval.append("    <file>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", fileName));
    retval.append("      ").append(XmlHandler.addTagValue("extention", extension));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("do_not_open_newfile_init", doNotOpenNewFileInit));
    retval.append("      ").append(XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_time", timeInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    retval.append("      ").append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    retval.append("      ").append(XmlHandler.addTagValue("sheetname", sheetname));
    retval.append("      ").append(XmlHandler.addTagValue("autosizecolums", autosizecolums));
    retval.append("      ").append(XmlHandler.addTagValue("stream_data", streamingData));
    retval.append("      ").append(XmlHandler.addTagValue("protect_sheet", protectsheet));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    retval.append("      ").append(XmlHandler.addTagValue("protected_by", protectedBy));
    retval.append("      ").append(XmlHandler.addTagValue("splitevery", splitEvery));
    retval.append("      ").append(XmlHandler.addTagValue("if_file_exists", ifFileExists));
    retval.append("      ").append(XmlHandler.addTagValue("if_sheet_exists", ifSheetExists));

    retval.append("      </file>").append(Const.CR);

    retval.append("    <template>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("enabled", templateEnabled));
    retval.append("      ").append(XmlHandler.addTagValue("sheet_enabled", templateSheetEnabled));
    retval.append("      ").append(XmlHandler.addTagValue("filename", templateFileName));
    retval.append("      ").append(XmlHandler.addTagValue("sheetname", templateSheetName));
    retval.append("      ").append(XmlHandler.addTagValue("hidden", templateSheetHidden));
    retval.append("    </template>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      ExcelWriterTransformField field = outputFields[i];

      if (field.getName() != null && field.getName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", field.getName()));
        retval.append("        ").append(XmlHandler.addTagValue("type", field.getTypeDesc()));
        retval.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
        retval.append("        ").append(XmlHandler.addTagValue("title", field.getTitle()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("titleStyleCell", field.getTitleStyleCell()));
        retval.append("        ").append(XmlHandler.addTagValue("styleCell", field.getStyleCell()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("commentField", field.getCommentField()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("commentAuthorField", field.getCommentAuthorField()));
        retval.append("        ").append(XmlHandler.addTagValue("formula", field.isFormula()));
        retval
            .append("        ")
            .append(XmlHandler.addTagValue("hyperlinkField", field.getHyperlinkField()));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
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
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ExcelWriterTransformMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < outputFields.length; i++) {
        int idx = prev.indexOfValue(outputFields[i].getName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputFields[i].getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "ExcelWriterTransformMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExcelWriterTransformMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExcelWriterTransformMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "ExcelWriterTransformMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "ExcelWriterTransformMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
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
      //
      if (!Utils.isEmpty(fileName)) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName));
        fileName = iResourceNaming.nameResource(fileObject, variables, true);
      }
      if (!Utils.isEmpty(templateFileName)) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(templateFileName));
        templateFileName = iResourceNaming.nameResource(fileObject, variables, true);
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public ExcelWriterTransform createTransform(
      TransformMeta transformMeta,
      ExcelWriterTransformData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExcelWriterTransform(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ExcelWriterTransformData getTransformData() {
    return new ExcelWriterTransformData();
  }

  /** @return the streamingData */
  public boolean isStreamingData() {
    return streamingData;
  }

  /** @param streamingData the streamingData to set */
  public void setStreamingData(boolean streamingData) {
    this.streamingData = streamingData;
  }

  public boolean isTemplateSheetHidden() {
    return templateSheetHidden;
  }

  public void setTemplateSheetHidden(boolean hide) {
    this.templateSheetHidden = hide;
  }
}

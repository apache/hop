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

package org.apache.hop.pipeline.transforms.textfileoutput;

import com.google.common.annotations.VisibleForTesting;
import java.text.SimpleDateFormat;
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
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "TextFileOutput",
    image = "textfileoutput.svg",
    name = "i18n::TextFileOutput.Name",
    description = "i18n::TextFileOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::TextFileOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/textfileoutput.html")
@InjectionSupported(
    localizationPrefix = "TextFileOutput.Injection.",
    groups = {"OUTPUT_FIELDS"})
@Getter
@Setter
public class TextFileOutputMeta extends BaseTransformMeta<TextFileOutput, TextFileOutputData> {

  private static final Class<?> PKG = TextFileOutputMeta.class;

  protected static final int FILE_COMPRESSION_TYPE_NONE = 0;

  protected static final int FILE_COMPRESSION_TYPE_ZIP = 1;

  protected static final String[] fileCompressionTypeCodes = new String[] {"None", "Zip"};

  public static final String[] formatMapperLineTerminator =
      new String[] {"DOS", "UNIX", "CR", "None"};
  public static final String CONST_FORMAT = "format";
  public static final String CONST_FIELD = "field";
  public static final String CONST_SPACES_LONG = "        ";
  public static final String CONST_SPACES = "      ";

  /** Flag: add the transformnr in the filename */
  @Injection(name = "INC_TRANSFORMNR_IN_FILENAME")
  protected boolean transformNrInFilename;

  /** Flag: add the partition number in the filename */
  @Injection(name = "INC_PARTNR_IN_FILENAME")
  protected boolean partNrInFilename;

  /** Flag: add the date in the filename */
  @Injection(name = "INC_DATE_IN_FILENAME")
  protected boolean dateInFilename;

  /** Flag: add the time in the filename */
  @Injection(name = "INC_TIME_IN_FILENAME")
  protected boolean timeInFilename;

  /** The file extention in case of a generated filename */
  @Injection(name = "EXTENSION")
  protected String extension;

  /** The base name of the output file */
  @Injection(name = "FILENAME")
  protected String fileName;

  /** Whether to treat this as a command to be executed and piped into */
  @Injection(name = "RUN_AS_COMMAND")
  private boolean fileAsCommand;

  /** Flag : Do not open new file when pipeline start */
  @Injection(name = "SPECIFY_DATE_FORMAT")
  private boolean specifyingFormat;

  /** The date format appended to the file name */
  @Injection(name = "DATE_FORMAT")
  private String dateTimeFormat;

  /** The file compression: None, Zip or Gzip */
  @Injection(name = "COMPRESSION")
  private String fileCompression;

  /**
   * Whether to push the output into the output of a servlet with the executePipeline
   * HopServer/DI-Server servlet
   */
  @Injection(name = "PASS_TO_SERVLET")
  private boolean servletOutput;

  /** Flag: create parent folder, default to true */
  @Injection(name = "CREATE_PARENT_FOLDER")
  private boolean createparentfolder = true;

  /** The separator to choose for the CSV file */
  @Injection(name = "SEPARATOR")
  private String separator;

  /** The enclosure to use in case the separator is part of a field's value */
  @Injection(name = "ENCLOSURE")
  private String enclosure;

  /**
   * Setting to allow the enclosure to be always surrounding a String value, even when there is no
   * separator inside
   */
  @Injection(name = "FORCE_ENCLOSURE")
  private boolean enclosureForced;

  /**
   * Setting to allow for backwards compatibility where the enclosure did not show up at all if
   * Force Enclosure was not checked
   */
  @Injection(name = "DISABLE_ENCLOSURE_FIX")
  private boolean disableEnclosureFix;

  /** Add a header at the top of the file? */
  @Injection(name = "HEADER")
  private boolean headerEnabled;

  /** Add a footer at the bottom of the file? */
  @Injection(name = "FOOTER")
  private boolean footerEnabled;

  /**
   * The file format: DOS or Unix It could be injected using the key "FORMAT" see the setter {@link
   * TextFileOutputMeta#setFileFormat(String)}.
   */
  private String fileFormat;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  @Injection(name = "SPLIT_EVERY")
  private String splitEveryRows;

  /** Flag to indicate the we want to append to the end of an existing file (if it exists) */
  @Injection(name = "APPEND")
  private boolean fileAppended;

  /** Flag: pad fields to their specified length */
  @Injection(name = "RIGHT_PAD_FIELDS")
  private boolean padded;

  /** Flag: Fast dump data without field formatting */
  @Injection(name = "FAST_DATA_DUMP")
  private boolean fastDump;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @InjectionDeep private TextFileField[] outputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING")
  private String encoding;

  /**
   * The string to use for append to end line of the whole file: null or empty string means no line
   * needed
   */
  @Injection(name = "ADD_ENDING_LINE")
  private String endedLine;

  /* Specification if file name is in field */

  @Injection(name = "FILENAME_IN_FIELD")
  private boolean fileNameInField;

  @Injection(name = "FILENAME_FIELD")
  private String fileNameField;

  /** Calculated value ... */
  @Injection(name = "NEW_LINE")
  private String newline;

  /** Flag: add the filenames to result filenames */
  @Injection(name = "ADD_TO_RESULT")
  private boolean addToResultFilenames;

  /** Flag : Do not open new file when pipeline start */
  @Injection(name = "DO_NOT_CREATE_FILE_AT_STARTUP")
  private boolean doNotOpenNewFileInit;

  @Injection(name = "SCHEMA_DEFINITION")
  private String schemaDefinition;

  @Injection(name = "IGNORE_FIELDS")
  public boolean ignoreFields;

  public TextFileOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @param createparentfolder The createparentfolder to set.
   */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  /**
   * @return Returns the createparentfolder.
   */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /**
   * @return Returns the enclosureFixDisabled.
   */
  public boolean isEnclosureFixDisabled() {
    return disableEnclosureFix;
  }

  /**
   * @param disableEnclosureFix The enclosureFixDisabled to set.
   */
  public void setEnclosureFixDisabled(boolean disableEnclosureFix) {
    this.disableEnclosureFix = disableEnclosureFix;
  }

  /**
   * @return Returns the add to result filesname.
   */
  public boolean isAddToResultFiles() {
    return addToResultFilenames;
  }

  /**
   * @param addtoresultfilenamesin The addtoresultfilenames to set.
   */
  public void setAddToResultFiles(boolean addtoresultfilenamesin) {
    this.addToResultFilenames = addtoresultfilenamesin;
  }

  /**
   * @param fileFormat The fileFormat to set.
   */
  @Injection(name = "FORMAT")
  public void setFileFormat(String fileFormat) {
    this.fileFormat = fileFormat;
    this.newline = getNewLine(fileFormat);
  }

  /**
   * @return Returns the splitEvery.
   * @deprecated use {@link #getSplitEvery(IVariables)} or {@link #getSplitEveryRows()}
   */
  @Deprecated(since = "2.0")
  public int getSplitEvery() {
    return Const.toInt(splitEveryRows, 0);
  }

  /**
   * @param varSpace for variable substitution
   * @return At how many rows to split into another file.
   */
  public int getSplitEvery(IVariables varSpace) {
    return Const.toInt(varSpace == null ? splitEveryRows : varSpace.resolve(splitEveryRows), 0);
  }

  /**
   * @return <i>1</i> if <i>isFooterEnabled()</i> and <i>0</i> otherwise
   */
  public int getFooterShift() {
    return isFooterEnabled() ? 1 : 0;
  }

  /**
   * @param splitEvery The splitEvery to set.
   * @deprecated use {@link #setSplitEveryRows(String)}
   */
  @Deprecated(since = "2.0")
  public void setSplitEvery(int splitEvery) {
    splitEveryRows = Integer.toString(splitEvery);
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrFields) {
    outputFields = new TextFileField[nrFields];
  }

  @Override
  public Object clone() {
    TextFileOutputMeta retval = (TextFileOutputMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.outputFields[i] = (TextFileField) outputFields[i].clone();
    }

    return retval;
  }

  protected void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      separator = XmlHandler.getTagValue(transformNode, "separator");
      if (separator == null) {
        separator = "";
      }

      enclosure = XmlHandler.getTagValue(transformNode, "enclosure");
      if (enclosure == null) {
        enclosure = "";
      }

      enclosureForced =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "enclosure_forced"));

      String sDisableEnclosureFix = XmlHandler.getTagValue(transformNode, "enclosure_fix_disabled");
      // Default this value to true for backwards compatibility
      if (sDisableEnclosureFix == null) {
        disableEnclosureFix = true;
      } else {
        disableEnclosureFix = "Y".equalsIgnoreCase(sDisableEnclosureFix);
      }

      // Default createparentfolder to true if the tag is missing
      String createParentFolderTagValue =
          XmlHandler.getTagValue(transformNode, "create_parent_folder");
      createparentfolder =
          (createParentFolderTagValue == null)
              ? true
              : "Y".equalsIgnoreCase(createParentFolderTagValue);

      headerEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      footerEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "footer"));
      fileFormat = XmlHandler.getTagValue(transformNode, CONST_FORMAT);
      setFileCompression(XmlHandler.getTagValue(transformNode, "compression"));
      if (getFileCompression() == null) {
        if ("Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "zipped"))) {
          setFileCompression(fileCompressionTypeCodes[FILE_COMPRESSION_TYPE_ZIP]);
        } else {
          setFileCompression(fileCompressionTypeCodes[FILE_COMPRESSION_TYPE_NONE]);
        }
      }
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      schemaDefinition = XmlHandler.getTagValue(transformNode, "schema_definition");
      ignoreFields = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "ignore_fields"));

      endedLine = XmlHandler.getTagValue(transformNode, "endedLine");
      if (endedLine == null) {
        endedLine = "";
      }

      fileName = loadSource(transformNode, metadataProvider);
      servletOutput =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "servlet_output"));
      doNotOpenNewFileInit =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "do_not_open_new_file_init"));
      extension =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, "file", "extention"),
              XmlHandler.getTagValue(transformNode, "file", "extension"));
      fileAppended = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "append"));
      transformNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "split"));
      partNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "haspartno"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_time"));
      setSpecifyingFormat(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "SpecifyFormat")));
      setDateTimeFormat(XmlHandler.getTagValue(transformNode, "file", "date_time_format"));

      String addToResultFiles =
          XmlHandler.getTagValue(transformNode, "file", "add_to_result_filenames");
      if (Utils.isEmpty(addToResultFiles)) {
        addToResultFilenames = true;
      } else {
        addToResultFilenames = "Y".equalsIgnoreCase(addToResultFiles);
      }

      padded = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "pad"));
      fastDump = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "fast_dump"));
      splitEveryRows = XmlHandler.getTagValue(transformNode, "file", "splitevery");

      newline = getNewLine(fileFormat);

      fileNameInField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "fileNameInField"));
      fileNameField = XmlHandler.getTagValue(transformNode, "fileNameField");

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, CONST_FIELD);

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, CONST_FIELD, i);

        outputFields[i] = new TextFileField();
        outputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
        outputFields[i].setType(XmlHandler.getTagValue(fnode, "type"));
        outputFields[i].setFormat(XmlHandler.getTagValue(fnode, CONST_FORMAT));
        outputFields[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        outputFields[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        outputFields[i].setGroupingSymbol(XmlHandler.getTagValue(fnode, "group"));
        outputFields[i].setTrimType(
            ValueMetaString.getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));
        outputFields[i].setRoundingType(
            Const.NVL(XmlHandler.getTagValue(fnode, "roundingType"), "half_even"));
        outputFields[i].setNullString(XmlHandler.getTagValue(fnode, "nullif"));
        outputFields[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        outputFields[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void readData(Node transformNode) throws HopXmlException {
    readData(transformNode, null);
  }

  public String getNewLine(String fformat) {
    String nl = System.getProperty("line.separator");

    if (fformat != null) {
      if (fformat.equalsIgnoreCase("DOS")) {
        nl = "\r\n";
      } else if (fformat.equalsIgnoreCase("UNIX")) {
        nl = "\n";
      } else if (fformat.equalsIgnoreCase("CR")) {
        nl = "\r";
      } else if (fformat.equalsIgnoreCase("None")) {
        nl = "";
      }
    }

    return nl;
  }

  @Override
  public void setDefault() {
    createparentfolder = true; // Default createparentfolder to true
    separator = ";";
    enclosure = "\"";
    setSpecifyingFormat(false);
    setDateTimeFormat(null);
    enclosureForced = false;
    disableEnclosureFix = false;
    headerEnabled = true;
    footerEnabled = false;
    fileFormat = "DOS";
    setFileCompression(fileCompressionTypeCodes[FILE_COMPRESSION_TYPE_NONE]);
    fileName = "";
    servletOutput = false;
    doNotOpenNewFileInit = true;
    extension = "";
    transformNrInFilename = false;
    partNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    padded = false;
    fastDump = false;
    addToResultFilenames = true;

    newline = getNewLine(fileFormat);

    int i;
    int nrFields = 0;

    allocate(nrFields);

    for (i = 0; i < nrFields; i++) {
      outputFields[i] = new TextFileField();

      outputFields[i].setName(CONST_FIELD + i);
      outputFields[i].setType("Number");
      outputFields[i].setFormat(" 0,000,000.00;-0,000,000.00");
      outputFields[i].setCurrencySymbol("");
      outputFields[i].setDecimalSymbol(",");
      outputFields[i].setGroupingSymbol(".");
      outputFields[i].setNullString("");
      outputFields[i].setLength(-1);
      outputFields[i].setPrecision(-1);
    }
    fileAppended = false;
  }

  public String buildFilename(
      String filename,
      String extension,
      IVariables variables,
      int transformnr,
      String partnr,
      int splitnr,
      boolean beamContext,
      String transformId,
      int bundleNr,
      boolean ziparchive,
      TextFileOutputMeta meta) {

    final String realFileName = variables.resolve(filename);
    final String realExtension = variables.resolve(extension);
    return buildFilename(
        variables,
        realFileName,
        realExtension,
        Integer.toString(transformnr),
        partnr,
        Integer.toString(splitnr),
        beamContext,
        transformId,
        bundleNr,
        new Date(),
        ziparchive,
        true,
        meta);
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // No values are added to the row in this type of transform
    // However, in case of Fixed length records,
    // the field precisions and lengths are altered!
    for (int i = 0; i < outputFields.length; i++) {
      TextFileField field = outputFields[i];
      IValueMeta v = row.searchValueMeta(field.getName());
      if (v != null) {
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        if (field.getFormat() != null) {
          v.setConversionMask(field.getFormat());
        }
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupingSymbol());
        v.setCurrencySymbol(field.getCurrencySymbol());
        v.setOutputPaddingEnabled(isPadded());
        v.setTrimType(field.getTrimType());
        v.setRoundingType(field.getRoundingType());
        if (!Utils.isEmpty(getEncoding())) {
          v.setStringEncoding(getEncoding());
        }

        // enable output padding by default to be compatible with v2.5.x
        //
        v.setOutputPaddingEnabled(true);
      }
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(800);

    retval.append("    ").append(XmlHandler.addTagValue("schema_definition", schemaDefinition));
    retval.append("    ").append(XmlHandler.addTagValue("ignore_fields", ignoreFields));
    retval.append("    ").append(XmlHandler.addTagValue("separator", separator));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure", enclosure));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure_forced", enclosureForced));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("enclosure_fix_disabled", disableEnclosureFix));
    retval.append("    ").append(XmlHandler.addTagValue("header", headerEnabled));
    retval.append("    ").append(XmlHandler.addTagValue("footer", footerEnabled));
    retval.append("    ").append(XmlHandler.addTagValue(CONST_FORMAT, fileFormat));
    retval.append("    ").append(XmlHandler.addTagValue("compression", getFileCompression()));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    ").append(XmlHandler.addTagValue("endedLine", endedLine));
    retval.append("    " + XmlHandler.addTagValue("fileNameInField", fileNameInField));
    retval.append("    " + XmlHandler.addTagValue("fileNameField", fileNameField));
    retval.append("    " + XmlHandler.addTagValue("create_parent_folder", createparentfolder));
    retval.append("    <file>").append(Const.CR);
    saveFileOptions(retval);
    retval.append("    </file>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      TextFileField field = outputFields[i];

      if (!Utils.isEmpty(field.getName())) {
        retval.append("      <field>").append(Const.CR);
        retval.append(CONST_SPACES_LONG).append(XmlHandler.addTagValue("name", field.getName()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("type", field.getTypeDesc()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue(CONST_FORMAT, field.getFormat()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("currency", field.getCurrencySymbol()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("decimal", field.getDecimalSymbol()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("group", field.getGroupingSymbol()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("nullif", field.getNullString()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("trim_type", field.getTrimTypeCode()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("roundingType", field.getRoundingType()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("length", field.getLength()));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("precision", field.getPrecision()));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

  protected void saveFileOptions(StringBuilder retval) {
    saveSource(retval, fileName);
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("servlet_output", servletOutput));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("do_not_open_new_file_init", doNotOpenNewFileInit));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("extention", extension));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("append", fileAppended));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("haspartno", partNrInFilename));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_time", timeInFilename));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("SpecifyFormat", isSpecifyingFormat()));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("date_time_format", getDateTimeFormat()));

    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("add_to_result_filenames", addToResultFilenames));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("pad", padded));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("fast_dump", fastDump));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("splitevery", splitEveryRows));
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
                  PKG, "TextFileOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
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
                PKG, "TextFileOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "TextFileOutputMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of the base path into an absolute path.
   *
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
      if (!fileNameInField) {

        if (!Utils.isEmpty(fileName)) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName), variables);
          fileName = iResourceNaming.nameResource(fileObject, variables, true);
        }
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  protected String loadSource(Node transformNode, IHopMetadataProvider metadataProvider) {
    return XmlHandler.getTagValue(transformNode, "file", "name");
  }

  protected void saveSource(StringBuilder retVal, String value) {
    retVal.append(CONST_SPACES).append(XmlHandler.addTagValue("name", fileName));
  }

  /** {@inheritDoc} */
  @Override
  public boolean passDataToServletOutput() {
    return servletOutput;
  }

  public String[] getFiles(final IVariables variables) {
    return getFiles(variables, true);
  }

  private String[] getFiles(final IVariables variables, final boolean showSamples) {

    String realFileName = variables.resolve(fileName);
    String realExtension = variables.resolve(extension);

    return getFiles(realFileName, realExtension, showSamples);
  }

  @VisibleForTesting
  String[] getFiles(
      final String realFileName, final String realExtension, final boolean showSamples) {
    final Date now = new Date();

    if (showSamples) {
      int copies = 1;
      int splits = 1;
      int parts = 1;

      if (isTransformNrInFilename()) {
        copies = 3;
      }

      if (isPartNrInFilename()) {
        parts = 3;
      }

      if (getSplitEvery() != 0) {
        splits = 3;
      }

      int nr = copies * parts * splits;
      if (nr > 1) {
        nr++;
      }

      String[] retval = new String[nr];

      int i = 0;
      for (int transform = 0; transform < copies; transform++) {
        for (int part = 0; part < parts; part++) {
          for (int split = 0; split < splits; split++) {
            retval[i] =
                buildFilename(
                    realFileName,
                    realExtension,
                    transform + "",
                    getPartPrefix() + part,
                    split + "",
                    false,
                    "",
                    1,
                    now,
                    false,
                    showSamples);
            i++;
          }
        }
      }
      if (i < nr) {
        retval[i] = "...";
      }

      return retval;
    } else {
      return new String[] {
        buildFilename(
            realFileName,
            realExtension,
            "<transform>",
            "<partition>",
            "<split>",
            false,
            "",
            1,
            now,
            false,
            showSamples)
      };
    }
  }

  protected String getPartPrefix() {
    return "";
  }

  private String buildFilename(
      final String realFileName,
      final String realExtension,
      final String transformnr,
      final String partnr,
      final String splitnr,
      final boolean beamContext,
      final String transformId,
      final int bundleNr,
      final Date date,
      final boolean ziparchive,
      final boolean showSamples) {
    return buildFilename(
        realFileName,
        realExtension,
        transformnr,
        partnr,
        splitnr,
        beamContext,
        transformId,
        bundleNr,
        date,
        ziparchive,
        showSamples,
        this);
  }

  protected String buildFilename(
      final String realFileName,
      final String realExtension,
      final String transformnr,
      final String partnr,
      final String splitnr,
      final boolean beamContext,
      final String transformId,
      final int bundleNr,
      final Date date,
      final boolean ziparchive,
      final boolean showSamples,
      final TextFileOutputMeta meta) {
    return buildFilename(
        null,
        realFileName,
        realExtension,
        transformnr,
        partnr,
        splitnr,
        beamContext,
        transformId,
        bundleNr,
        date,
        ziparchive,
        showSamples,
        meta);
  }

  protected String buildFilename(
      final IVariables variables,
      final String realFileName,
      final String realExtension,
      final String transformnr,
      final String partnr,
      final String splitnr,
      final boolean beamContext,
      final String transformId,
      final int bundleNr,
      final Date date,
      final boolean ziparchive,
      final boolean showSamples,
      final TextFileOutputMeta meta) {

    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = realFileName;

    if (meta.isFileAsCommand()) {
      return retval;
    }

    Date now = date == null ? new Date() : date;

    if (meta.isSpecifyingFormat() && !Utils.isEmpty(meta.getDateTimeFormat())) {
      daf.applyPattern(meta.getDateTimeFormat());
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (meta.isDateInFilename()) {
        if (showSamples) {
          daf.applyPattern("yyyMMdd");
          String d = daf.format(now);
          retval += "_" + d;
        } else {
          retval += "_<yyyMMdd>";
        }
      }
      if (meta.isTimeInFilename()) {
        if (showSamples) {
          daf.applyPattern("HHmmss");
          String t = daf.format(now);
          retval += "_" + t;
        } else {
          retval += "_<HHmmss>";
        }
      }
    }
    if (meta.isTransformNrInFilename()) {
      retval += "_" + transformnr;
    }
    if (meta.isPartNrInFilename()) {
      retval += "_" + partnr;
    }
    if (meta.getSplitEvery(variables) > 0) {
      retval += "_" + splitnr;
    }
    // In a Beam context, always add the transform ID and bundle number
    //
    if (beamContext) {
      retval += "_" + transformId + "_" + bundleNr;
    }

    if ("Zip".equals(meta.getFileCompression())) {
      if (ziparchive) {
        retval += ".zip";
      } else {
        if (!Utils.isEmpty(realExtension)) {
          retval += "." + realExtension;
        }
      }
    } else {
      if (!Utils.isEmpty(realExtension)) {
        retval += "." + realExtension;
      }
      if ("GZip".equals(meta.getFileCompression())) {
        retval += ".gz";
      }
    }
    return retval;
  }

  public String[] getFilePaths(IVariables variables, final boolean showSamples) {
    final TransformMeta parentTransformMeta = getParentTransformMeta();
    if (parentTransformMeta != null) {
      final PipelineMeta parentPipelineMeta = parentTransformMeta.getParentPipelineMeta();
      if (parentPipelineMeta != null) {
        return getFiles(variables, showSamples);
      }
    }
    return new String[] {};
  }
}

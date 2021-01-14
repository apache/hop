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

package org.apache.hop.pipeline.transforms.exceloutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
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
    id = "ExcelOutput",
    image = "exceloutput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ExcelOutput",
    description = "i18n::BaseTransform.TypeTooltipDesc.ExcelOutput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/exceloutput.html")
@InjectionSupported(
    localizationPrefix = "ExcelOutput.Injection.",
    groups = {"FIELDS", "CUSTOM", "CONTENT"})
public class ExcelOutputMeta extends BaseTransformMeta
    implements ITransformMeta<ExcelOutput, ExcelOutputData> {
  private static final Class<?> PKG = ExcelOutputMeta.class; // For Translator

  public static final int FONT_NAME_ARIAL = 0;
  public static final int FONT_NAME_COURIER = 1;
  public static final int FONT_NAME_TAHOMA = 2;
  public static final int FONT_NAME_TIMES = 3;

  public static final String[] fontNameCode = {"arial", "courier", "tahoma", "times"};

  public static final String[] fontNameDesc = {
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_name.Arial"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_name.Courier"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_name.Tahoma"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_name.Times")
  };

  public static final int FONT_UNDERLINE_NO = 0;
  public static final int FONT_UNDERLINE_SINGLE = 1;
  public static final int FONT_UNDERLINE_SINGLE_ACCOUNTING = 2;
  public static final int FONT_UNDERLINE_DOUBLE = 3;
  public static final int FONT_UNDERLINE_DOUBLE_ACCOUNTING = 4;

  public static final String[] font_underlineCode = {
    "no", "single", "single_accounting", "double", "double_accounting"
  };

  public static final String[] font_underlineDesc = {
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_underline.No"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_underline.Single"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_underline.SingleAccounting"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_underline.Double"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_underline.DoubleAccounting")
  };

  public static final int FONT_ORIENTATION_HORIZONTAL = 0;
  public static final int FONT_ORIENTATION_MINUS_45 = 1;
  public static final int FONT_ORIENTATION_MINUS_90 = 2;
  public static final int FONT_ORIENTATION_PLUS_45 = 3;
  public static final int FONT_ORIENTATION_PLUS_90 = 4;
  public static final int FONT_ORIENTATION_STACKED = 5;
  public static final int FONT_ORIENTATION_VERTICAL = 6;

  public static final String[] font_orientationCode = {
    "horizontal", "minus_45", "minus_90", "plus_45", "plus_90", "stacked", "vertical"
  };

  public static final String[] font_orientationDesc = {
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Horizontal"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Minus_45"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Minus_90"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Plus_45"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Plus_90"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Stacked"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_orientation.Vertical")
  };

  public static final int FONT_COLOR_NONE = 0;
  public static final int FONT_COLOR_BLACK = 1;
  public static final int FONT_COLOR_WHITE = 2;
  public static final int FONT_COLOR_RED = 3;
  public static final int FONT_COLOR_BRIGHT_GREEN = 4;
  public static final int FONT_COLOR_BLUE = 5;
  public static final int FONT_COLOR_YELLOW = 6;
  public static final int FONT_COLOR_PINK = 7;
  public static final int FONT_COLOR_TURQUOISE = 8;
  public static final int FONT_COLOR_DARK_RED = 9;
  public static final int FONT_COLOR_GREEN = 10;
  public static final int FONT_COLOR_DARK_BLUE = 11;
  public static final int FONT_COLOR_DARK_YELLOW = 12;
  public static final int FONT_COLOR_VIOLET = 13;
  public static final int FONT_COLOR_TEAL = 14;
  public static final int FONT_COLOR_GREY_25pct = 15;
  public static final int FONT_COLOR_GREY_50pct = 16;
  public static final int FONT_COLOR_PERIWINKLEpct = 17;
  public static final int FONT_COLOR_PLUM = 18;
  public static final int FONT_COLOR_IVORY = 19;
  public static final int FONT_COLOR_LIGHT_TURQUOISE = 20;
  public static final int FONT_COLOR_DARK_PURPLE = 21;
  public static final int FONT_COLOR_CORAL = 22;
  public static final int FONT_COLOR_OCEAN_BLUE = 23;
  public static final int FONT_COLOR_ICE_BLUE = 24;
  public static final int FONT_COLOR_TURQOISE = 25;
  public static final int FONT_COLOR_SKY_BLUE = 26;
  public static final int FONT_COLOR_LIGHT_GREEN = 27;
  public static final int FONT_COLOR_VERY_LIGHT_YELLOW = 28;
  public static final int FONT_COLOR_PALE_BLUE = 29;
  public static final int FONT_COLOR_ROSE = 30;
  public static final int FONT_COLOR_LAVENDER = 31;
  public static final int FONT_COLOR_TAN = 32;
  public static final int FONT_COLOR_LIGHT_BLUE = 33;
  public static final int FONT_COLOR_AQUA = 34;
  public static final int FONT_COLOR_LIME = 35;
  public static final int FONT_COLOR_GOLD = 36;
  public static final int FONT_COLOR_LIGHT_ORANGE = 37;
  public static final int FONT_COLOR_ORANGE = 38;
  public static final int FONT_COLOR_BLUE_GREY = 39;
  public static final int FONT_COLOR_GREY_40pct = 40;
  public static final int FONT_COLOR_DARK_TEAL = 41;
  public static final int FONT_COLOR_SEA_GREEN = 42;
  public static final int FONT_COLOR_DARK_GREEN = 43;
  public static final int FONT_COLOR_OLIVE_GREEN = 44;
  public static final int FONT_COLOR_BROWN = 45;
  public static final int FONT_COLOR_GREY_80pct = 46;

  public static final String[] fontColorCode = {
    "none",
    "black",
    "white",
    "red",
    "bright_green",
    "blue",
    "yellow",
    "pink",
    "turquoise",
    "dark_red",
    "green",
    "dark_blue",
    "dark_yellow",
    "violet",
    "teal",
    "grey_25pct",
    "grey_50pct",
    "periwinklepct",
    "plum",
    "ivory",
    "light_turquoise",
    "dark_purple",
    "coral",
    "ocean_blue",
    "ice_blue",
    "turqoise",
    "sky_blue",
    "light_green",
    "very_light_yellow",
    "pale_blue",
    "rose",
    "lavender",
    "tan",
    "light_blue",
    "aqua",
    "lime",
    "gold",
    "light_orange",
    "orange",
    "blue_grey",
    "grey_40pct",
    "dark_teal",
    "sea_green",
    "dark_green",
    "olive_green",
    "brown",
    "grey_80pct"
  };

  public static final String[] fontColorDesc = {
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.None"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.BLACK"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.WHITE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.RED"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.BRIGHT_GREEN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.YELLOW"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.PINK"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.TURQUOISE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.DARK_RED"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.GREEN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.DARK_BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.DARK_YELLOW"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.VIOLET"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.TEAL"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.GREY_25pct"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.GREY_50pct"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.PERIWINKLEpct"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.PLUM"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.IVORY"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.LIGHT_TURQUOISE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.DARK_PURPLE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.CORAL"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.OCEAN_BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.ICE_BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.TURQOISE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.SKY_BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.LIGHT_GREEN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.VERY_LIGHT_YELLOW"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.PALE_BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.ROSE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.LAVENDER"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.TAN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.LIGHT_BLUE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.AQUA"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.LIME"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.GOLD"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.LIGHT_ORANGE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.ORANGE"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.BLUE_GREY"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.GREY_40pct"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.DARK_TEAL"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.SEA_GREEN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.DARK_GREEN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.OLIVE_GREEN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.BROWN"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_color.GREY_80pct")
  };

  public static final int FONT_ALIGNMENT_LEFT = 0;
  public static final int FONT_ALIGNMENT_RIGHT = 1;
  public static final int FONT_ALIGNMENT_CENTER = 2;
  public static final int FONT_ALIGNMENT_FILL = 3;
  public static final int FONT_ALIGNMENT_GENERAL = 4;
  public static final int FONT_ALIGNMENT_JUSTIFY = 5;

  public static final String[] font_alignmentCode = {
    "left", "right", "center", "fill", "general", "justify"
  };

  public static final String[] font_alignmentDesc = {
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_alignment.Left"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_alignment.Right"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_alignment.Center"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_alignment.Fill"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_alignment.General"),
    BaseMessages.getString(PKG, "ExcelOutputMeta.font_alignment.Justify")
  };

  public static final int DEFAULT_FONT_SIZE = 10;
  public static final int DEFAULT_ROW_HEIGHT = 255;
  public static final int DEFAULT_ROW_WIDTH = 255;

  private int header_fontName;

  @Injection(name = "HEADER_FONT_SIZE", group = "CUSTOM")
  private String header_font_size;

  @Injection(name = "HEADER_FONT_BOLD", group = "CUSTOM")
  private boolean header_font_bold;

  @Injection(name = "HEADER_FONT_ITALIC", group = "CUSTOM")
  private boolean header_font_italic;

  private int header_font_underline;
  private int header_font_orientation;

  @Injection(name = "HEADER_FONT_COLOR", group = "CUSTOM")
  private int header_fontColor;

  @Injection(name = "HEADER_BACKGROUND_COLOR", group = "CUSTOM")
  private int header_backgroundColor;

  @Injection(name = "HEADER_ROW_HEIGHT", group = "CUSTOM")
  private String headerRow_height;

  private int header_alignment;

  @Injection(name = "HEADER_IMAGE", group = "CUSTOM")
  private String header_image;
  // Row font
  private int row_fontName;

  @Injection(name = "ROW_FONT_SIZE", group = "CUSTOM")
  private String row_font_size;

  @Injection(name = "ROW_FONT_COLOR", group = "CUSTOM")
  private int row_fontColor;

  @Injection(name = "ROW_BACKGROUND_COLOR", group = "CUSTOM")
  private int row_backgroundColor;

  /** The base name of the output file */
  @Injection(name = "FILENAME")
  private String fileName;

  /** The file extention in case of a generated filename */
  @Injection(name = "EXTENSION")
  private String extension;

  /** The password to protect the sheet */
  @Injection(name = "PASSWORD", group = "CONTENT")
  private String password;

  /** Add a header at the top of the file? */
  @Injection(name = "HEADER_ENABLED", group = "CONTENT")
  private boolean headerEnabled;

  /** Add a footer at the bottom of the file? */
  @Injection(name = "FOOTER_ENABLED", group = "CONTENT")
  private boolean footerEnabled;

  /**
   * if this value is larger then 0, the text file is split up into parts of this number of lines
   */
  @Injection(name = "SPLIT_EVERY", group = "CONTENT")
  private int splitEvery;

  /** Flag: add the transformnr in the filename */
  @Injection(name = "TRANSFORM_NR_IN_FILENAME")
  private boolean transformNrInFilename;

  /** Flag: add the date in the filename */
  @Injection(name = "DATE_IN_FILENAME")
  private boolean dateInFilename;

  /** Flag: add the filenames to result filenames */
  @Injection(name = "FILENAME_TO_RESULT")
  private boolean addToResultFilenames;

  /** Flag: protect the sheet */
  @Injection(name = "PROTECT", group = "CONTENT")
  private boolean protectsheet;

  /** Flag: add the time in the filename */
  @Injection(name = "TIME_IN_FILENAME")
  private boolean timeInFilename;

  /** Flag: use a template */
  @Injection(name = "TEMPLATE", group = "CONTENT")
  private boolean templateEnabled;

  /** the excel template */
  @Injection(name = "TEMPLATE_FILENAME", group = "CONTENT")
  private String templateFileName;

  /** Flag: append when template */
  @Injection(name = "TEMPLATE_APPEND", group = "CONTENT")
  private boolean templateAppend;

  /** the excel sheet name */
  @Injection(name = "SHEET_NAME", group = "CONTENT")
  private String sheetname;

  /** Flag : use temporary files while writing? */
  @Injection(name = "USE_TEMPFILES", group = "CONTENT")
  private boolean usetempfiles;

  /** Temporary directory */
  @Injection(name = "TEMPDIR", group = "CONTENT")
  private String tempdirectory;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @InjectionDeep private ExcelField[] outputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection(name = "ENCODING", group = "CONTENT")
  private String encoding;

  /** Calculated value ... */
  @Injection(name = "NEWLINE", group = "CONTENT")
  private String newline;

  /** Flag : append workbook? */
  @Injection(name = "APPEND", group = "CONTENT")
  private boolean append;

  /** Flag : Do not open new file when pipeline start */
  @Injection(name = "DONT_OPEN_NEW_FILE")
  private boolean doNotOpenNewFileInit;

  /** Flag: create parent folder when necessary */
  @Injection(name = "CREATE_PARENT_FOLDER")
  private boolean createparentfolder;

  @Injection(name = "DATE_FORMAT_SPECIFIED")
  private boolean specifyFormat;

  @Injection(name = "DATE_FORMAT")
  private String dateTimeFormat;

  /** Flag : auto size columns? */
  @Injection(name = "AUTOSIZE_COLUMNS", group = "CONTENT")
  private boolean autoSizeColumns;

  /** Flag : write null field values as blank Excel cells? */
  @Injection(name = "NULL_AS_BLANK", group = "CONTENT")
  private boolean nullIsBlank;

  public ExcelOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the createparentfolder. */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /** @param createparentfolder The createparentfolder to set. */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
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

  /** @return Returns the autosizecolumns. */
  public boolean isAutoSizeColumns() {
    return autoSizeColumns;
  }

  /** @param autosizecolumns The autosizecolumns to set. */
  public void setAutoSizeColumns(boolean autosizecolumns) {
    this.autoSizeColumns = autosizecolumns;
  }

  /**
   * @return Returns the autosizecolums.
   * @deprecated due to typo
   */
  @Deprecated
  public boolean isAutoSizeColums() {
    return autoSizeColumns;
  }

  /**
   * @param autosizecolums The autosizecolums to set.
   * @deprecated due to typo
   */
  @Deprecated
  public void setAutoSizeColums(boolean autosizecolums) {
    this.autoSizeColumns = autosizecolums;
  }

  public void setTempDirectory(String directory) {
    this.tempdirectory = directory;
  }

  public String getTempDirectory() {
    return tempdirectory;
  }

  /** @return Returns whether or not null values are written as blank cells. */
  public boolean isNullBlank() {
    return nullIsBlank;
  }

  /**
   * @param nullIsBlank The boolean indicating whether or not to write null values as blank cells
   */
  public void setNullIsBlank(boolean nullIsBlank) {
    this.nullIsBlank = nullIsBlank;
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

  /** @return Returns the newline. */
  public String getNewline() {
    return newline;
  }

  /** @param newline The newline to set. */
  public void setNewline(String newline) {
    this.newline = newline;
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

  /** @return Returns the usetempfile. */
  public boolean isUseTempFiles() {
    return usetempfiles;
  }

  /** @param usetempfiles The usetempfiles to set. */
  public void setUseTempFiles(boolean usetempfiles) {
    this.usetempfiles = usetempfiles;
  }

  /** @return Returns the outputFields. */
  public ExcelField[] getOutputFields() {
    return outputFields;
  }

  /** @param outputFields The outputFields to set. */
  public void setOutputFields(ExcelField[] outputFields) {
    this.outputFields = outputFields;
  }

  /**
   * @return The desired encoding of output file, null or empty if the default system encoding needs
   *     to be used.
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding The desired encoding of output file, null or empty if the default system
   *     encoding needs to be used.
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @return Returns the template. */
  public boolean isTemplateEnabled() {
    return templateEnabled;
  }

  /** @param template The template to set. */
  public void setTemplateEnabled(boolean template) {
    this.templateEnabled = template;
  }

  /** @return Returns the templateAppend. */
  public boolean isTemplateAppend() {
    return templateAppend;
  }

  /** @param templateAppend The templateAppend to set. */
  public void setTemplateAppend(boolean templateAppend) {
    this.templateAppend = templateAppend;
  }

  /** @return Returns the templateFileName. */
  public String getTemplateFileName() {
    return templateFileName;
  }

  /** @param templateFileName The templateFileName to set. */
  public void setTemplateFileName(String templateFileName) {
    this.templateFileName = templateFileName;
  }

  /** @return Returns the "do not open new file at init" flag. */
  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  /** @param doNotOpenNewFileInit The "do not open new file at init" flag to set. */
  public void setDoNotOpenNewFileInit(boolean doNotOpenNewFileInit) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }

  /** @return Returns the append. */
  public boolean isAppend() {
    return append;
  }

  /** @param append The append to set. */
  public void setAppend(boolean append) {
    this.append = append;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public void allocate(int nrFields) {
    outputFields = new ExcelField[nrFields];
  }

  @Override
  public Object clone() {
    ExcelOutputMeta retval = (ExcelOutputMeta) super.clone();
    int nrFields = outputFields.length;

    retval.allocate(nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.outputFields[i] = (ExcelField) outputFields[i].clone();
    }

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      headerEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      footerEnabled = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "footer"));
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      append = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "append"));
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
      createparentfolder =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "create_parent_folder"));

      transformNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "split"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_time"));
      specifyFormat =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "SpecifyFormat"));
      dateTimeFormat = XmlHandler.getTagValue(transformNode, "file", "date_time_format");
      usetempfiles =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "usetempfiles"));

      tempdirectory = XmlHandler.getTagValue(transformNode, "file", "tempdirectory");
      autoSizeColumns =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "autosizecolums"));
      nullIsBlank =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "nullisblank"));
      protectsheet =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "protect_sheet"));
      password =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "file", "password"));
      splitEvery = Const.toInt(XmlHandler.getTagValue(transformNode, "file", "splitevery"), 0);

      templateEnabled =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "template", "enabled"));
      templateAppend =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "template", "append"));
      templateFileName = XmlHandler.getTagValue(transformNode, "template", "filename");
      sheetname = XmlHandler.getTagValue(transformNode, "file", "sheetname");
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFields);

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        outputFields[i] = new ExcelField();
        outputFields[i].setName(XmlHandler.getTagValue(fnode, "name"));
        outputFields[i].setType(XmlHandler.getTagValue(fnode, "type"));
        outputFields[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
      }
      Node customnode = XmlHandler.getSubNode(transformNode, "custom");
      header_fontName =
          getFontNameByCode(Const.NVL(XmlHandler.getTagValue(customnode, "header_font_name"), ""));
      header_font_size =
          Const.NVL(XmlHandler.getTagValue(customnode, "header_font_size"), "" + DEFAULT_FONT_SIZE);
      header_font_bold =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(customnode, "header_font_bold"));
      header_font_italic =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(customnode, "header_font_italic"));
      header_font_underline =
          getFontUnderlineByCode(
              Const.NVL(XmlHandler.getTagValue(customnode, "header_font_underline"), ""));
      header_font_orientation =
          getFontOrientationByCode(
              Const.NVL(XmlHandler.getTagValue(customnode, "header_font_orientation"), ""));
      header_fontColor =
          getFontColorByCode(
              Const.NVL(
                  XmlHandler.getTagValue(customnode, "header_font_color"), "" + FONT_COLOR_BLACK));
      header_backgroundColor =
          getFontColorByCode(
              Const.NVL(
                  XmlHandler.getTagValue(customnode, "header_background_color"),
                  "" + FONT_COLOR_NONE));
      headerRow_height = XmlHandler.getTagValue(customnode, "header_row_height");
      header_alignment =
          getFontAlignmentByCode(
              Const.NVL(XmlHandler.getTagValue(customnode, "header_alignment"), ""));
      header_image = XmlHandler.getTagValue(customnode, "header_image");
      // Row font
      row_fontName =
          getFontNameByCode(Const.NVL(XmlHandler.getTagValue(customnode, "row_font_name"), ""));
      row_font_size =
          Const.NVL(XmlHandler.getTagValue(customnode, "row_font_size"), "" + DEFAULT_FONT_SIZE);
      row_fontColor =
          getFontColorByCode(
              Const.NVL(
                  XmlHandler.getTagValue(customnode, "row_font_color"), "" + FONT_COLOR_BLACK));
      row_backgroundColor =
          getFontColorByCode(
              Const.NVL(
                  XmlHandler.getTagValue(customnode, "row_background_color"),
                  "" + FONT_COLOR_NONE));

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
    usetempfiles = false;
    tempdirectory = null;
    header_fontName = FONT_NAME_ARIAL;
    header_font_size = "" + DEFAULT_FONT_SIZE;
    header_font_bold = false;
    header_font_italic = false;
    header_font_underline = FONT_UNDERLINE_NO;
    header_font_orientation = FONT_ORIENTATION_HORIZONTAL;
    header_fontColor = FONT_COLOR_BLACK;
    header_backgroundColor = FONT_COLOR_NONE;
    headerRow_height = "" + DEFAULT_ROW_HEIGHT;
    header_alignment = FONT_ALIGNMENT_LEFT;
    header_image = null;

    row_fontName = FONT_NAME_ARIAL;
    row_font_size = "" + DEFAULT_FONT_SIZE;
    row_fontColor = FONT_COLOR_BLACK;
    row_backgroundColor = FONT_COLOR_NONE;

    autoSizeColumns = false;
    headerEnabled = true;
    footerEnabled = false;
    fileName = "file";
    extension = "xls";
    doNotOpenNewFileInit = false;
    createparentfolder = false;
    transformNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    dateTimeFormat = null;
    specifyFormat = false;
    addToResultFilenames = true;
    protectsheet = false;
    splitEvery = 0;
    templateEnabled = false;
    templateAppend = false;
    templateFileName = "template.xls";
    sheetname = "Sheet1";
    append = false;
    nullIsBlank = false;
    int i, nrFields = 0;
    allocate(nrFields);

    for (i = 0; i < nrFields; i++) {
      outputFields[i] = new ExcelField();

      outputFields[i].setName("field" + i);
      outputFields[i].setType("Number");
      outputFields[i].setFormat(" 0,000,000.00;-0,000,000.00");
    }
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
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    " + XmlHandler.addTagValue("append", append));
    retval.append("    " + XmlHandler.addTagValue("add_to_result_filenames", addToResultFilenames));

    retval.append("    <file>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", fileName));
    retval.append("      ").append(XmlHandler.addTagValue("extention", extension));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("do_not_open_newfile_init", doNotOpenNewFileInit));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("create_parent_folder", createparentfolder));
    retval.append("      ").append(XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("add_time", timeInFilename));
    retval.append("      ").append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    retval.append("      ").append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    retval.append("      ").append(XmlHandler.addTagValue("sheetname", sheetname));
    retval.append("      ").append(XmlHandler.addTagValue("autosizecolums", autoSizeColumns));
    retval.append("      ").append(XmlHandler.addTagValue("nullisblank", nullIsBlank));
    retval.append("      ").append(XmlHandler.addTagValue("protect_sheet", protectsheet));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    retval.append("      ").append(XmlHandler.addTagValue("splitevery", splitEvery));
    retval.append("      ").append(XmlHandler.addTagValue("usetempfiles", usetempfiles));
    retval.append("      ").append(XmlHandler.addTagValue("tempdirectory", tempdirectory));
    retval.append("      </file>").append(Const.CR);

    retval.append("    <template>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("enabled", templateEnabled));
    retval.append("      ").append(XmlHandler.addTagValue("append", templateAppend));
    retval.append("      ").append(XmlHandler.addTagValue("filename", templateFileName));
    retval.append("    </template>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < outputFields.length; i++) {
      ExcelField field = outputFields[i];

      if (field.getName() != null && field.getName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue("name", field.getName()));
        retval.append("        ").append(XmlHandler.addTagValue("type", field.getTypeDesc()));
        retval.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

    retval.append("    <custom>" + Const.CR);
    retval.append(
        "    " + XmlHandler.addTagValue("header_font_name", getFontNameCode(header_fontName)));
    retval.append("    " + XmlHandler.addTagValue("header_font_size", header_font_size));
    retval.append("    " + XmlHandler.addTagValue("header_font_bold", header_font_bold));
    retval.append("    " + XmlHandler.addTagValue("header_font_italic", header_font_italic));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "header_font_underline", getFontUnderlineCode(header_font_underline)));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "header_font_orientation", getFontOrientationCode(header_font_orientation)));
    retval.append(
        "    " + XmlHandler.addTagValue("header_font_color", getFontColorCode(header_fontColor)));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "header_background_color", getFontColorCode(header_backgroundColor)));
    retval.append("    " + XmlHandler.addTagValue("header_row_height", headerRow_height));
    retval.append(
        "    "
            + XmlHandler.addTagValue("header_alignment", getFontAlignmentCode(header_alignment)));
    retval.append("    " + XmlHandler.addTagValue("header_image", header_image));
    // row font
    retval.append("    " + XmlHandler.addTagValue("row_font_name", getFontNameCode(row_fontName)));
    retval.append("    " + XmlHandler.addTagValue("row_font_size", row_font_size));
    retval.append(
        "    " + XmlHandler.addTagValue("row_font_color", getFontColorCode(row_fontColor)));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "row_background_color", getFontColorCode(row_backgroundColor)));
    retval.append("      </custom>" + Const.CR);
    return retval.toString();
  }

  private static String getFontNameCode(int i) {
    if (i < 0 || i >= fontNameCode.length) {
      return fontNameCode[0];
    }
    return fontNameCode[i];
  }

  private static String getFontUnderlineCode(int i) {
    if (i < 0 || i >= font_underlineCode.length) {
      return font_underlineCode[0];
    }
    return font_underlineCode[i];
  }

  private static String getFontAlignmentCode(int i) {
    if (i < 0 || i >= font_alignmentCode.length) {
      return font_alignmentCode[0];
    }
    return font_alignmentCode[i];
  }

  private static String getFontOrientationCode(int i) {
    if (i < 0 || i >= font_orientationCode.length) {
      return font_orientationCode[0];
    }
    return font_orientationCode[i];
  }

  private static String getFontColorCode(int i) {
    if (i < 0 || i >= fontColorCode.length) {
      return fontColorCode[0];
    }
    return fontColorCode[i];
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
                  PKG, "ExcelOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
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
            BaseMessages.getString(PKG, "ExcelOutputMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExcelOutputMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExcelOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExcelOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "ExcelOutputMeta.CheckResult.FilesNotChecked"),
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

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public ExcelOutput createTransform(
      TransformMeta transformMeta,
      ExcelOutputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExcelOutput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ExcelOutputData getTransformData() {
    return new ExcelOutputData();
  }

  public static String getFontNameDesc(int i) {
    if (i < 0 || i >= fontNameDesc.length) {
      return fontNameDesc[0];
    }
    return fontNameDesc[i];
  }

  public static String getFontUnderlineDesc(int i) {
    if (i < 0 || i >= font_underlineDesc.length) {
      return font_underlineDesc[0];
    }
    return font_underlineDesc[i];
  }

  public static String getFontOrientationDesc(int i) {
    if (i < 0 || i >= font_orientationDesc.length) {
      return font_orientationDesc[0];
    }
    return font_orientationDesc[i];
  }

  public static String getFontColorDesc(int i) {
    if (i < 0 || i >= fontColorDesc.length) {
      return fontColorDesc[0];
    }
    return fontColorDesc[i];
  }

  public static String getFontAlignmentDesc(int i) {
    if (i < 0 || i >= font_alignmentDesc.length) {
      return font_alignmentDesc[0];
    }
    return font_alignmentDesc[i];
  }

  public int getHeaderFontName() {
    return header_fontName;
  }

  public int getRowFontName() {
    return row_fontName;
  }

  public int getHeaderFontUnderline() {
    return header_font_underline;
  }

  public int getHeaderFontOrientation() {
    return header_font_orientation;
  }

  public int getHeaderAlignment() {
    return header_alignment;
  }

  public int getHeaderFontColor() {
    return header_fontColor;
  }

  public int getRowFontColor() {
    return row_fontColor;
  }

  public int getHeaderBackGroundColor() {
    return header_backgroundColor;
  }

  public int getRowBackGroundColor() {
    return row_backgroundColor;
  }

  public static int getFontNameByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < fontNameDesc.length; i++) {
      if (fontNameDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getFontNameByCode(tt);
  }

  private static int getFontNameByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < fontNameCode.length; i++) {
      if (fontNameCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getFontUnderlineByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < font_underlineDesc.length; i++) {
      if (font_underlineDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getFontUnderlineByCode(tt);
  }

  public static int getFontOrientationByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < font_orientationDesc.length; i++) {
      if (font_orientationDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getFontOrientationByCode(tt);
  }

  private static int getFontUnderlineByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < font_underlineCode.length; i++) {
      if (font_underlineCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getFontOrientationByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < font_orientationCode.length; i++) {
      if (font_orientationCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static int getFontColorByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < fontColorDesc.length; i++) {
      if (fontColorDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getFontColorByCode(tt);
  }

  public static int getFontAlignmentByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < font_alignmentDesc.length; i++) {
      if (font_alignmentDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getFontAlignmentByCode(tt);
  }

  private static int getFontAlignmentByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < font_alignmentCode.length; i++) {
      if (font_alignmentCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  private static int getFontColorByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < fontColorCode.length; i++) {
      if (fontColorCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public void setHeaderFontName(int fontname) {
    this.header_fontName = fontname;
  }

  @Injection(name = "HEADER_FONT_NAME", group = "CUSTOM")
  public void setHeaderFontName(String fontname) {
    this.header_fontName = getFontNameByCode(fontname);
  }

  public void setRowFontName(int fontname) {
    this.row_fontName = fontname;
  }

  @Injection(name = "ROW_FONT_NAME", group = "CUSTOM")
  public void setRowFontName(String fontname) {
    this.row_fontName = getFontNameByCode(fontname);
  }

  public void setHeaderFontUnderline(int fontunderline) {
    this.header_font_underline = fontunderline;
  }

  @Injection(name = "HEADER_FONT_UNDERLINE", group = "CUSTOM")
  public void setHeaderFontUnderline(String fontunderline) {
    this.header_font_underline = getFontUnderlineByCode(fontunderline);
  }

  public void setHeaderFontOrientation(int fontorientation) {
    this.header_font_orientation = fontorientation;
  }

  @Injection(name = "HEADER_FONT_ORIENTATION", group = "CUSTOM")
  public void setHeaderFontOrientation(String fontorientation) {
    this.header_font_orientation = getFontOrientationByCode(fontorientation);
  }

  public void setHeaderFontColor(int fontcolor) {
    this.header_fontColor = fontcolor;
  }

  public void setRowFontColor(int fontcolor) {
    this.row_fontColor = fontcolor;
  }

  public void setHeaderBackGroundColor(int fontcolor) {
    this.header_backgroundColor = fontcolor;
  }

  public void setRowBackGroundColor(int fontcolor) {
    this.row_backgroundColor = fontcolor;
  }

  public void setHeaderAlignment(int alignment) {
    this.header_alignment = alignment;
  }

  @Injection(name = "HEADER_ALIGNMENT", group = "CUSTOM")
  public void setHeaderAlignment(String alignment) {
    this.header_alignment = getFontAlignmentByCode(alignment);
  }

  public void setHeaderFontSize(String fontsize) {
    this.header_font_size = fontsize;
  }

  public void setRowFontSize(String fontsize) {
    this.row_font_size = fontsize;
  }

  public String getHeaderFontSize() {
    return this.header_font_size;
  }

  public String getRowFontSize() {
    return this.row_font_size;
  }

  public void setHeaderImage(String image) {
    this.header_image = image;
  }

  public String getHeaderImage() {
    return this.header_image;
  }

  public void setHeaderRowHeight(String height) {
    this.headerRow_height = height;
  }

  public String getHeaderRowHeight() {
    return this.headerRow_height;
  }

  public boolean isHeaderFontBold() {
    return this.header_font_bold;
  }

  public void setHeaderFontItalic(boolean fontitalic) {
    this.header_font_italic = fontitalic;
  }

  public boolean isHeaderFontItalic() {
    return this.header_font_italic;
  }

  public void setHeaderFontBold(boolean font_bold) {
    this.header_font_bold = font_bold;
  }
}

// CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.pipeline.transforms.fileinput;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IInputFileMeta;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
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
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** @deprecated replaced by implementation in the ...transforms.fileinput.text package */
@Deprecated
@Transform(
    id = "TextFileInput",
    image = "textfileinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.TextFileInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.TextFileInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/textfileinput.html")
public class TextFileInputMeta extends BaseTransformMeta
    implements ITransformMeta<TextFileInput, TextFileInputData>,
        IInputFileMeta<TextFileInput, TextFileInputData> {

  private static final Class<?> PKG = TextFileInputMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  private static final String NO = "N";

  private static final String YES = "Y";

  private static final String STRING_BASE64_PREFIX = "Base64: ";

  public static final int FILE_FORMAT_DOS = 0;
  public static final int FILE_FORMAT_UNIX = 1;
  public static final int FILE_FORMAT_MIXED = 2;

  public static final int FILE_TYPE_CSV = 0;
  public static final int FILE_TYPE_FIXED = 1;

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  private String[] excludeFileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

  /** Type of file: CSV or fixed */
  private String fileType;

  /** String used to separated field (;) */
  private String separator;

  /** String used to enclose separated fields (") */
  private String enclosure;

  /** Escape character used to escape the enclosure String (\) */
  private String escapeCharacter;

  /** Switch to allow breaks (CR/LF) in Enclosures */
  private boolean breakInEnclosureAllowed;

  /** Flag indicating that the file contains one header line that should be skipped. */
  private boolean header;

  /** The number of header lines, defaults to 1 */
  private int nrHeaderLines;

  /** Flag indicating that the file contains one footer line that should be skipped. */
  private boolean footer;

  /** The number of footer lines, defaults to 1 */
  private int nrFooterLines;

  /** Flag indicating that a single line is wrapped onto one or more lines in the text file. */
  private boolean lineWrapped;

  /** The number of times the line wrapped */
  private int nrWraps;

  /** Flag indicating that the text-file has a paged layout. */
  private boolean layoutPaged;

  /** The number of lines in the document header */
  private int nrLinesDocHeader;

  /** The number of lines to read per page */
  private int nrLinesPerPage;

  /** Type of compression being used */
  private String fileCompression;

  /** Flag indicating that we should skip all empty lines */
  private boolean noEmptyLines;

  /** Flag indicating that we should include the filename in the output */
  private boolean includeFilename;

  /** The name of the field in the output containing the filename */
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** Flag indicating row number is per file */
  private boolean rowNumberByFile;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  /** The file format: DOS or UNIX or mixed */
  private String fileFormat;

  /** The maximum number or lines to read */
  private long rowLimit;

  /** The fields to import... */
  private TextFileInputField[] inputFields;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  private String[] includeSubFolders;

  /** The filters to use... */
  private TextFileFilter[] filter;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** Ignore error : turn into warnings */
  private boolean errorIgnored;

  /** The name of the field that will contain the number of errors in the row */
  private String errorCountField;

  /**
   * The name of the field that will contain the names of the fields that generated errors,
   * separated by ,
   */
  private String errorFieldsField;

  /** The name of the field that will contain the error texts, separated by CR */
  private String errorTextField;

  /** The directory that will contain warning files */
  private String warningFilesDestinationDirectory;

  /** The extension of warning files */
  private String warningFilesExtension;

  /** The directory that will contain error files */
  private String errorFilesDestinationDirectory;

  /** The extension of error files */
  private String errorFilesExtension;

  /** The directory that will contain line number files */
  private String lineNumberFilesDestinationDirectory;

  /** The extension of line number files */
  private String lineNumberFilesExtension;

  /** Indicate whether or not we want to date fields strictly according to the format or lenient */
  private boolean dateFormatLenient;

  /** Specifies the Locale of the Date format, null means the default */
  private Locale dateFormatLocale;

  /** If error line are skipped, you can replay without introducing doubles. */
  private boolean errorLineSkipped;

  /** Are we accepting filenames in input rows? */
  private boolean acceptingFilenames;

  /** If receiving input rows, should we pass through existing fields? */
  private boolean passingThruFields;

  /** The field in which the filename is placed */
  private String acceptingField;

  /** The transformName to accept filenames from */
  private String acceptingTransformName;

  /** The transform to accept filenames from */
  private TransformMeta acceptingTransform;

  /** The add filenames to result filenames flag */
  private boolean isaddresult;

  /** Additional fields */
  private String shortFileFieldName;

  private String pathFieldName;
  private String hiddenFieldName;
  private String lastModificationTimeFieldName;
  private String uriNameFieldName;
  private String rootUriNameFieldName;
  private String extensionFieldName;
  private String sizeFieldName;

  private boolean skipBadFiles;
  private String fileErrorField;
  private String fileErrorMessageField;

  /** @return Returns the shortFileFieldName. */
  public String getShortFileNameField() {
    return shortFileFieldName;
  }

  /** @param field The shortFileFieldName to set. */
  public void setShortFileNameField(String field) {
    shortFileFieldName = field;
  }

  /** @return Returns the pathFieldName. */
  public String getPathField() {
    return pathFieldName;
  }

  /** @param field The pathFieldName to set. */
  public void setPathField(String field) {
    this.pathFieldName = field;
  }

  /** @return Returns the hiddenFieldName. */
  public String isHiddenField() {
    return hiddenFieldName;
  }

  /** @param field The hiddenFieldName to set. */
  public void setIsHiddenField(String field) {
    hiddenFieldName = field;
  }

  /** @return Returns the lastModificationTimeFieldName. */
  public String getLastModificationDateField() {
    return lastModificationTimeFieldName;
  }

  /** @param field The lastModificationTimeFieldName to set. */
  public void setLastModificationDateField(String field) {
    lastModificationTimeFieldName = field;
  }

  /** @return Returns the uriNameFieldName. */
  public String getUriField() {
    return uriNameFieldName;
  }

  /** @param field The uriNameFieldName to set. */
  public void setUriField(String field) {
    uriNameFieldName = field;
  }

  /** @return Returns the uriNameFieldName. */
  public String getRootUriField() {
    return rootUriNameFieldName;
  }

  /** @param field The rootUriNameFieldName to set. */
  public void setRootUriField(String field) {
    rootUriNameFieldName = field;
  }

  /** @return Returns the extensionFieldName. */
  public String getExtensionField() {
    return extensionFieldName;
  }

  /** @param field The extensionFieldName to set. */
  public void setExtensionField(String field) {
    extensionFieldName = field;
  }

  /** @return Returns the sizeFieldName. */
  public String getSizeField() {
    return sizeFieldName;
  }

  /** @param field The sizeFieldName to set. */
  public void setSizeField(String field) {
    sizeFieldName = field;
  }

  /** @return If should continue processing after failing to open a file */
  public boolean isSkipBadFiles() {
    return skipBadFiles;
  }

  /** @param value If should continue processing after failing to open a file */
  public void setSkipBadFiles(boolean value) {
    skipBadFiles = value;
  }

  public String getFileErrorField() {
    return fileErrorField;
  }

  public void setFileErrorField(String field) {
    fileErrorField = field;
  }

  public String getFileErrorMessageField() {
    return fileErrorMessageField;
  }

  public void setFileErrorMessageField(String field) {
    fileErrorMessageField = field;
  }

  /** @return Returns the encoding. */
  public String getEncoding() {
    return encoding;
  }

  /** @param encoding The encoding to set. */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public TextFileInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the input fields. */
  @Override
  public TextFileInputField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields The input fields to set. */
  public void setInputFields(TextFileInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  /** @return Returns the enclosure. */
  @Override
  public String getEnclosure() {
    return enclosure;
  }

  /** @param enclosure The enclosure to set. */
  public void setEnclosure(String enclosure) {
    this.enclosure = enclosure;
  }

  /** @return Returns the breakInEnclosureAllowed. */
  public boolean isBreakInEnclosureAllowed() {
    return breakInEnclosureAllowed;
  }

  /** @param breakInEnclosureAllowed The breakInEnclosureAllowed to set. */
  public void setBreakInEnclosureAllowed(boolean breakInEnclosureAllowed) {
    this.breakInEnclosureAllowed = breakInEnclosureAllowed;
  }

  /** @return Returns the excludeFileMask. */
  public String[] getExludeFileMask() {
    return excludeFileMask;
  }

  /** @param excludeFileMask The excludeFileMask to set. */
  public void setExcludeFileMask(String[] excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  /** @return Returns the fileFormat. */
  public String getFileFormat() {
    return fileFormat;
  }

  /** @param fileFormat The fileFormat to set. */
  public void setFileFormat(String fileFormat) {
    this.fileFormat = fileFormat;
  }

  /** @return Returns the fileMask. */
  public String[] getFileMask() {
    return fileMask;
  }

  /** @return Returns the fileRequired. */
  public String[] getFileRequired() {
    return fileRequired;
  }

  /** @param fileMask The fileMask to set. */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  /** @param fileRequired The fileRequired to set. */
  public void setFileRequired(String[] fileRequired) {
    for (int i = 0; i < fileRequired.length; i++) {
      this.fileRequired[i] = getRequiredFilesCode(fileRequired[i]);
    }
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    for (int i = 0; i < includeSubFoldersin.length; i++) {
      this.includeSubFolders[i] = getRequiredFilesCode(includeSubFoldersin[i]);
    }
  }

  public String getRequiredFilesCode(String tt) {
    if (tt == null) {
      return RequiredFilesCode[0];
    }
    if (tt.equals(RequiredFilesDesc[1])) {
      return RequiredFilesCode[1];
    } else {
      return RequiredFilesCode[0];
    }
  }

  /** @return Returns the fileName. */
  public String[] getFileName() {
    return fileName;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String[] fileName) {
    this.fileName = fileName;
  }

  /** @return Returns the filenameField. */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set. */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /** @return Returns the fileType. */
  @Override
  public String getFileType() {
    return fileType;
  }

  /** @param fileType The fileType to set. */
  public void setFileType(String fileType) {
    this.fileType = fileType;
  }

  /** @return The array of filters for the metadata of this text file input transform. */
  public TextFileFilter[] getFilter() {
    return filter;
  }

  /** @param filter The array of filters to use */
  public void setFilter(TextFileFilter[] filter) {
    this.filter = filter;
  }

  /** @return Returns the footer. */
  public boolean hasFooter() {
    return footer;
  }

  /** @param footer The footer to set. */
  public void setFooter(boolean footer) {
    this.footer = footer;
  }

  /** @return Returns the header. */
  @Override
  public boolean hasHeader() {
    return header;
  }

  /** @param header The header to set. */
  public void setHeader(boolean header) {
    this.header = header;
  }

  /** @return Returns the includeFilename. */
  @Override
  public boolean includeFilename() {
    return includeFilename;
  }

  /** @param includeFilename The includeFilename to set. */
  public void setIncludeFilename(boolean includeFilename) {
    this.includeFilename = includeFilename;
  }

  /** @return Returns the includeRowNumber. */
  @Override
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * true if row number reset for each file
   *
   * @return rowNumberByFile
   */
  public boolean isRowNumberByFile() {
    return rowNumberByFile;
  }

  /** @param rowNumberByFile True if row number field is reset for each file */
  public void setRowNumberByFile(boolean rowNumberByFile) {
    this.rowNumberByFile = rowNumberByFile;
  }

  /** @return Returns the noEmptyLines. */
  public boolean noEmptyLines() {
    return noEmptyLines;
  }

  /** @param noEmptyLines The noEmptyLines to set. */
  public void setNoEmptyLines(boolean noEmptyLines) {
    this.noEmptyLines = noEmptyLines;
  }

  /** @return Returns the rowLimit. */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /** @return Returns the rowNumberField. */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /** @return Returns the separator. */
  @Override
  public String getSeparator() {
    return separator;
  }

  /** @param separator The separator to set. */
  public void setSeparator(String separator) {
    this.separator = separator;
  }

  /** @return Returns the type of compression used */
  public String getFileCompression() {
    return fileCompression;
  }

  /** @param fileCompression Sets the compression type */
  public void setFileCompression(String fileCompression) {
    this.fileCompression = fileCompression;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      acceptingFilenames =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "accept_filenames"));
      passingThruFields =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "passing_through_fields"));
      acceptingField = XmlHandler.getTagValue(transformNode, "accept_field");
      acceptingTransformName = XmlHandler.getTagValue(transformNode, "accept_transform_name");

      separator = XmlHandler.getTagValue(transformNode, "separator");
      enclosure = XmlHandler.getTagValue(transformNode, "enclosure");
      breakInEnclosureAllowed =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "enclosure_breaks"));
      escapeCharacter = XmlHandler.getTagValue(transformNode, "escapechar");
      header = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      nrHeaderLines = Const.toInt(XmlHandler.getTagValue(transformNode, "nr_headerlines"), 1);
      footer = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "footer"));
      nrFooterLines = Const.toInt(XmlHandler.getTagValue(transformNode, "nr_footerlines"), 1);
      lineWrapped = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "line_wrapped"));
      nrWraps = Const.toInt(XmlHandler.getTagValue(transformNode, "nr_wraps"), 1);
      layoutPaged = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "layout_paged"));
      nrLinesPerPage = Const.toInt(XmlHandler.getTagValue(transformNode, "nr_lines_per_page"), 1);
      nrLinesDocHeader =
          Const.toInt(XmlHandler.getTagValue(transformNode, "nr_lines_doc_header"), 1);
      String addToResult = XmlHandler.getTagValue(transformNode, "add_to_result_filenames");
      if (Utils.isEmpty(addToResult)) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase(addToResult);
      }

      String nempty = XmlHandler.getTagValue(transformNode, "noempty");
      noEmptyLines = YES.equalsIgnoreCase(nempty) || nempty == null;
      includeFilename = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include"));
      filenameField = XmlHandler.getTagValue(transformNode, "include_field");
      includeRowNumber = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      rowNumberByFile = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownumByFile"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      fileFormat = XmlHandler.getTagValue(transformNode, "format");
      encoding = XmlHandler.getTagValue(transformNode, "encoding");

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      Node filtersNode = XmlHandler.getSubNode(transformNode, "filters");
      int nrfiles = XmlHandler.countNodes(filenode, "name");
      int nrFields = XmlHandler.countNodes(fields, "field");
      int nrfilters = XmlHandler.countNodes(filtersNode, "filter");

      allocate(nrfiles, nrFields, nrfilters);

      for (int i = 0; i < nrfiles; i++) {
        Node filenamenode = XmlHandler.getSubNodeByNr(filenode, "name", i);
        Node filemasknode = XmlHandler.getSubNodeByNr(filenode, "filemask", i);
        Node excludefilemasknode = XmlHandler.getSubNodeByNr(filenode, "exclude_filemask", i);
        Node fileRequirednode = XmlHandler.getSubNodeByNr(filenode, "file_required", i);
        Node includeSubFoldersnode = XmlHandler.getSubNodeByNr(filenode, "include_subfolders", i);
        fileName[i] = loadSource(filenode, filenamenode, i, metadataProvider);
        fileMask[i] = XmlHandler.getNodeValue(filemasknode);
        excludeFileMask[i] = XmlHandler.getNodeValue(excludefilemasknode);
        fileRequired[i] = XmlHandler.getNodeValue(fileRequirednode);
        includeSubFolders[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

      fileType = XmlHandler.getTagValue(transformNode, "file", "type");
      fileCompression = XmlHandler.getTagValue(transformNode, "file", "compression");
      if (fileCompression == null) {
        fileCompression = "None";
        if (YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "zipped"))) {
          fileCompression = "Zip";
        }
      }

      // Backward compatibility : just one filter
      if (XmlHandler.getTagValue(transformNode, "filter") != null) {
        filter = new TextFileFilter[1];
        filter[0] = new TextFileFilter();

        filter[0].setFilterPosition(
            Const.toInt(XmlHandler.getTagValue(transformNode, "filter_position"), -1));
        filter[0].setFilterString(XmlHandler.getTagValue(transformNode, "filter_string"));
        filter[0].setFilterLastLine(
            YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "filter_is_last_line")));
        filter[0].setFilterPositive(
            YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "filter_is_positive")));
      } else {
        for (int i = 0; i < nrfilters; i++) {
          Node fnode = XmlHandler.getSubNodeByNr(filtersNode, "filter", i);
          filter[i] = new TextFileFilter();

          filter[i].setFilterPosition(
              Const.toInt(XmlHandler.getTagValue(fnode, "filter_position"), -1));

          String filterString = XmlHandler.getTagValue(fnode, "filter_string");
          if (filterString != null && filterString.startsWith(STRING_BASE64_PREFIX)) {
            filter[i].setFilterString(
                new String(
                    Base64.decodeBase64(
                        filterString.substring(STRING_BASE64_PREFIX.length()).getBytes())));
          } else {
            filter[i].setFilterString(filterString);
          }

          filter[i].setFilterLastLine(
              YES.equalsIgnoreCase(XmlHandler.getTagValue(fnode, "filter_is_last_line")));
          filter[i].setFilterPositive(
              YES.equalsIgnoreCase(XmlHandler.getTagValue(fnode, "filter_is_positive")));
        }
      }

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        TextFileInputField field = new TextFileInputField();

        field.setName(XmlHandler.getTagValue(fnode, "name"));
        field.setType(ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fnode, "type")));
        field.setFormat(XmlHandler.getTagValue(fnode, "format"));
        field.setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        field.setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        field.setGroupSymbol(XmlHandler.getTagValue(fnode, "group"));
        field.setNullString(XmlHandler.getTagValue(fnode, "nullif"));
        field.setIfNullValue(XmlHandler.getTagValue(fnode, "ifnull"));
        field.setPosition(Const.toInt(XmlHandler.getTagValue(fnode, "position"), -1));
        field.setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        field.setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
        field.setTrimType(
            ValueMetaString.getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));
        field.setRepeated(YES.equalsIgnoreCase(XmlHandler.getTagValue(fnode, "repeat")));

        inputFields[i] = field;
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);

      errorIgnored = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "error_ignored"));
      skipBadFiles = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "skip_bad_files"));
      fileErrorField = XmlHandler.getTagValue(transformNode, "file_error_field");
      fileErrorMessageField = XmlHandler.getTagValue(transformNode, "file_error_message_field");
      errorLineSkipped =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "error_line_skipped"));
      errorCountField = XmlHandler.getTagValue(transformNode, "error_count_field");
      errorFieldsField = XmlHandler.getTagValue(transformNode, "error_fields_field");
      errorTextField = XmlHandler.getTagValue(transformNode, "error_text_field");
      warningFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "bad_line_files_destination_directory");
      warningFilesExtension = XmlHandler.getTagValue(transformNode, "bad_line_files_extension");
      errorFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "error_line_files_destination_directory");
      errorFilesExtension = XmlHandler.getTagValue(transformNode, "error_line_files_extension");
      lineNumberFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "line_number_files_destination_directory");
      lineNumberFilesExtension =
          XmlHandler.getTagValue(transformNode, "line_number_files_extension");
      // Backward compatible

      dateFormatLenient =
          !NO.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "date_format_lenient"));
      String dateLocale = XmlHandler.getTagValue(transformNode, "date_format_locale");
      if (dateLocale != null) {
        dateFormatLocale = EnvUtil.createLocale(dateLocale);
      } else {
        dateFormatLocale = Locale.getDefault();
      }

      shortFileFieldName = XmlHandler.getTagValue(transformNode, "shortFileFieldName");
      pathFieldName = XmlHandler.getTagValue(transformNode, "pathFieldName");
      hiddenFieldName = XmlHandler.getTagValue(transformNode, "hiddenFieldName");
      lastModificationTimeFieldName =
          XmlHandler.getTagValue(transformNode, "lastModificationTimeFieldName");
      uriNameFieldName = XmlHandler.getTagValue(transformNode, "uriNameFieldName");
      rootUriNameFieldName = XmlHandler.getTagValue(transformNode, "rootUriNameFieldName");
      extensionFieldName = XmlHandler.getTagValue(transformNode, "extensionFieldName");
      sizeFieldName = XmlHandler.getTagValue(transformNode, "sizeFieldName");
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  @Override
  public Object clone() {
    TextFileInputMeta retval = (TextFileInputMeta) super.clone();

    int nrFiles = fileName.length;
    int nrFields = inputFields.length;
    int nrfilters = filter.length;

    retval.allocate(nrFiles, nrFields, nrfilters);

    System.arraycopy(fileName, 0, retval.fileName, 0, nrFiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrFiles);
    System.arraycopy(excludeFileMask, 0, retval.excludeFileMask, 0, nrFiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrFiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles);

    for (int i = 0; i < nrFields; i++) {
      retval.inputFields[i] = (TextFileInputField) inputFields[i].clone();
    }

    for (int i = 0; i < nrfilters; i++) {
      retval.filter[i] = (TextFileFilter) filter[i].clone();
    }

    retval.dateFormatLocale = (Locale) dateFormatLocale.clone();
    retval.fileCompression = fileCompression;

    return retval;
  }

  public void allocate(int nrfiles, int nrFields, int nrfilters) {
    allocateFiles(nrfiles);

    inputFields = new TextFileInputField[nrFields];
    filter = new TextFileFilter[nrfilters];
  }

  public void allocateFiles(int nrFiles) {
    fileName = new String[nrFiles];
    fileMask = new String[nrFiles];
    excludeFileMask = new String[nrFiles];
    fileRequired = new String[nrFiles];
    includeSubFolders = new String[nrFiles];
  }

  @Override
  public void setDefault() {
    shortFileFieldName = null;
    pathFieldName = null;
    hiddenFieldName = null;
    lastModificationTimeFieldName = null;
    uriNameFieldName = null;
    rootUriNameFieldName = null;
    extensionFieldName = null;
    sizeFieldName = null;

    isaddresult = true;
    separator = ";";
    enclosure = "\"";
    breakInEnclosureAllowed = false;
    header = true;
    nrHeaderLines = 1;
    footer = false;
    nrFooterLines = 1;
    lineWrapped = false;
    nrWraps = 1;
    layoutPaged = false;
    nrLinesPerPage = 80;
    nrLinesDocHeader = 0;
    fileCompression = "None";
    noEmptyLines = true;
    fileFormat = "DOS";
    fileType = "CSV";
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    errorIgnored = false;
    skipBadFiles = false;
    errorLineSkipped = false;
    warningFilesDestinationDirectory = null;
    warningFilesExtension = "warning";
    errorFilesDestinationDirectory = null;
    errorFilesExtension = "error";
    lineNumberFilesDestinationDirectory = null;
    lineNumberFilesExtension = "line";
    dateFormatLenient = true;
    rowNumberByFile = false;

    int nrfiles = 0;
    int nrFields = 0;
    int nrfilters = 0;

    allocate(nrfiles, nrFields, nrfilters);

    for (int i = 0; i < nrfiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      excludeFileMask[i] = "";
      fileRequired[i] = NO;
      includeSubFolders[i] = NO;
    }

    for (int i = 0; i < nrFields; i++) {
      inputFields[i] = new TextFileInputField("field" + (i + 1), 1, -1);
    }

    dateFormatLocale = Locale.getDefault();

    rowLimit = 0L;
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
    if (!isPassingThruFields()) {
      // all incoming fields are not transmitted !
      row.clear();
    } else {
      if (info != null) {
        boolean found = false;
        for (int i = 0; i < info.length && !found; i++) {
          if (info[i] != null) {
            row.mergeRowMeta(info[i]);
            found = true;
          }
        }
      }
    }

    for (int i = 0; i < inputFields.length; i++) {
      TextFileInputField field = inputFields[i];

      int type = field.getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }

      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(field.getName(), type);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setOrigin(name);
        v.setConversionMask(field.getFormat());
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupSymbol());
        v.setCurrencySymbol(field.getCurrencySymbol());
        v.setDateFormatLenient(dateFormatLenient);
        v.setDateFormatLocale(dateFormatLocale);
        v.setTrimType(field.getTrimType());

        row.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
    if (errorIgnored) {
      if (errorCountField != null && errorCountField.length() > 0) {
        IValueMeta v = new ValueMetaInteger(errorCountField);
        v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
        v.setOrigin(name);
        row.addValueMeta(v);
      }
      if (errorFieldsField != null && errorFieldsField.length() > 0) {
        IValueMeta v = new ValueMetaString(errorFieldsField);
        v.setOrigin(name);
        row.addValueMeta(v);
      }
      if (errorTextField != null && errorTextField.length() > 0) {
        IValueMeta v = new ValueMetaString(errorTextField);
        v.setOrigin(name);
        row.addValueMeta(v);
      }
    }
    if (includeFilename) {
      IValueMeta v = new ValueMetaString(filenameField);
      v.setLength(100);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(rowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Add additional fields

    if (getShortFileNameField() != null && getShortFileNameField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getShortFileNameField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getExtensionField() != null && getExtensionField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getExtensionField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getPathField() != null && getPathField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getPathField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getSizeField() != null && getSizeField().length() > 0) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(getSizeField()));
      v.setOrigin(name);
      v.setLength(9);
      row.addValueMeta(v);
    }
    if (isHiddenField() != null && isHiddenField().length() > 0) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(isHiddenField()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (getLastModificationDateField() != null && getLastModificationDateField().length() > 0) {
      IValueMeta v = new ValueMetaDate(variables.resolve(getLastModificationDateField()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getUriField() != null && getUriField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (getRootUriField() != null && getRootUriField().length() > 0) {
      IValueMeta v = new ValueMetaString(getRootUriField());
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(1500);

    retval.append("    ").append(XmlHandler.addTagValue("accept_filenames", acceptingFilenames));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("passing_through_fields", passingThruFields));
    retval.append("    ").append(XmlHandler.addTagValue("accept_field", acceptingField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "accept_transform_name",
                (acceptingTransform != null ? acceptingTransform.getName() : "")));

    retval.append("    ").append(XmlHandler.addTagValue("separator", separator));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure", enclosure));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("enclosure_breaks", breakInEnclosureAllowed));
    retval.append("    ").append(XmlHandler.addTagValue("escapechar", escapeCharacter));
    retval.append("    ").append(XmlHandler.addTagValue("header", header));
    retval.append("    ").append(XmlHandler.addTagValue("nr_headerlines", nrHeaderLines));
    retval.append("    ").append(XmlHandler.addTagValue("footer", footer));
    retval.append("    ").append(XmlHandler.addTagValue("nr_footerlines", nrFooterLines));
    retval.append("    ").append(XmlHandler.addTagValue("line_wrapped", lineWrapped));
    retval.append("    ").append(XmlHandler.addTagValue("nr_wraps", nrWraps));
    retval.append("    ").append(XmlHandler.addTagValue("layout_paged", layoutPaged));
    retval.append("    ").append(XmlHandler.addTagValue("nr_lines_per_page", nrLinesPerPage));
    retval.append("    ").append(XmlHandler.addTagValue("nr_lines_doc_header", nrLinesDocHeader));
    retval.append("    ").append(XmlHandler.addTagValue("noempty", noEmptyLines));
    retval.append("    ").append(XmlHandler.addTagValue("include", includeFilename));
    retval.append("    ").append(XmlHandler.addTagValue("include_field", filenameField));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("rownumByFile", rowNumberByFile));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("format", fileFormat));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    " + XmlHandler.addTagValue("add_to_result_filenames", isaddresult));

    retval.append("    <file>").append(Const.CR);
    for (int i = 0; i < fileName.length; i++) {
      saveSource(retval, fileName[i]);
      retval.append("      ").append(XmlHandler.addTagValue("filemask", fileMask[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("exclude_filemask", excludeFileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_required", fileRequired[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("include_subfolders", includeSubFolders[i]));
    }
    retval.append("      ").append(XmlHandler.addTagValue("type", fileType));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "compression", (fileCompression == null) ? "None" : fileCompression));
    retval.append("    </file>").append(Const.CR);

    retval.append("    <filters>").append(Const.CR);
    for (int i = 0; i < filter.length; i++) {
      String filterString = filter[i].getFilterString();
      byte[] filterBytes = new byte[] {};
      String filterPrefix = "";
      if (filterString != null) {
        filterBytes = filterString.getBytes();
        filterPrefix = STRING_BASE64_PREFIX;
      }
      String filterEncoded = filterPrefix + new String(Base64.encodeBase64(filterBytes));

      retval.append("      <filter>").append(Const.CR);
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("filter_string", filterEncoded, false));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("filter_position", filter[i].getFilterPosition(), false));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue("filter_is_last_line", filter[i].isFilterLastLine(), false));
      retval
          .append("        ")
          .append(
              XmlHandler.addTagValue("filter_is_positive", filter[i].isFilterPositive(), false));
      retval.append("      </filter>").append(Const.CR);
    }
    retval.append("    </filters>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      TextFileInputField field = inputFields[i];

      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", field.getName()));
      retval.append("        ").append(XmlHandler.addTagValue("type", field.getTypeDesc()));
      retval.append("        ").append(XmlHandler.addTagValue("format", field.getFormat()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("currency", field.getCurrencySymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("decimal", field.getDecimalSymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("group", field.getGroupSymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("nullif", field.getNullString()));
      retval.append("        ").append(XmlHandler.addTagValue("ifnull", field.getIfNullValue()));
      retval.append("        ").append(XmlHandler.addTagValue("position", field.getPosition()));
      retval.append("        ").append(XmlHandler.addTagValue("length", field.getLength()));
      retval.append("        ").append(XmlHandler.addTagValue("precision", field.getPrecision()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("trim_type", field.getTrimTypeCode()));
      retval.append("        ").append(XmlHandler.addTagValue("repeat", field.isRepeated()));
      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </fields>").append(Const.CR);
    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));

    // ERROR HANDLING
    retval.append("    ").append(XmlHandler.addTagValue("error_ignored", errorIgnored));
    retval.append("    ").append(XmlHandler.addTagValue("skip_bad_files", skipBadFiles));
    retval.append("    ").append(XmlHandler.addTagValue("file_error_field", fileErrorField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("file_error_message_field", fileErrorMessageField));
    retval.append("    ").append(XmlHandler.addTagValue("error_line_skipped", errorLineSkipped));
    retval.append("    ").append(XmlHandler.addTagValue("error_count_field", errorCountField));
    retval.append("    ").append(XmlHandler.addTagValue("error_fields_field", errorFieldsField));
    retval.append("    ").append(XmlHandler.addTagValue("error_text_field", errorTextField));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "bad_line_files_destination_directory", warningFilesDestinationDirectory));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("bad_line_files_extension", warningFilesExtension));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "error_line_files_destination_directory", errorFilesDestinationDirectory));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("error_line_files_extension", errorFilesExtension));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "line_number_files_destination_directory", lineNumberFilesDestinationDirectory));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("line_number_files_extension", lineNumberFilesExtension));

    retval.append("    ").append(XmlHandler.addTagValue("date_format_lenient", dateFormatLenient));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "date_format_locale",
                dateFormatLocale != null
                    ? dateFormatLocale.toString()
                    : Locale.getDefault().toString()));

    retval.append("    ").append(XmlHandler.addTagValue("shortFileFieldName", shortFileFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("pathFieldName", pathFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("hiddenFieldName", hiddenFieldName));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("lastModificationTimeFieldName", lastModificationTimeFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("uriNameFieldName", uriNameFieldName));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("rootUriNameFieldName", rootUriNameFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("extensionFieldName", extensionFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("sizeFieldName", sizeFieldName));

    return retval.toString();
  }

  public String getLookupTransformName() {
    if (acceptingFilenames
        && acceptingTransform != null
        && !Utils.isEmpty(acceptingTransform.getName())) {
      return acceptingTransform.getName();
    }
    return null;
  }

  /** @param transforms optionally search the info transform in a list of transforms */
  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    acceptingTransform = TransformMeta.findTransform(transforms, acceptingTransformName);
  }

  public String[] getInfoTransforms() {
    if (acceptingFilenames && acceptingTransform != null) {
      return new String[] {acceptingTransform.getName()};
    }
    return null;
  }

  @Override
  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubFolderBoolean());
  }

  public FileInputList getTextFileList(IVariables variables) {
    return FileInputList.createFileList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubFolderBoolean());
  }

  private boolean[] includeSubFolderBoolean() {
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
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

    // See if we get input...
    if (input.length > 0) {
      if (!isAcceptingFilenames()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "TextFileInputMeta.CheckResult.NoInputError"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TextFileInputMeta.CheckResult.AcceptFilenamesOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TextFileInputMeta.CheckResult.NoInputOk"),
              transformMeta);
      remarks.add(cr);
    }

    FileInputList textFileList = getTextFileList(variables);
    if (textFileList.nrOfFiles() == 0) {
      if (!isAcceptingFilenames()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "TextFileInputMeta.CheckResult.ExpectedFilesError"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "TextFileInputMeta.CheckResult.ExpectedFilesOk",
                  "" + textFileList.nrOfFiles()),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      TextFileInputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TextFileInput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public TextFileInputData getTransformData() {
    return new TextFileInputData();
  }

  /** @return Returns the escapeCharacter. */
  @Override
  public String getEscapeCharacter() {
    return escapeCharacter;
  }

  /** @param escapeCharacter The escapeCharacter to set. */
  public void setEscapeCharacter(String escapeCharacter) {
    this.escapeCharacter = escapeCharacter;
  }

  @Override
  public String getErrorCountField() {
    return errorCountField;
  }

  public void setErrorCountField(String errorCountField) {
    this.errorCountField = errorCountField;
  }

  @Override
  public String getErrorFieldsField() {
    return errorFieldsField;
  }

  public void setErrorFieldsField(String errorFieldsField) {
    this.errorFieldsField = errorFieldsField;
  }

  @Override
  public boolean isErrorIgnored() {
    return errorIgnored;
  }

  public void setErrorIgnored(boolean errorIgnored) {
    this.errorIgnored = errorIgnored;
  }

  @Override
  public String getErrorTextField() {
    return errorTextField;
  }

  public void setErrorTextField(String errorTextField) {
    this.errorTextField = errorTextField;
  }

  /** @return Returns the lineWrapped. */
  public boolean isLineWrapped() {
    return lineWrapped;
  }

  /** @param lineWrapped The lineWrapped to set. */
  public void setLineWrapped(boolean lineWrapped) {
    this.lineWrapped = lineWrapped;
  }

  /** @return Returns the nrFooterLines. */
  public int getNrFooterLines() {
    return nrFooterLines;
  }

  /** @param nrFooterLines The nrFooterLines to set. */
  public void setNrFooterLines(int nrFooterLines) {
    this.nrFooterLines = nrFooterLines;
  }

  public String getRequiredFilesDesc(String tt) {
    if (tt == null) {
      return RequiredFilesDesc[0];
    }
    if (tt.equals(RequiredFilesCode[1])) {
      return RequiredFilesDesc[1];
    } else {
      return RequiredFilesDesc[0];
    }
  }

  /** @return Returns the nrHeaderLines. */
  @Override
  public int getNrHeaderLines() {
    return nrHeaderLines;
  }

  /** @param nrHeaderLines The nrHeaderLines to set. */
  public void setNrHeaderLines(int nrHeaderLines) {
    this.nrHeaderLines = nrHeaderLines;
  }

  /** @return Returns the nrWraps. */
  public int getNrWraps() {
    return nrWraps;
  }

  /** @param nrWraps The nrWraps to set. */
  public void setNrWraps(int nrWraps) {
    this.nrWraps = nrWraps;
  }

  /** @return Returns the layoutPaged. */
  public boolean isLayoutPaged() {
    return layoutPaged;
  }

  /** @param layoutPaged The layoutPaged to set. */
  public void setLayoutPaged(boolean layoutPaged) {
    this.layoutPaged = layoutPaged;
  }

  /** @return Returns the nrLinesPerPage. */
  public int getNrLinesPerPage() {
    return nrLinesPerPage;
  }

  /** @param nrLinesPerPage The nrLinesPerPage to set. */
  public void setNrLinesPerPage(int nrLinesPerPage) {
    this.nrLinesPerPage = nrLinesPerPage;
  }

  /** @return Returns the nrLinesDocHeader. */
  public int getNrLinesDocHeader() {
    return nrLinesDocHeader;
  }

  /** @param nrLinesDocHeader The nrLinesDocHeader to set. */
  public void setNrLinesDocHeader(int nrLinesDocHeader) {
    this.nrLinesDocHeader = nrLinesDocHeader;
  }

  public String getWarningFilesDestinationDirectory() {
    return warningFilesDestinationDirectory;
  }

  public void setWarningFilesDestinationDirectory(String warningFilesDestinationDirectory) {
    this.warningFilesDestinationDirectory = warningFilesDestinationDirectory;
  }

  public String getWarningFilesExtension() {
    return warningFilesExtension;
  }

  public void setWarningFilesExtension(String warningFilesExtension) {
    this.warningFilesExtension = warningFilesExtension;
  }

  public String getLineNumberFilesDestinationDirectory() {
    return lineNumberFilesDestinationDirectory;
  }

  public void setLineNumberFilesDestinationDirectory(String lineNumberFilesDestinationDirectory) {
    this.lineNumberFilesDestinationDirectory = lineNumberFilesDestinationDirectory;
  }

  public String getLineNumberFilesExtension() {
    return lineNumberFilesExtension;
  }

  public void setLineNumberFilesExtension(String lineNumberFilesExtension) {
    this.lineNumberFilesExtension = lineNumberFilesExtension;
  }

  public String getErrorFilesDestinationDirectory() {
    return errorFilesDestinationDirectory;
  }

  public void setErrorFilesDestinationDirectory(String errorFilesDestinationDirectory) {
    this.errorFilesDestinationDirectory = errorFilesDestinationDirectory;
  }

  public String getErrorLineFilesExtension() {
    return errorFilesExtension;
  }

  public void setErrorLineFilesExtension(String errorLineFilesExtension) {
    this.errorFilesExtension = errorLineFilesExtension;
  }

  public boolean isDateFormatLenient() {
    return dateFormatLenient;
  }

  public void setDateFormatLenient(boolean dateFormatLenient) {
    this.dateFormatLenient = dateFormatLenient;
  }

  /** @param isaddresult The isaddresult to set. */
  public void setAddResultFile(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /** @return Returns isaddresult. */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  @Override
  public boolean isErrorLineSkipped() {
    return errorLineSkipped;
  }

  public void setErrorLineSkipped(boolean errorLineSkipped) {
    this.errorLineSkipped = errorLineSkipped;
  }

  /** @return Returns the dateFormatLocale. */
  public Locale getDateFormatLocale() {
    return dateFormatLocale;
  }

  /** @param dateFormatLocale The dateFormatLocale to set. */
  public void setDateFormatLocale(Locale dateFormatLocale) {
    this.dateFormatLocale = dateFormatLocale;
  }

  public boolean isAcceptingFilenames() {
    return acceptingFilenames;
  }

  public void setAcceptingFilenames(boolean getFileFromJob) {
    this.acceptingFilenames = getFileFromJob;
  }

  public boolean isPassingThruFields() {
    return passingThruFields;
  }

  public void setPassingThruFields(boolean passingThruFields) {
    this.passingThruFields = passingThruFields;
  }

  /** @return Returns the fileNameField. */
  public String getAcceptingField() {
    return acceptingField;
  }

  /** @param fileNameField The fileNameField to set. */
  public void setAcceptingField(String fileNameField) {
    this.acceptingField = fileNameField;
  }

  /** @return Returns the acceptingTransform. */
  public String getAcceptingTransformName() {
    return acceptingTransformName;
  }

  /** @param acceptingTransform The acceptingTransform to set. */
  public void setAcceptingTransformName(String acceptingTransform) {
    this.acceptingTransformName = acceptingTransform;
  }

  /** @return Returns the acceptingTransform. */
  public TransformMeta getAcceptingTransform() {
    return acceptingTransform;
  }

  /** @param acceptingTransform The acceptingTransform to set. */
  public void setAcceptingTransform(TransformMeta acceptingTransform) {
    this.acceptingTransform = acceptingTransform;
  }

  @Override
  public int getFileFormatTypeNr() {
    // calculate the file format type in advance so we can use a switch
    if (getFileFormat().equalsIgnoreCase("DOS")) {
      return FILE_FORMAT_DOS;
    } else if (getFileFormat().equalsIgnoreCase("unix")) {
      return TextFileInputMeta.FILE_FORMAT_UNIX;
    } else {
      return TextFileInputMeta.FILE_FORMAT_MIXED;
    }
  }

  public int getFileTypeNr() {
    // calculate the file type in advance CSV or Fixed?
    if (getFileType().equalsIgnoreCase("CSV")) {
      return TextFileInputMeta.FILE_TYPE_CSV;
    } else {
      return TextFileInputMeta.FILE_TYPE_FIXED;
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    String[] textFiles = getFilePaths(variables);
    if (textFiles != null) {
      for (int i = 0; i < textFiles.length; i++) {
        reference.getEntries().add(new ResourceEntry(textFiles[i], ResourceType.FILE));
      }
    }
    return references;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
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
      if (!acceptingFilenames) {

        // Replace the filename ONLY (folder or filename)
        //
        for (int i = 0; i < fileName.length; i++) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName[i]));
          fileName[i] =
              iResourceNaming.nameResource(fileObject, variables, Utils.isEmpty(fileMask[i]));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return isErrorIgnored() && isSkipBadFiles();
  }

  @VisibleForTesting
  public void setFileNameForTest(String[] fileName) {
    allocateFiles(fileName.length);
    setFileName(fileName);
  }

  protected String loadSource(
      Node filenode, Node filenamenode, int i, IHopMetadataProvider metadataProvider) {
    return XmlHandler.getNodeValue(filenamenode);
  }

  protected void saveSource(StringBuilder retVal, String source) {
    retVal.append("      ").append(XmlHandler.addTagValue("name", source));
  }
}

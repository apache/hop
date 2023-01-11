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

package org.apache.hop.pipeline.transforms.fileinput.text;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.*;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IStringObjectConverter;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.common.ICsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputFiles;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@SuppressWarnings("deprecation")
@Transform(
    id = "TextFileInput2",
    image = "textfileinput.svg",
    name = "i18n::TextFileInput.Name",
    description = "i18n::TextFileInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::TextFileInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/textfileinput.html")
public class TextFileInputMeta
    extends BaseFileInputMeta<
        TextFileInput,
        TextFileInputData,
        BaseFileInputAdditionalField,
        BaseFileInputFiles,
        BaseFileField>
    implements ICsvInputAwareMeta {
  private static final Class<?> PKG = TextFileInputMeta.class; // For Translator

  @HopMetadataProperty(inline = true)
  public Content content = new Content();

  public static class Content implements Cloneable {

    /** Type of file: CSV or fixed */
    @HopMetadataProperty(
        key = "type",
        injectionKey = "FILE_TYPE",
        injectionKeyDescription = "TextFileInput.Injection.FILE_TYPE")
    public String fileType;

    /** String used to separated field (;) */
    @HopMetadataProperty(
        key = "separator",
        injectionKey = "SEPARATOR",
        injectionKeyDescription = "TextFileInput.Injection.SEPARATOR")
    public String separator;

    /** String used to enclose separated fields (") */
    @HopMetadataProperty(
        key = "enclosure",
        injectionKey = "ENCLOSURE",
        injectionKeyDescription = "TextFileInput.Injection.ENCLOSURE")
    public String enclosure;

    /** Switch to allow breaks (CR/LF) in Enclosures */
    @HopMetadataProperty(
        key = "enclosure_breaks",
        injectionKey = "BREAK_IN_ENCLOSURE",
        injectionKeyDescription = "TextFileInput.Injection.BREAK_IN_ENCLOSURE")
    public boolean breakInEnclosureAllowed;

    /** Escape character used to escape the enclosure String (\) */
    @HopMetadataProperty(
        key = "escapechar",
        injectionKey = "ESCAPE_CHAR",
        injectionKeyDescription = "TextFileInput.Injection.ESCAPE_CHAR")
    public String escapeCharacter;

    /** Flag indicating that the file contains one header line that should be skipped. */
    @HopMetadataProperty(
        key = "header",
        injectionKey = "HEADER_PRESENT",
        injectionKeyDescription = "TextFileInput.Injection.HEADER_PRESENT")
    public boolean header;

    /** The number of header lines, defaults to 1 */
    @HopMetadataProperty(
        key = "nr_headerlines",
        injectionKey = "NR_HEADER_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NR_HEADER_LINES")
    public int nrHeaderLines = -1;

    /** Flag indicating that the file contains one footer line that should be skipped. */
    @HopMetadataProperty(
        key = "footer",
        injectionKey = "HAS_FOOTER",
        injectionKeyDescription = "TextFileInput.Injection.HAS_FOOTER")
    public boolean footer;

    /** The number of footer lines, defaults to 1 */
    @HopMetadataProperty(
        key = "nr_footerlines",
        injectionKey = "NR_FOOTER_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NR_FOOTER_LINES")
    public int nrFooterLines = -1;

    /** Flag indicating that a single line is wrapped onto one or more lines in the text file. */
    @HopMetadataProperty(
        key = "line_wrapped",
        injectionKey = "HAS_WRAPPED_LINES",
        injectionKeyDescription = "TextFileInput.Injection.HAS_WRAPPED_LINES")
    public boolean lineWrapped;

    /** The number of times the line wrapped */
    @HopMetadataProperty(
        key = "nr_wraps",
        injectionKey = "NR_WRAPS",
        injectionKeyDescription = "TextFileInput.Injection.NR_WRAPS")
    public int nrWraps = -1;

    /** Flag indicating that the text-file has a paged layout. */
    @HopMetadataProperty(
        key = "layout_paged",
        injectionKey = "HAS_PAGED_LAYOUT",
        injectionKeyDescription = "TextFileInput.Injection.HAS_PAGED_LAYOUT")
    public boolean layoutPaged;

    /** The number of lines to read per page */
    @HopMetadataProperty(
        key = "nr_lines_per_page",
        injectionKey = "NR_LINES_PER_PAGE",
        injectionKeyDescription = "TextFileInput.Injection.NR_LINES_PER_PAGE")
    public int nrLinesPerPage = -1;

    /** The number of lines in the document header */
    @HopMetadataProperty(
        key = "nr_lines_doc_header",
        injectionKey = "NR_DOC_HEADER_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NR_DOC_HEADER_LINES")
    public int nrLinesDocHeader = -1;

    /** Type of compression being used */
    @HopMetadataProperty(
        key = "compression",
        injectionKey = "COMPRESSION_TYPE",
        injectionKeyDescription = "TextFileInput.Injection.COMPRESSION_TYPE")
    public String fileCompression;

    /** Flag indicating that we should skip all empty lines */
    @HopMetadataProperty(
        key = "noempty",
        injectionKey = "NO_EMPTY_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NO_EMPTY_LINES")
    public boolean noEmptyLines;

    /** Flag indicating that we should include the filename in the output */
    @HopMetadataProperty(
        key = "include",
        injectionKey = "INCLUDE_FILENAME",
        injectionKeyDescription = "TextFileInput.Injection.INCLUDE_FILENAME")
    public boolean includeFilename;

    /** The name of the field in the output containing the filename */
    @HopMetadataProperty(
        key = "include_field",
        injectionKey = "FILENAME_FIELD",
        injectionKeyDescription = "TextFileInput.Injection.FILENAME_FIELD")
    public String filenameField;

    /** Flag indicating that a row number field should be included in the output */
    @HopMetadataProperty(
        key = "rownum",
        injectionKey = "INCLUDE_ROW_NUMBER",
        injectionKeyDescription = "TextFileInput.Injection.INCLUDE_ROW_NUMBER")
    public boolean includeRowNumber;

    /** The name of the field in the output containing the row number */
    @HopMetadataProperty(
        key = "rownum_field",
        injectionKey = "ROW_NUMBER_FIELD",
        injectionKeyDescription = "TextFileInput.Injection.ROW_NUMBER_FIELD")
    public String rowNumberField;

    /** Flag indicating row number is per file */
    @HopMetadataProperty(
        key = "rownumByFile",
        injectionKey = "ROW_NUMBER_BY_FILE",
        injectionKeyDescription = "TextFileInput.Injection.ROW_NUMBER_BY_FILE")
    public boolean rowNumberByFile;

    /** The file format: DOS or UNIX or mixed */
    @HopMetadataProperty(
        key = "format",
        injectionKey = "FILE_FORMAT",
        injectionKeyDescription = "TextFileInput.Injection.FILE_FORMAT")
    public String fileFormat;

    /** The encoding to use for reading: null or empty string means system default encoding */
    @HopMetadataProperty(
        key = "encoding",
        injectionKey = "ENCODING",
        injectionKeyDescription = "TextFileInput.Injection.ENCODING")
    public String encoding;

    /** The maximum number or lines to read */
    @HopMetadataProperty(
        key = "limit",
        injectionKey = "ROW_LIMIT",
        injectionKeyDescription = "TextFileInput.Injection.ROW_LIMIT")
    public long rowLimit = -1;

    /**
     * Indicate whether or not we want to date fields strictly according to the format or lenient
     */
    @HopMetadataProperty(
        key = "date_format_lenient",
        injectionKey = "DATE_FORMAT_LENIENT",
        injectionKeyDescription = "TextFileInput.Injection.DATE_FORMAT_LENIENT")
    public boolean dateFormatLenient;

    /** Specifies the Locale of the Date format, null means the default */
    @HopMetadataProperty(
        key = "date_format_locale",
        injectionKey = "DATE_FORMAT_LOCALE",
        injectionKeyDescription = "TextFileInput.Injection.DATE_FORMAT_LOCALE",
        injectionStringObjectConverter = DateFormatLocaleConverter.class)
    public Locale dateFormatLocale;

    public void setDateFormatLocale(String locale) {
      this.dateFormatLocale = new Locale(locale);
    }

    public Locale getDateFormatLocale() {
      return dateFormatLocale;
    }

    /** Length based on bytes or characters */
    @HopMetadataProperty(
        key = "length",
        injectionKey = "LENGTH",
        injectionKeyDescription = "TextFileInput.Injection.LENGTH")
    public String length;

    public String getFileType() {
      return fileType;
    }

    public void setFileType(String fileType) {
      this.fileType = fileType;
    }

    public String getSeparator() {
      return separator;
    }

    public void setSeparator(String separator) {
      this.separator = separator;
    }

    public String getEnclosure() {
      return enclosure;
    }

    public void setEnclosure(String enclosure) {
      this.enclosure = enclosure;
    }

    public boolean isBreakInEnclosureAllowed() {
      return breakInEnclosureAllowed;
    }

    public void setBreakInEnclosureAllowed(boolean breakInEnclosureAllowed) {
      this.breakInEnclosureAllowed = breakInEnclosureAllowed;
    }

    public String getEscapeCharacter() {
      return escapeCharacter;
    }

    public void setEscapeCharacter(String escapeCharacter) {
      this.escapeCharacter = escapeCharacter;
    }

    public boolean isHeader() {
      return header;
    }

    public void setHeader(boolean header) {
      this.header = header;
    }

    public int getNrHeaderLines() {
      return nrHeaderLines;
    }

    public void setNrHeaderLines(int nrHeaderLines) {
      this.nrHeaderLines = nrHeaderLines;
    }

    public boolean isFooter() {
      return footer;
    }

    public void setFooter(boolean footer) {
      this.footer = footer;
    }

    public int getNrFooterLines() {
      return nrFooterLines;
    }

    public void setNrFooterLines(int nrFooterLines) {
      this.nrFooterLines = nrFooterLines;
    }

    public boolean isLineWrapped() {
      return lineWrapped;
    }

    public void setLineWrapped(boolean lineWrapped) {
      this.lineWrapped = lineWrapped;
    }

    public int getNrWraps() {
      return nrWraps;
    }

    public void setNrWraps(int nrWraps) {
      this.nrWraps = nrWraps;
    }

    public boolean isLayoutPaged() {
      return layoutPaged;
    }

    public void setLayoutPaged(boolean layoutPaged) {
      this.layoutPaged = layoutPaged;
    }

    public int getNrLinesPerPage() {
      return nrLinesPerPage;
    }

    public void setNrLinesPerPage(int nrLinesPerPage) {
      this.nrLinesPerPage = nrLinesPerPage;
    }

    public int getNrLinesDocHeader() {
      return nrLinesDocHeader;
    }

    public void setNrLinesDocHeader(int nrLinesDocHeader) {
      this.nrLinesDocHeader = nrLinesDocHeader;
    }

    public String getFileCompression() {
      return fileCompression;
    }

    public void setFileCompression(String fileCompression) {
      this.fileCompression = fileCompression;
    }

    public boolean isNoEmptyLines() {
      return noEmptyLines;
    }

    public void setNoEmptyLines(boolean noEmptyLines) {
      this.noEmptyLines = noEmptyLines;
    }

    public boolean isIncludeFilename() {
      return includeFilename;
    }

    public void setIncludeFilename(boolean includeFilename) {
      this.includeFilename = includeFilename;
    }

    public String getFilenameField() {
      return filenameField;
    }

    public void setFilenameField(String filenameField) {
      this.filenameField = filenameField;
    }

    public boolean isIncludeRowNumber() {
      return includeRowNumber;
    }

    public void setIncludeRowNumber(boolean includeRowNumber) {
      this.includeRowNumber = includeRowNumber;
    }

    public String getRowNumberField() {
      return rowNumberField;
    }

    public void setRowNumberField(String rowNumberField) {
      this.rowNumberField = rowNumberField;
    }

    public boolean isRowNumberByFile() {
      return rowNumberByFile;
    }

    public void setRowNumberByFile(boolean rowNumberByFile) {
      this.rowNumberByFile = rowNumberByFile;
    }

    public String getFileFormat() {
      return fileFormat;
    }

    public void setFileFormat(String fileFormat) {
      this.fileFormat = fileFormat;
    }

    public String getEncoding() {
      return encoding;
    }

    public void setEncoding(String encoding) {
      this.encoding = encoding;
    }

    public long getRowLimit() {
      return rowLimit;
    }

    public void setRowLimit(long rowLimit) {
      this.rowLimit = rowLimit;
    }

    public boolean isDateFormatLenient() {
      return dateFormatLenient;
    }

    public void setDateFormatLenient(boolean dateFormatLenient) {
      this.dateFormatLenient = dateFormatLenient;
    }

    public void setDateFormatLocale(Locale dateFormatLocale) {
      this.dateFormatLocale = dateFormatLocale;
    }

    public String getLength() {
      return length;
    }

    public void setLength(String length) {
      this.length = length;
    }
  }

  /** The filters to use... */
  @HopMetadataProperty(
      key = "filter",
      injectionKey = "FILE_TYPE",
      injectionKeyDescription = "TextFileInput.Injection.FILE_TYPE")
  private List<TextFileFilter> filter;

  /** The name of the field that will contain the number of errors in the row */
  @HopMetadataProperty(
      key = "error_count_field",
      injectionKey = "ERROR_COUNT_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_COUNT_FIELD")
  public String errorCountField;

  /**
   * The name of the field that will contain the names of the fields that generated errors,
   * separated by ,
   */
  @HopMetadataProperty(
      key = "error_fields_field",
      injectionKey = "ERROR_FIELDS_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_FIELDS_FIELD")
  public String errorFieldsField;

  /** The name of the field that will contain the error texts, separated by CR */
  @HopMetadataProperty(
      key = "error_text_field",
      injectionKey = "ERROR_TEXT_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_TEXT_FIELD")
  public String errorTextField;

  /** If error line are skipped, you can replay without introducing doubles. */
  @HopMetadataProperty(
      key = "error_line_skipped",
      injectionKey = "ERROR_LINES_SKIPPED",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_LINES_SKIPPED")
  public boolean errorLineSkipped;

  /** The transform to accept filenames from */
  private TransformMeta acceptingTransform;

  public TextFileInputMeta() {
    additionalOutputFields = new BaseFileInputAdditionalField();
    inputFiles = new BaseFileInputFiles();
    inputFields = new BaseFileField[0];
  }

  /** @return Returns the fileName. */
  public List<String> getFileName() {
    return inputFiles.fileName;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(List<String> fileName) {
    inputFiles.fileName = fileName;
  }

  /** @return The array of filters for the metadata of this text file input transform. */
  public List<TextFileFilter> getFilter() {
    return filter;
  }

  /** @param filter The array of filters to use */
  public void setFilter(List<TextFileFilter> filter) {
    this.filter = filter;
  }

  public Content getContent() {
    return content;
  }

  public void setContent(Content content) {
    this.content = content;
  }

  @Override
  public Object clone() {
    TextFileInputMeta retval = (TextFileInputMeta) super.clone();
    retval.inputFiles = (BaseFileInputFiles) inputFiles.clone();
    retval.inputFields = new BaseFileField[inputFields.length];
    for (int i = 0; i < inputFields.length; i++) {
      retval.inputFields[i] = (BaseFileField) inputFields[i].clone();
    }

    retval.filter = new ArrayList<>();
    for (TextFileFilter fileFilter : filter) {
      retval.filter.add((TextFileFilter) fileFilter.clone());
    }
    return retval;
  }

  public void allocate(int nrfiles, int nrFields) {
    allocateFiles();

    inputFields = new BaseFileField[nrFields];
    filter = new ArrayList<>();
  }

  public void allocateFiles() {
    inputFiles.fileName = new ArrayList<>();
    inputFiles.fileMask = new ArrayList<>();
    inputFiles.excludeFileMask = new ArrayList<>();
    inputFiles.fileRequired = new ArrayList<>();
    inputFiles.includeSubFolders = new ArrayList<>();
  }

  @Override
  public void setDefault() {
    additionalOutputFields.shortFilenameField = null;
    additionalOutputFields.pathField = null;
    additionalOutputFields.hiddenField = null;
    additionalOutputFields.lastModificationField = null;
    additionalOutputFields.uriField = null;
    additionalOutputFields.rootUriField = null;
    additionalOutputFields.extensionField = null;
    additionalOutputFields.sizeField = null;

    inputFiles.isaddresult = true;

    content.separator = ";";
    content.enclosure = "\"";
    content.breakInEnclosureAllowed = false;
    content.header = true;
    content.nrHeaderLines = 1;
    content.footer = false;
    content.nrFooterLines = 1;
    content.lineWrapped = false;
    content.nrWraps = 1;
    content.layoutPaged = false;
    content.nrLinesPerPage = 80;
    content.nrLinesDocHeader = 0;
    content.fileCompression = "None";
    content.noEmptyLines = true;
    content.fileFormat = "DOS";
    content.fileType = "CSV";
    content.includeFilename = false;
    content.filenameField = "";
    content.includeRowNumber = false;
    content.rowNumberField = "";
    content.dateFormatLenient = true;
    content.rowNumberByFile = false;

    errorHandling.errorIgnored = false;
    errorHandling.skipBadFiles = false;
    errorLineSkipped = false;
    errorHandling.warningFilesDestinationDirectory = null;
    errorHandling.warningFilesExtension = "warning";
    errorHandling.errorFilesDestinationDirectory = null;
    errorHandling.errorFilesExtension = "error";
    errorHandling.lineNumberFilesDestinationDirectory = null;
    errorHandling.lineNumberFilesExtension = "line";

    int nrfiles = 0;
    int nrFields = 0;

    allocate(nrfiles, nrFields);

    for (int i = 0; i < nrfiles; i++) {
      inputFiles.fileName.add("filename" + (i + 1));
      inputFiles.fileMask.add("");
      inputFiles.excludeFileMask.add("");
      inputFiles.fileRequired.add(NO);
      inputFiles.includeSubFolders.add(NO);
    }

    for (int i = 0; i < nrFields; i++) {
      inputFields[i] = new BaseFileField("field" + (i + 1), 1, -1);
    }

    content.dateFormatLocale = Locale.getDefault();

    content.rowLimit = 0L;
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
    if (!inputFiles.passingThruFields) {
      // all incoming fields are not transmitted !
      row.clear();
    } else {
      if (info != null) {
        boolean found = false;
        for (int i = 0; i < info.length && !found; i++) {
          if (info[i] != null) {
            row.mergeRowMeta(info[i], name);
            found = true;
          }
        }
      }
    }

    for (int i = 0; i < inputFields.length; i++) {
      BaseFileField field = inputFields[i];

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
        v.setDateFormatLenient(content.dateFormatLenient);
        v.setDateFormatLocale(content.dateFormatLocale);
        v.setTrimType(field.getTrimType());

        row.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
    if (errorHandling.errorIgnored) {
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
    if (content.includeFilename) {
      IValueMeta v = new ValueMetaString(content.filenameField);
      v.setLength(100);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (content.includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(content.rowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Add additional fields

    if (StringUtils.isNotBlank(additionalOutputFields.shortFilenameField)) {
      IValueMeta v =
          new ValueMetaString(variables.resolve(additionalOutputFields.shortFilenameField));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.extensionField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.extensionField));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.pathField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.pathField));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.sizeField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.sizeField));
      v.setOrigin(name);
      v.setLength(9);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.hiddenField)) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(additionalOutputFields.hiddenField));
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (StringUtils.isNotBlank(additionalOutputFields.lastModificationField)) {
      IValueMeta v =
          new ValueMetaDate(variables.resolve(additionalOutputFields.lastModificationField));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.uriField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.uriField));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (StringUtils.isNotBlank(additionalOutputFields.rootUriField)) {
      IValueMeta v = new ValueMetaString(additionalOutputFields.rootUriField);
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  public String getLookupTransformName() {
    if (inputFiles.acceptingFilenames
        && acceptingTransform != null
        && !Utils.isEmpty(acceptingTransform.getName())) {
      return acceptingTransform.getName();
    }
    return null;
  }

  /** @param transforms optionally search the info transform in a list of transforms */
  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    acceptingTransform = TransformMeta.findTransform(transforms, inputFiles.acceptingTransformName);
  }

  public String[] getInfoTransforms() {
    if (inputFiles.acceptingFilenames && acceptingTransform != null) {
      return new String[] {acceptingTransform.getName()};
    }
    return null;
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
      if (!inputFiles.acceptingFilenames) {
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

    FileInputList textFileList = getFileInputList(variables);
    if (textFileList.nrOfFiles() == 0) {
      if (!inputFiles.acceptingFilenames) {
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

  public String getErrorCountField() {
    return errorCountField;
  }

  public void setErrorCountField(String errorCountField) {
    this.errorCountField = errorCountField;
  }

  public String getErrorFieldsField() {
    return errorFieldsField;
  }

  public void setErrorFieldsField(String errorFieldsField) {
    this.errorFieldsField = errorFieldsField;
  }

  public String getErrorTextField() {
    return errorTextField;
  }

  public void setErrorTextField(String errorTextField) {
    this.errorTextField = errorTextField;
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

  public boolean isErrorLineSkipped() {
    return errorLineSkipped;
  }

  public void setErrorLineSkipped(boolean errorLineSkipped) {
    this.errorLineSkipped = errorLineSkipped;
  }

  /** @param acceptingTransform The accepting Transform to set. */
  public void setAcceptingTransform(TransformMeta acceptingTransform) {
    this.acceptingTransform = acceptingTransform;
  }

  @Override
  public int getFileFormatTypeNr() {
    // calculate the file format type in advance so we can use a switch
    if (content.fileFormat.equalsIgnoreCase("DOS")) {
      return TextFileLineUtil.FILE_FORMAT_DOS;
    } else if (content.fileFormat.equalsIgnoreCase("unix")) {
      return TextFileLineUtil.FILE_FORMAT_UNIX;
    } else {
      return TextFileLineUtil.FILE_FORMAT_MIXED;
    }
  }

  public int getFileTypeNr() {
    // calculate the file type in advance CSV or Fixed?
    if (content.fileType.equalsIgnoreCase("CSV")) {
      return TextFileLineUtil.FILE_TYPE_CSV;
    } else {
      return TextFileLineUtil.FILE_TYPE_FIXED;
    }
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
      if (!inputFiles.acceptingFilenames) {

        // Replace the filename ONLY (folder or filename)
        //
        for (int i = 0; i < inputFiles.fileName.size(); i++) {
          final String fileName = inputFiles.fileName.get(i);
          if (fileName == null || fileName.isEmpty()) {
            continue;
          }

          FileObject fileObject = getFileObject(variables.resolve(fileName), variables);

          inputFiles.fileName.set(i, iResourceNaming.nameResource(fileObject, variables, Utils.isEmpty(inputFiles.fileMask.get(i))));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return errorHandling.errorIgnored && errorHandling.skipBadFiles;
  }

  @VisibleForTesting
  public void setFileNameForTest(List<String> fileName) {
    allocateFiles();
    setFileName(fileName);
  }

  protected String loadSource(
      Node filenode, Node filenamenode, int i, IHopMetadataProvider metadataProvider) {
    return XmlHandler.getNodeValue(filenamenode);
  }

  protected void saveSource(StringBuilder retVal, String source) {
    retVal.append("      ").append(XmlHandler.addTagValue("name", source));
  }

  @Override
  public String getEncoding() {
    return content.encoding;
  }

  /** @return the length */
  public String getLength() {
    return content.length;
  }

  /** @param length the length to set */
  public void setLength(String length) {
    content.length = length;
  }

  /** Required for the Data Lineage. */
  @Override
  public boolean isAcceptingFilenames() {
    return inputFiles.acceptingFilenames;
  }

  /** Required for the Data Lineage. */
  @Override
  public String getAcceptingTransformName() {
    return inputFiles.acceptingTransformName;
  }

  /** Required for the Data Lineage. */
  public TransformMeta getAcceptingTransform() {
    return acceptingTransform;
  }

  /** Required for the Data Lineage. */
  @Override
  public String getAcceptingField() {
    return inputFiles.acceptingField;
  }

  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(
        variables,
        inputFiles.fileName.toArray(new String[0]),
        inputFiles.fileMask.toArray(new String[0]),
        inputFiles.excludeFileMask.toArray(new String[0]),
        inputFiles.fileRequired.toArray(new String[0]),
        inputFiles.includeSubFolderBoolean());
  }

  public FileInputList getTextFileList(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        inputFiles.fileName.toArray(new String[0]),
        inputFiles.fileMask.toArray(new String[0]),
        inputFiles.excludeFileMask.toArray(new String[0]),
        inputFiles.fileRequired.toArray(new String[0]),
        inputFiles.includeSubFolderBoolean());
  }

  /** For testing */
  FileObject getFileObject(String vfsFileName, IVariables variables) throws HopFileException {
    return HopVfs.getFileObject(variables.resolve(vfsFileName));
  }

  @Override
  public boolean hasHeader() {
    return content == null ? false : content.header;
  }

  @Override
  public String getEscapeCharacter() {
    return content == null ? null : content.escapeCharacter;
  }

  @Override
  public String getDelimiter() {
    return content == null ? null : content.separator;
  }

  @Override
  public String getEnclosure() {
    return content == null ? null : content.enclosure;
  }

  @Override
  public FileObject getHeaderFileObject(final IVariables variables) {
    final FileInputList fileList = getFileInputList(variables);
    return fileList.nrOfFiles() == 0 ? null : fileList.getFile(0);
  }

  public static final class DateFormatLocaleConverter implements IStringObjectConverter {
    @Override
    public String getString(Object object) throws HopException {
      if (!(object instanceof Locale)) {
        throw new HopException("We only support XML serialization of Locale objects here");
      }
      try {
        return ((Locale) object).toString();
      } catch (Exception e) {
        throw new HopException("Error serializing Locale to XML", e);
      }
    }

    @Override
    public Object getObject(String xml) throws HopException {
      try {
        if (xml != null) {
          return EnvUtil.createLocale(xml);
        } else {
          return Locale.getDefault();
        }
      } catch (Exception e) {
        throw new HopException("Error serializing Locale from XML", e);
      }
    }
  }
}

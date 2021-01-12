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
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
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
import org.apache.hop.pipeline.transform.ITransformMeta;
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

import java.util.List;
import java.util.Locale;
import java.util.Map;

@SuppressWarnings("deprecation")
@Transform(
    id = "TextFileInput2",
    image = "textfileinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.TextFileInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.TextFileInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input")
@InjectionSupported(
    localizationPrefix = "TextFileInput.Injection.",
    groups = {"FILENAME_LINES", "FIELDS", "FILTERS"})
public class TextFileInputMeta
    extends BaseFileInputMeta<
        BaseFileInputAdditionalField,
        BaseFileInputFiles,
        BaseFileField,
        TextFileInput,
        TextFileInputData>
    implements ITransformMeta<TextFileInput, TextFileInputData>, ICsvInputAwareMeta {
  private static final Class<?> PKG = TextFileInputMeta.class; // For Translator

  private static final String STRING_BASE64_PREFIX = "Base64: ";

  @InjectionDeep public Content content = new Content();

  public static class Content implements Cloneable {

    /** Type of file: CSV or fixed */
    @Injection(name = "FILE_TYPE")
    public String fileType;

    /** String used to separated field (;) */
    @Injection(name = "SEPARATOR")
    public String separator;

    /** String used to enclose separated fields (") */
    @Injection(name = "ENCLOSURE")
    public String enclosure;

    /** Switch to allow breaks (CR/LF) in Enclosures */
    @Injection(name = "BREAK_IN_ENCLOSURE")
    public boolean breakInEnclosureAllowed;

    /** Escape character used to escape the enclosure String (\) */
    @Injection(name = "ESCAPE_CHAR")
    public String escapeCharacter;

    /** Flag indicating that the file contains one header line that should be skipped. */
    @Injection(name = "HEADER_PRESENT")
    public boolean header;

    /** The number of header lines, defaults to 1 */
    @Injection(name = "NR_HEADER_LINES")
    public int nrHeaderLines = -1;

    /** Flag indicating that the file contains one footer line that should be skipped. */
    @Injection(name = "HAS_FOOTER")
    public boolean footer;

    /** The number of footer lines, defaults to 1 */
    @Injection(name = "NR_FOOTER_LINES")
    public int nrFooterLines = -1;

    /** Flag indicating that a single line is wrapped onto one or more lines in the text file. */
    @Injection(name = "HAS_WRAPPED_LINES")
    public boolean lineWrapped;

    /** The number of times the line wrapped */
    @Injection(name = "NR_WRAPS")
    public int nrWraps = -1;

    /** Flag indicating that the text-file has a paged layout. */
    @Injection(name = "HAS_PAGED_LAYOUT")
    public boolean layoutPaged;

    /** The number of lines to read per page */
    @Injection(name = "NR_LINES_PER_PAGE")
    public int nrLinesPerPage = -1;

    /** The number of lines in the document header */
    @Injection(name = "NR_DOC_HEADER_LINES")
    public int nrLinesDocHeader = -1;

    /** Type of compression being used */
    @Injection(name = "COMPRESSION_TYPE")
    public String fileCompression;

    /** Flag indicating that we should skip all empty lines */
    @Injection(name = "NO_EMPTY_LINES")
    public boolean noEmptyLines;

    /** Flag indicating that we should include the filename in the output */
    @Injection(name = "INCLUDE_FILENAME")
    public boolean includeFilename;

    /** The name of the field in the output containing the filename */
    @Injection(name = "FILENAME_FIELD")
    public String filenameField;

    /** Flag indicating that a row number field should be included in the output */
    @Injection(name = "INCLUDE_ROW_NUMBER")
    public boolean includeRowNumber;

    /** The name of the field in the output containing the row number */
    @Injection(name = "ROW_NUMBER_FIELD")
    public String rowNumberField;

    /** Flag indicating row number is per file */
    @Injection(name = "ROW_NUMBER_BY_FILE")
    public boolean rowNumberByFile;

    /** The file format: DOS or UNIX or mixed */
    @Injection(name = "FILE_FORMAT")
    public String fileFormat;

    /** The encoding to use for reading: null or empty string means system default encoding */
    @Injection(name = "ENCODING")
    public String encoding;

    /** The maximum number or lines to read */
    @Injection(name = "ROW_LIMIT")
    public long rowLimit = -1;

    /**
     * Indicate whether or not we want to date fields strictly according to the format or lenient
     */
    @Injection(name = "DATE_FORMAT_LENIENT")
    public boolean dateFormatLenient;

    /** Specifies the Locale of the Date format, null means the default */
    public Locale dateFormatLocale;

    @Injection(name = "DATE_FORMAT_LOCALE")
    public void setDateFormatLocale(String locale) {
      this.dateFormatLocale = new Locale(locale);
    }

    /** Length based on bytes or characters */
    @Injection(name = "LENGTH")
    public String length;
  }

  /** The filters to use... */
  @InjectionDeep private TextFileFilter[] filter = {};

  /** The name of the field that will contain the number of errors in the row */
  @Injection(name = "ERROR_COUNT_FIELD")
  public String errorCountField;

  /**
   * The name of the field that will contain the names of the fields that generated errors,
   * separated by ,
   */
  @Injection(name = "ERROR_FIELDS_FIELD")
  public String errorFieldsField;

  /** The name of the field that will contain the error texts, separated by CR */
  @Injection(name = "ERROR_TEXT_FIELD")
  public String errorTextField;

  /** If error line are skipped, you can replay without introducing doubles. */
  @Injection(name = "ERROR_LINES_SKIPPED")
  public boolean errorLineSkipped;

  /** The transform to accept filenames from */
  private TransformMeta acceptingTransform;

  public TextFileInputMeta() {
    additionalOutputFields = new BaseFileInputAdditionalField();
    inputFiles = new BaseFileInputFiles();
    inputFields = new BaseFileField[0];
  }

  /** @return Returns the fileName. */
  public String[] getFileName() {
    return inputFiles.fileName;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String[] fileName) {
    inputFiles.fileName = fileName;
  }

  /** @return The array of filters for the metadata of this text file input transform. */
  public TextFileFilter[] getFilter() {
    return filter;
  }

  /** @param filter The array of filters to use */
  public void setFilter(TextFileFilter[] filter) {
    this.filter = filter;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      inputFiles.acceptingFilenames =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "accept_filenames"));
      inputFiles.passingThruFields =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "passing_through_fields"));
      inputFiles.acceptingField = XmlHandler.getTagValue(transformNode, "accept_field");
      inputFiles.acceptingTransformName =
          XmlHandler.getTagValue(transformNode, "accept_transform_name");

      content.separator = XmlHandler.getTagValue(transformNode, "separator");
      content.enclosure = XmlHandler.getTagValue(transformNode, "enclosure");
      content.breakInEnclosureAllowed =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "enclosure_breaks"));
      content.escapeCharacter = XmlHandler.getTagValue(transformNode, "escapechar");
      content.header = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      content.nrHeaderLines =
          Const.toInt(XmlHandler.getTagValue(transformNode, "nr_headerlines"), 1);
      content.footer = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "footer"));
      content.nrFooterLines =
          Const.toInt(XmlHandler.getTagValue(transformNode, "nr_footerlines"), 1);
      content.lineWrapped =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "line_wrapped"));
      content.nrWraps = Const.toInt(XmlHandler.getTagValue(transformNode, "nr_wraps"), 1);
      content.layoutPaged =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "layout_paged"));
      content.nrLinesPerPage =
          Const.toInt(XmlHandler.getTagValue(transformNode, "nr_lines_per_page"), 1);
      content.nrLinesDocHeader =
          Const.toInt(XmlHandler.getTagValue(transformNode, "nr_lines_doc_header"), 1);
      String addToResult = XmlHandler.getTagValue(transformNode, "add_to_result_filenames");
      if (Utils.isEmpty(addToResult)) {
        inputFiles.isaddresult = true;
      } else {
        inputFiles.isaddresult = "Y".equalsIgnoreCase(addToResult);
      }

      String nempty = XmlHandler.getTagValue(transformNode, "noempty");
      content.noEmptyLines = YES.equalsIgnoreCase(nempty) || nempty == null;
      content.includeFilename =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include"));
      content.filenameField = XmlHandler.getTagValue(transformNode, "include_field");
      content.includeRowNumber =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      content.rowNumberByFile =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownumByFile"));
      content.rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      content.fileFormat = XmlHandler.getTagValue(transformNode, "format");
      content.encoding = XmlHandler.getTagValue(transformNode, "encoding");
      content.length = XmlHandler.getTagValue(transformNode, "length");

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
        inputFiles.fileName[i] = loadSource(filenode, filenamenode, i, metadataProvider);
        inputFiles.fileMask[i] = XmlHandler.getNodeValue(filemasknode);
        inputFiles.excludeFileMask[i] = XmlHandler.getNodeValue(excludefilemasknode);
        inputFiles.fileRequired[i] = XmlHandler.getNodeValue(fileRequirednode);
        inputFiles.includeSubFolders[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

      content.fileType = XmlHandler.getTagValue(transformNode, "file", "type");
      content.fileCompression = XmlHandler.getTagValue(transformNode, "file", "compression");
      if (content.fileCompression == null) {
        content.fileCompression = "None";
        if (YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "zipped"))) {
          content.fileCompression = "Zip";
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
        BaseFileField field = new BaseFileField();

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
      content.rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);

      errorHandling.errorIgnored =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "error_ignored"));
      errorHandling.skipBadFiles =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "skip_bad_files"));
      errorHandling.fileErrorField = XmlHandler.getTagValue(transformNode, "file_error_field");
      errorHandling.fileErrorMessageField =
          XmlHandler.getTagValue(transformNode, "file_error_message_field");
      errorLineSkipped =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "error_line_skipped"));
      errorCountField = XmlHandler.getTagValue(transformNode, "error_count_field");
      errorFieldsField = XmlHandler.getTagValue(transformNode, "error_fields_field");
      errorTextField = XmlHandler.getTagValue(transformNode, "error_text_field");
      errorHandling.warningFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "bad_line_files_destination_directory");
      errorHandling.warningFilesExtension =
          XmlHandler.getTagValue(transformNode, "bad_line_files_extension");
      errorHandling.errorFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "error_line_files_destination_directory");
      errorHandling.errorFilesExtension =
          XmlHandler.getTagValue(transformNode, "error_line_files_extension");
      errorHandling.lineNumberFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "line_number_files_destination_directory");
      errorHandling.lineNumberFilesExtension =
          XmlHandler.getTagValue(transformNode, "line_number_files_extension");
      // Backward compatible

      content.dateFormatLenient =
          !NO.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "date_format_lenient"));
      String dateLocale = XmlHandler.getTagValue(transformNode, "date_format_locale");
      if (dateLocale != null) {
        content.dateFormatLocale = EnvUtil.createLocale(dateLocale);
      } else {
        content.dateFormatLocale = Locale.getDefault();
      }

      additionalOutputFields.shortFilenameField =
          XmlHandler.getTagValue(transformNode, "shortFileFieldName");
      additionalOutputFields.pathField = XmlHandler.getTagValue(transformNode, "pathFieldName");
      additionalOutputFields.hiddenField = XmlHandler.getTagValue(transformNode, "hiddenFieldName");
      additionalOutputFields.lastModificationField =
          XmlHandler.getTagValue(transformNode, "lastModificationTimeFieldName");
      additionalOutputFields.uriField = XmlHandler.getTagValue(transformNode, "uriNameFieldName");
      additionalOutputFields.rootUriField =
          XmlHandler.getTagValue(transformNode, "rootUriNameFieldName");
      additionalOutputFields.extensionField =
          XmlHandler.getTagValue(transformNode, "extensionFieldName");
      additionalOutputFields.sizeField = XmlHandler.getTagValue(transformNode, "sizeFieldName");
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  @Override
  public Object clone() {
    TextFileInputMeta retval = (TextFileInputMeta) super.clone();
    retval.inputFiles = (BaseFileInputFiles) inputFiles.clone();
    retval.inputFields = new BaseFileField[inputFields.length];
    for (int i = 0; i < inputFields.length; i++) {
      retval.inputFields[i] = (BaseFileField) inputFields[i].clone();
    }

    retval.filter = new TextFileFilter[filter.length];
    for (int i = 0; i < filter.length; i++) {
      retval.filter[i] = (TextFileFilter) filter[i].clone();
    }
    return retval;
  }

  public void allocate(int nrfiles, int nrFields, int nrfilters) {
    allocateFiles(nrfiles);

    inputFields = new BaseFileField[nrFields];
    filter = new TextFileFilter[nrfilters];
  }

  public void allocateFiles(int nrFiles) {
    inputFiles.fileName = new String[nrFiles];
    inputFiles.fileMask = new String[nrFiles];
    inputFiles.excludeFileMask = new String[nrFiles];
    inputFiles.fileRequired = new String[nrFiles];
    inputFiles.includeSubFolders = new String[nrFiles];
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
    int nrfilters = 0;

    allocate(nrfiles, nrFields, nrfilters);

    for (int i = 0; i < nrfiles; i++) {
      inputFiles.fileName[i] = "filename" + (i + 1);
      inputFiles.fileMask[i] = "";
      inputFiles.excludeFileMask[i] = "";
      inputFiles.fileRequired[i] = NO;
      inputFiles.includeSubFolders[i] = NO;
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

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(1500);

    retval
        .append("    ")
        .append(XmlHandler.addTagValue("accept_filenames", inputFiles.acceptingFilenames));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("passing_through_fields", inputFiles.passingThruFields));
    retval.append("    ").append(XmlHandler.addTagValue("accept_field", inputFiles.acceptingField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "accept_transform_name",
                (acceptingTransform != null ? acceptingTransform.getName() : "")));

    retval.append("    ").append(XmlHandler.addTagValue("separator", content.separator));
    retval.append("    ").append(XmlHandler.addTagValue("enclosure", content.enclosure));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("enclosure_breaks", content.breakInEnclosureAllowed));
    retval.append("    ").append(XmlHandler.addTagValue("escapechar", content.escapeCharacter));
    retval.append("    ").append(XmlHandler.addTagValue("header", content.header));
    retval.append("    ").append(XmlHandler.addTagValue("nr_headerlines", content.nrHeaderLines));
    retval.append("    ").append(XmlHandler.addTagValue("footer", content.footer));
    retval.append("    ").append(XmlHandler.addTagValue("nr_footerlines", content.nrFooterLines));
    retval.append("    ").append(XmlHandler.addTagValue("line_wrapped", content.lineWrapped));
    retval.append("    ").append(XmlHandler.addTagValue("nr_wraps", content.nrWraps));
    retval.append("    ").append(XmlHandler.addTagValue("layout_paged", content.layoutPaged));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("nr_lines_per_page", content.nrLinesPerPage));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("nr_lines_doc_header", content.nrLinesDocHeader));
    retval.append("    ").append(XmlHandler.addTagValue("noempty", content.noEmptyLines));
    retval.append("    ").append(XmlHandler.addTagValue("include", content.includeFilename));
    retval.append("    ").append(XmlHandler.addTagValue("include_field", content.filenameField));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", content.includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("rownumByFile", content.rowNumberByFile));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", content.rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("format", content.fileFormat));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", content.encoding));
    retval.append("    ").append(XmlHandler.addTagValue("length", content.length));
    retval.append(
        "    " + XmlHandler.addTagValue("add_to_result_filenames", inputFiles.isaddresult));

    retval.append("    <file>").append(Const.CR);
    // we need the equals by size arrays for inputFiles.fileName[i], inputFiles.fileMask[i],
    // inputFiles.fileRequired[i], inputFiles.includeSubFolders[i]
    // to prevent the ArrayIndexOutOfBoundsException
    inputFiles.normalizeAllocation(inputFiles.fileName.length);
    for (int i = 0; i < inputFiles.fileName.length; i++) {
      saveSource(retval, inputFiles.fileName[i]);
      retval.append("      ").append(XmlHandler.addTagValue("filemask", inputFiles.fileMask[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("exclude_filemask", inputFiles.excludeFileMask[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("file_required", inputFiles.fileRequired[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("include_subfolders", inputFiles.includeSubFolders[i]));
    }
    retval.append("      ").append(XmlHandler.addTagValue("type", content.fileType));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "compression",
                (content.fileCompression == null) ? "None" : content.fileCompression));
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
      BaseFileField field = inputFields[i];

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
    retval.append("    ").append(XmlHandler.addTagValue("limit", content.rowLimit));

    // ERROR HANDLING
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("error_ignored", errorHandling.errorIgnored));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("skip_bad_files", errorHandling.skipBadFiles));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("file_error_field", errorHandling.fileErrorField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "file_error_message_field", errorHandling.fileErrorMessageField));
    retval.append("    ").append(XmlHandler.addTagValue("error_line_skipped", errorLineSkipped));
    retval.append("    ").append(XmlHandler.addTagValue("error_count_field", errorCountField));
    retval.append("    ").append(XmlHandler.addTagValue("error_fields_field", errorFieldsField));
    retval.append("    ").append(XmlHandler.addTagValue("error_text_field", errorTextField));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "bad_line_files_destination_directory",
                errorHandling.warningFilesDestinationDirectory));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "bad_line_files_extension", errorHandling.warningFilesExtension));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "error_line_files_destination_directory",
                errorHandling.errorFilesDestinationDirectory));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "error_line_files_extension", errorHandling.errorFilesExtension));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "line_number_files_destination_directory",
                errorHandling.lineNumberFilesDestinationDirectory));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "line_number_files_extension", errorHandling.lineNumberFilesExtension));

    retval
        .append("    ")
        .append(XmlHandler.addTagValue("date_format_lenient", content.dateFormatLenient));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "date_format_locale",
                content.dateFormatLocale != null ? content.dateFormatLocale.toString() : null));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "shortFileFieldName", additionalOutputFields.shortFilenameField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("pathFieldName", additionalOutputFields.pathField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("hiddenFieldName", additionalOutputFields.hiddenField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "lastModificationTimeFieldName", additionalOutputFields.lastModificationField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("uriNameFieldName", additionalOutputFields.uriField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("rootUriNameFieldName", additionalOutputFields.rootUriField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("extensionFieldName", additionalOutputFields.extensionField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("sizeFieldName", additionalOutputFields.sizeField));

    return retval.toString();
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

  @Override
  public TextFileInput createTransform(
      TransformMeta transformMeta,
      TextFileInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TextFileInput(
        transformMeta, this, (TextFileInputData) data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public TextFileInputData getTransformData() {
    return new TextFileInputData();
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
        for (int i = 0; i < inputFiles.fileName.length; i++) {
          final String fileName = inputFiles.fileName[i];
          if (fileName == null || fileName.isEmpty()) {
            continue;
          }

          FileObject fileObject = getFileObject(variables.resolve(fileName), variables);

          inputFiles.fileName[i] =
              iResourceNaming.nameResource(
                  fileObject, variables, Utils.isEmpty(inputFiles.fileMask[i]));
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
  public boolean isAcceptingFilenames() {
    return inputFiles.acceptingFilenames;
  }

  /** Required for the Data Lineage. */
  public String getAcceptingTransformName() {
    return inputFiles.acceptingTransformName;
  }

  /** Required for the Data Lineage. */
  public TransformMeta getAcceptingTransform() {
    return acceptingTransform;
  }

  /** Required for the Data Lineage. */
  public String getAcceptingField() {
    return inputFiles.acceptingField;
  }

  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(
        variables,
        inputFiles.fileName,
        inputFiles.fileMask,
        inputFiles.excludeFileMask,
        inputFiles.fileRequired,
        inputFiles.includeSubFolderBoolean());
  }

  public FileInputList getTextFileList(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        inputFiles.fileName,
        inputFiles.fileMask,
        inputFiles.excludeFileMask,
        inputFiles.fileRequired,
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
}

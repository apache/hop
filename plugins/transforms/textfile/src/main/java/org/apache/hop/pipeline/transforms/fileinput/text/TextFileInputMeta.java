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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.gui.ITextFileInputField;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.common.ICsvInputAwareMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileErrorHandling;
import org.apache.hop.pipeline.transforms.file.BaseFileInput;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalFields;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;
import org.w3c.dom.Node;

@Transform(
    id = "TextFileInput2",
    image = "textfileinput.svg",
    name = "i18n::TextFileInput.Name",
    description = "i18n::TextFileInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::TextFileInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/textfileinput.html")
@Getter
@Setter
public class TextFileInputMeta
    extends BaseFileInputMeta<TextFileInput, TextFileInputData, BaseFileInput>
    implements ICsvInputAwareMeta<TextFileInputField> {
  private static final Class<?> PKG = TextFileInputMeta.class;

  // Legacy
  public static final int FILE_FORMAT_DOS = 0;
  public static final int FILE_FORMAT_UNIX = 1;
  public static final int FILE_FORMAT_MIXED = 2;

  // Legacy
  public static final int FILE_TYPE_CSV = 0;
  public static final int FILE_TYPE_FIXED = 1;

  @HopMetadataProperty(inline = true)
  private Content content = new Content();

  @Getter
  @Setter
  public static class Content {
    // Stored as file_type (not "type") so it does not clash with the transform's <type> plugin id
    /** Type of file: CSV or fixed */
    @HopMetadataProperty(
        key = "file_type",
        injectionKey = "FILE_TYPE",
        injectionKeyDescription = "TextFileInput.Injection.")
    private String fileType;

    /** Type of compression being used */
    @HopMetadataProperty(
        key = "compression",
        injectionKey = "COMPRESSION_TYPE",
        injectionKeyDescription = "TextFileInput.Injection.COMPRESSION_TYPE")
    private String fileCompression;

    /** String used to separated field (;) */
    @HopMetadataProperty(
        key = "separator",
        injectionKey = "SEPARATOR",
        injectionKeyDescription = "TextFileInput.Injection.SEPARATOR")
    private String separator;

    /** String used to enclose separated fields (") */
    @HopMetadataProperty(
        key = "enclosure",
        injectionKey = "ENCLOSURE",
        injectionKeyDescription = "TextFileInput.Injection.ENCLOSURE")
    private String enclosure;

    /** Switch to allow breaks (CR/LF) in Enclosures */
    @HopMetadataProperty(
        key = "enclosure_breaks",
        injectionKey = "BREAK_IN_ENCLOSURE",
        injectionKeyDescription = "TextFileInput.Injection.BREAK_IN_ENCLOSURE")
    private boolean breakInEnclosureAllowed;

    /** Escape character used to escape the enclosure String (\) */
    @HopMetadataProperty(
        key = "escapechar",
        injectionKey = "ESCAPE_CHAR",
        injectionKeyDescription = "TextFileInput.Injection.ESCAPE_CHAR")
    private String escapeCharacter;

    /** Flag indicating that the file contains one header line that should be skipped. */
    @HopMetadataProperty(
        key = "header",
        injectionKey = "HEADER_PRESENT",
        injectionKeyDescription = "TextFileInput.Injection.HEADER_PRESENT")
    private boolean header;

    /** Addition Flag indicating that the filename should be prepended to headers. */
    @HopMetadataProperty(
        key = "prependFileName",
        injectionKey = "PREPEND_FILENAME",
        injectionKeyDescription = "TextFileInput.Injection.PREPEND_FILENAME")
    private boolean prependFileName;

    /** The number of header lines, defaults to 1 */
    @HopMetadataProperty(
        key = "nr_headerlines",
        injectionKey = "NR_HEADER_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NR_HEADER_LINES")
    private int nrHeaderLines = -1;

    /** Flag indicating that the file contains one footer line that should be skipped. */
    @HopMetadataProperty(
        key = "footer",
        injectionKey = "HAS_FOOTER",
        injectionKeyDescription = "TextFileInput.Injection.HAS_FOOTER")
    private boolean footer;

    /** The number of footer lines, defaults to 1 */
    @HopMetadataProperty(
        key = "nr_footerlines",
        injectionKey = "NR_FOOTER_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NR_FOOTER_LINES")
    private int nrFooterLines = -1;

    /** Flag indicating that a single line is wrapped onto one or more lines in the text file. */
    @HopMetadataProperty(
        key = "line_wrapped",
        injectionKey = "HAS_WRAPPED_LINES",
        injectionKeyDescription = "TextFileInput.Injection.HAS_WRAPPED_LINES")
    private boolean lineWrapped;

    /** The number of times the line wrapped */
    @HopMetadataProperty(
        key = "nr_wraps",
        injectionKey = "NR_WRAPS",
        injectionKeyDescription = "TextFileInput.Injection.NR_WRAPS")
    private int nrWraps = -1;

    /** Flag indicating that the text-file has a paged layout. */
    @HopMetadataProperty(
        key = "layout_paged",
        injectionKey = "HAS_PAGED_LAYOUT",
        injectionKeyDescription = "TextFileInput.Injection.HAS_PAGED_LAYOUT")
    private boolean layoutPaged;

    /** The number of lines to read per page */
    @HopMetadataProperty(
        key = "nr_lines_per_page",
        injectionKey = "NR_LINES_PER_PAGE",
        injectionKeyDescription = "TextFileInput.Injection.NR_LINES_PER_PAGE")
    private int nrLinesPerPage = -1;

    /** The number of lines in the document header */
    @HopMetadataProperty(
        key = "nr_lines_doc_header",
        injectionKey = "NR_DOC_HEADER_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NR_DOC_HEADER_LINES")
    private int nrLinesDocHeader = -1;

    /** Flag indicating that we should skip all empty lines */
    @HopMetadataProperty(
        key = "noempty",
        injectionKey = "NO_EMPTY_LINES",
        injectionKeyDescription = "TextFileInput.Injection.NO_EMPTY_LINES")
    private boolean noEmptyLines;

    /** Flag indicating that we should include the filename in the output */
    @HopMetadataProperty(
        key = "include",
        injectionKey = "INCLUDE_FILENAME",
        injectionKeyDescription = "TextFileInput.Injection.INCLUDE_FILENAME")
    private boolean includeFilename;

    /** The name of the field in the output containing the filename */
    @HopMetadataProperty(
        key = "include_field",
        injectionKey = "FILENAME_FIELD",
        injectionKeyDescription = "TextFileInput.Injection.FILENAME_FIELD")
    private String filenameField;

    /** Flag indicating that a row number field should be included in the output */
    @HopMetadataProperty(
        key = "rownum",
        injectionKey = "INCLUDE_ROW_NUMBER",
        injectionKeyDescription = "TextFileInput.Injection.INCLUDE_ROW_NUMBER")
    private boolean includeRowNumber;

    /** The name of the field in the output containing the row number */
    @HopMetadataProperty(
        key = "rownum_field",
        injectionKey = "ROW_NUMBER_FIELD",
        injectionKeyDescription = "TextFileInput.Injection.ROW_NUMBER_FIELD")
    private String rowNumberField;

    /** Flag indicating row number is per file */
    @HopMetadataProperty(
        key = "rownumByFile",
        injectionKey = "ROW_NUMBER_BY_FILE",
        injectionKeyDescription = "TextFileInput.Injection.ROW_NUMBER_BY_FILE")
    private boolean rowNumberByFile;

    /** The file format: DOS or UNIX or mixed */
    @HopMetadataProperty(
        key = "format",
        injectionKey = "FILE_FORMAT",
        injectionKeyDescription = "TextFileInput.Injection.FILE_FORMAT")
    private String fileFormat;

    /** The encoding to use for reading: null or empty string means system default encoding */
    @HopMetadataProperty(
        key = "encoding",
        injectionKey = "ENCODING",
        injectionKeyDescription = "TextFileInput.Injection.ENCODING")
    private String encoding;

    /** The maximum number or lines to read */
    @HopMetadataProperty(
        key = "limit",
        injectionKey = "ROW_LIMIT",
        injectionKeyDescription = "TextFileInput.Injection.ROW_LIMIT")
    private long rowLimit = -1;

    /**
     * Indicate whether or not we want to date fields strictly according to the format or lenient
     */
    @HopMetadataProperty(
        key = "date_format_lenient",
        injectionKey = "DATE_FORMAT_LENIENT",
        injectionKeyDescription = "TextFileInput.Injection.DATE_FORMAT_LENIENT")
    private boolean dateFormatLenient;

    /** Specifies the Locale of the Date format, null means the default */
    @HopMetadataProperty(
        key = "date_format_locale",
        injectionKey = "DATE_FORMAT_LOCALE",
        injectionKeyDescription = "TextFileInput.Injection.DATE_FORMAT_LOCALE")
    private String dateFormatLocale;

    /** Length based on bytes or characters */
    @HopMetadataProperty(
        key = "length",
        injectionKey = "LENGTH",
        injectionKeyDescription = "TextFileInput.Injection.LENGTH")
    private String length;

    public Content() {}

    public Content(Content c) {
      this();
      this.breakInEnclosureAllowed = c.breakInEnclosureAllowed;
      this.dateFormatLenient = c.dateFormatLenient;
      this.dateFormatLocale = c.dateFormatLocale;
      this.enclosure = c.enclosure;
      this.encoding = c.encoding;
      this.escapeCharacter = c.escapeCharacter;
      this.fileFormat = c.fileFormat;
      this.filenameField = c.filenameField;
      this.fileType = c.fileType;
      this.fileCompression = c.fileCompression;
      this.footer = c.footer;
      this.header = c.header;
      this.includeFilename = c.includeFilename;
      this.includeRowNumber = c.includeRowNumber;
      this.layoutPaged = c.layoutPaged;
      this.length = c.length;
      this.lineWrapped = c.lineWrapped;
      this.noEmptyLines = c.noEmptyLines;
      this.nrFooterLines = c.nrFooterLines;
      this.nrHeaderLines = c.nrHeaderLines;
      this.nrLinesDocHeader = c.nrLinesDocHeader;
      this.nrLinesPerPage = c.nrLinesPerPage;
      this.nrWraps = c.nrWraps;
      this.prependFileName = c.prependFileName;
      this.rowLimit = c.rowLimit;
      this.rowNumberByFile = c.rowNumberByFile;
      this.rowNumberField = c.rowNumberField;
      this.separator = c.separator;
    }
  }

  /** The filters to use... */
  @HopMetadataProperty(
      key = "filter",
      groupKey = "filters",
      injectionKey = "FILTER",
      injectionGroupKey = "FILTERS",
      injectionKeyDescription = "TextFileInput.Injection.FILTER",
      injectionGroupDescription = "TextFileInput.Injection.FILTERS")
  private List<TextFileFilter> filters;

  /** The name of the field that will contain the number of errors in the row */
  @HopMetadataProperty(
      key = "error_count_field",
      injectionKey = "ERROR_COUNT_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_COUNT_FIELD")
  private String errorCountField;

  /**
   * The name of the field that will contain the names of the fields that generated errors,
   * separated by ,
   */
  @HopMetadataProperty(
      key = "error_fields_field",
      injectionKey = "ERROR_FIELDS_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.")
  private String errorFieldsField;

  /** The name of the field that will contain the error texts, separated by CR */
  @HopMetadataProperty(
      key = "error_text_field",
      injectionKey = "ERROR_TEXT_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_TEXT_FIELD")
  private String errorTextField;

  /** If error line are skipped, you can replay without introducing doubles. */
  @HopMetadataProperty(
      key = "error_line_skipped",
      injectionKey = "ERROR_LINES_SKIPPED",
      injectionKeyDescription = "TextFileInput.Injection.ERROR_LINES_SKIPPED")
  private boolean errorLineSkipped;

  /** Reference to schema definition. If any */
  @HopMetadataProperty(
      key = "schema_definition",
      injectionKey = "SCHEMA_DEFINITION",
      injectionKeyDescription = "TextFileInput.Injection.SCHEMA_DEFINITION",
      hopMetadataPropertyType = HopMetadataPropertyType.STATIC_SCHEMA_DEFINITION)
  private String schemaDefinition;

  /** Reference to ignore fields tab */
  @HopMetadataProperty(
      key = "ignore_fields",
      injectionKey = "IGNORE_FIELDS",
      injectionKeyDescription = "TextFileInput.Injection.IGNORE_FIELDS")
  private boolean ignoreFields;

  @HopMetadataProperty(inline = true)
  protected BaseFileInputAdditionalFields additionalOutputFields;

  @HopMetadataProperty(
      key = "file",
      inline = true,
      injectionKey = "FILE",
      injectionKeyDescription = "TextFileInput.Injection.FILE")
  protected BaseFileInput fileInput;

  @HopMetadataProperty(inline = true)
  protected BaseFileErrorHandling errorHandling;

  /** The fields to import... */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "TextFileInput.Injection.FIELD",
      injectionGroupDescription = "TextFileInput.Injection.FIELDS")
  private List<TextFileInputField> inputFields;

  public TextFileInputMeta() {
    super();
    filters = new ArrayList<>();
    inputFields = new ArrayList<>();
    errorHandling = new BaseFileErrorHandling();
    additionalOutputFields = new BaseFileInputAdditionalFields();
    additionalOutputFields.setShortFilenameField(null);
    additionalOutputFields.setPathField(null);
    additionalOutputFields.setHiddenField(null);
    additionalOutputFields.setLastModificationField(null);
    additionalOutputFields.setUriField(null);
    additionalOutputFields.setRootUriField(null);
    additionalOutputFields.setExtensionField(null);
    additionalOutputFields.setSizeField(null);
    fileInput = new BaseFileInput();
    fileInput.setAddingResult(true);

    content.separator = ";";
    content.enclosure = "\"";
    content.escapeCharacter = "\\";
    content.breakInEnclosureAllowed = false;
    content.header = true;
    content.prependFileName = false;
    content.nrHeaderLines = 1;
    content.footer = false;
    content.nrFooterLines = 1;
    content.lineWrapped = false;
    content.nrWraps = 1;
    content.layoutPaged = false;
    content.nrLinesPerPage = 80;
    content.nrLinesDocHeader = 0;
    content.noEmptyLines = true;
    content.fileFormat = "mixed";
    content.includeFilename = false;
    content.filenameField = "";
    content.includeRowNumber = false;
    content.rowNumberField = "";
    content.dateFormatLenient = true;
    content.rowNumberByFile = false;
    content.fileCompression = "None";
    content.fileType = "CSV";

    errorHandling.setErrorIgnored(false);
    errorHandling.setSkipBadFiles(false);
    errorLineSkipped = false;
    errorHandling.setWarningFilesDestinationDirectory(null);
    errorHandling.setWarningFilesExtension("warning");
    errorHandling.setErrorFilesDestinationDirectory(null);
    errorHandling.setErrorFilesExtension("error");
    errorHandling.setLineNumberFilesDestinationDirectory(null);
    errorHandling.setLineNumberFilesExtension("line");
    ignoreFields = false;

    content.dateFormatLocale = Locale.getDefault().toString();

    content.rowLimit = 0L;
  }

  public TextFileInputMeta(TextFileInputMeta m) {
    this();
    this.content = new Content(m.content);
    this.errorCountField = m.errorCountField;
    this.errorFieldsField = m.errorFieldsField;
    this.errorLineSkipped = m.errorLineSkipped;
    this.errorTextField = m.errorTextField;
    this.ignoreFields = m.ignoreFields;
    this.schemaDefinition = m.schemaDefinition;
    this.additionalOutputFields = new BaseFileInputAdditionalFields(m.additionalOutputFields);
    this.fileInput = new BaseFileInput(m.fileInput);
    m.filters.forEach(filter -> this.filters.add(new TextFileFilter(filter)));
    m.inputFields.forEach(f -> this.inputFields.add(new TextFileInputField(f.clone())));
  }

  @Override
  public TextFileInputMeta clone() {
    return new TextFileInputMeta(this);
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
    if (!fileInput.isPassingThruFields()) {
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

    if (ignoreFields) {
      try {
        SchemaDefinition loadedSchemaDefinition =
            (new SchemaDefinitionUtil())
                .loadSchemaDefinition(metadataProvider, getSchemaDefinition());
        if (loadedSchemaDefinition != null) {
          IRowMeta r = loadedSchemaDefinition.getRowMeta();
          if (r != null) {
            for (int i = 0; i < r.size(); i++) {
              row.addValueMeta(r.getValueMeta(i));
            }
          }
        }
      } catch (HopTransformException | HopPluginException e) {
        // ignore any errors here.
      }
    } else {
      for (ITextFileInputField field : inputFields) {
        int type = field.getType();
        if (type == IValueMeta.TYPE_NONE) {
          type = IValueMeta.TYPE_STRING;
        }

        FileInputList fileInputList =
            FileInputList.createFileList(variables, fileInput.getInputFiles());
        String fileNameToPrepend = null;
        if (fileInputList.nrOfFiles() > 0) {
          fileNameToPrepend = fileInputList.getFile(0).getName().getURI();
        } else if (!fileInputList.getNonExistentFiles().isEmpty()) {
          fileNameToPrepend = fileInputList.getNonExistentFiles().get(0).getName().getURI();
        }
        // When file list is empty (e.g. not required and missing), use fictional path for prepend
        if (content.prependFileName
            && fileNameToPrepend == null
            && !fileInput.getInputFiles().isEmpty()) {
          String firstPath = variables.resolve(fileInput.getInputFiles().getFirst().getFileName());
          if (!Utils.isEmpty(firstPath)) {
            fileNameToPrepend = firstPath;
          }
        }

        try {
          String fieldName =
              content.prependFileName && fileNameToPrepend != null
                  ? FileNameUtils.getBaseName(fileNameToPrepend) + "_" + field.getName()
                  : field.getName();
          IValueMeta v = ValueMetaFactory.createValueMeta(fieldName, type);
          v.setLength(field.getLength());
          v.setPrecision(field.getPrecision());
          v.setOrigin(name);
          v.setConversionMask(field.getFormat());
          v.setDecimalSymbol(field.getDecimalSymbol());
          v.setGroupingSymbol(field.getGroupSymbol());
          v.setCurrencySymbol(field.getCurrencySymbol());
          v.setDateFormatLenient(content.dateFormatLenient);
          v.setDateFormatLocale(Locale.of(content.dateFormatLocale));
          v.setTrimType(field.getTrimType());

          row.addValueMeta(v);
        } catch (Exception e) {
          throw new HopTransformException(e);
        }
      }
    }
    if (errorHandling.isErrorIgnored()) {
      if (!Utils.isEmpty(errorCountField)) {
        IValueMeta v = new ValueMetaInteger(errorCountField);
        v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
        v.setOrigin(name);
        row.addValueMeta(v);
      }
      if (!Utils.isEmpty(errorFieldsField)) {
        IValueMeta v = new ValueMetaString(errorFieldsField);
        v.setOrigin(name);
        row.addValueMeta(v);
      }
      if (!Utils.isEmpty(errorTextField)) {
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

    if (StringUtils.isNotBlank(additionalOutputFields.getShortFilenameField())) {
      IValueMeta v =
          new ValueMetaString(variables.resolve(additionalOutputFields.getShortFilenameField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.getExtensionField())) {
      IValueMeta v =
          new ValueMetaString(variables.resolve(additionalOutputFields.getExtensionField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.getPathField())) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.getPathField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.getSizeField())) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.getSizeField()));
      v.setOrigin(name);
      v.setLength(9);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.getHiddenField())) {
      IValueMeta v =
          new ValueMetaBoolean(variables.resolve(additionalOutputFields.getHiddenField()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (StringUtils.isNotBlank(additionalOutputFields.getLastModificationField())) {
      IValueMeta v =
          new ValueMetaDate(variables.resolve(additionalOutputFields.getLastModificationField()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotBlank(additionalOutputFields.getUriField())) {
      IValueMeta v = new ValueMetaString(variables.resolve(additionalOutputFields.getUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (StringUtils.isNotBlank(additionalOutputFields.getRootUriField())) {
      IValueMeta v = new ValueMetaString(additionalOutputFields.getRootUriField());
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public boolean isIncludeFilename() {
    return false;
  }

  @Override
  public boolean isIncludeRowNumber() {
    return false;
  }

  public String[] getInfoTransforms() {
    if (fileInput.isAcceptingFilenames()
        && StringUtils.isNotEmpty(fileInput.getAcceptingTransformName())) {
      return new String[] {fileInput.getAcceptingTransformName()};
    }
    return new String[0];
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
      if (!this.getFileInput().isAcceptingFilenames()) {
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
      if (!this.getFileInput().isAcceptingFilenames()) {
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
   * @param definitions The definitions to use
   * @param iResourceNaming The resource naming interface
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
      if (!fileInput.isAcceptingFilenames()) {

        // Replace the filename ONLY (folder or filename)
        //
        for (InputFile inputFile : fileInput.getInputFiles()) {
          if (Utils.isEmpty(inputFile.getFileName())) {
            continue;
          }

          FileObject fileObject =
              getFileObject(variables.resolve(inputFile.getFileName()), variables);

          inputFile.setFileName(
              iResourceNaming.nameResource(
                  fileObject, variables, Utils.isEmpty(inputFile.getFileMask())));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return errorHandling.isErrorIgnored() && errorHandling.isSkipBadFiles();
  }

  @Override
  public String getEncoding() {
    return content.encoding;
  }

  /**
   * @return the length
   */
  public String getLength() {
    return content.length;
  }

  /**
   * @param length the length to set
   */
  public void setLength(String length) {
    content.length = length;
  }

  /** Required for the Data Lineage. */
  @Override
  public boolean isAcceptingFilenames() {
    return fileInput.isAcceptingFilenames();
  }

  /** Required for the Data Lineage. */
  @Override
  public String getAcceptingTransformName() {
    return fileInput.getAcceptingTransformName();
  }

  /** Required for the Data Lineage. */
  @Override
  public String getAcceptingField() {
    return fileInput.getAcceptingField();
  }

  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(variables, fileInput.getInputFiles());
  }

  public FileInputList getTextFileList(IVariables variables) {
    return FileInputList.createFileList(variables, fileInput.getInputFiles());
  }

  /** For testing */
  FileObject getFileObject(String vfsFileName, IVariables variables) throws HopFileException {
    return HopVfs.getFileObject(variables.resolve(vfsFileName), variables);
  }

  @Override
  public boolean hasHeader() {
    return content != null && content.header;
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
  public boolean isBreakInEnclosureAllowed() {
    return content.breakInEnclosureAllowed;
  }

  @Override
  public FileObject getHeaderFileObject(final IVariables variables) {
    final FileInputList fileList = getFileInputList(variables);
    return fileList.nrOfFiles() == 0 ? null : fileList.getFile(0);
  }

  @Override
  public int getNrHeaderLines() {
    return content.nrHeaderLines;
  }

  @Override
  public String getFileType() {
    return content.fileType;
  }

  @Override
  public List<InputFile> getInputFiles() {
    return fileInput.getInputFiles();
  }

  /** Convert inline file block contents from old XML */
  @Override
  public void convertLegacyXml(Node node) {
    convertLegacyXml(getFileInput().getInputFiles(), node);

    // Also move compression and type up one level
    //
    Node fileNode = XmlHandler.getSubNode(node, "file");
    if (fileNode != null) {
      Node typeNode = XmlHandler.getSubNode(fileNode, "type");
      if (typeNode != null) {
        content.fileType = XmlHandler.getNodeValue(typeNode);
      }
      Node compressionNode = XmlHandler.getSubNode(fileNode, "compression");
      if (compressionNode != null) {
        content.fileCompression = XmlHandler.getNodeValue(compressionNode);
      }
    }
  }
}

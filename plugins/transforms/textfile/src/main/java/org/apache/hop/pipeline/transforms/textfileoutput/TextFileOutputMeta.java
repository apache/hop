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
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
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
@Getter
@Setter
public class TextFileOutputMeta extends BaseTransformMeta<TextFileOutput, TextFileOutputData> {

  private static final Class<?> PKG = TextFileOutputMeta.class;

  protected static final int FILE_COMPRESSION_TYPE_NONE = 0;

  protected static final int FILE_COMPRESSION_TYPE_ZIP = 1;

  protected static final String[] fileCompressionTypeCodes = new String[] {"None", "Zip"};

  protected static final String[] formatMapperLineTerminator =
      new String[] {"DOS", "UNIX", "CR", "None"};

  @Getter
  @Setter
  public static class FileSettings {
    /** The base name of the output file */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FILENAME",
        injectionKeyDescription = "TextFileOutput.Injection.FILENAME")
    protected String fileName;

    /** Flag : Do not open new file when pipeline start */
    @HopMetadataProperty(
        key = "do_not_open_new_file_init",
        injectionKey = "DO_NOT_CREATE_FILE_AT_STARTUP",
        injectionKeyDescription = "TextFileOutput.Injection.DO_NOT_CREATE_FILE_AT_STARTUP")
    private boolean doNotOpenNewFileInit;

    /** The file extention in case of a generated filename */
    @HopMetadataProperty(
        key = "extension",
        injectionKey = "EXTENSION",
        injectionKeyDescription = "TextFileOutput.Injection.EXTENSION")
    protected String extension;

    /** Flag to indicate then we want to append to the end of an existing file (if it exists) */
    @HopMetadataProperty(
        key = "append",
        injectionKey = "APPEND",
        injectionKeyDescription = "TextFileOutput.Injection.APPEND")
    private boolean fileAppended;

    /** Flag: add the transformnr in the filename */
    @HopMetadataProperty(
        key = "split",
        injectionKey = "INC_TRANSFORMNR_IN_FILENAME",
        injectionKeyDescription = "TextFileOutput.Injection.INC_TRANSFORMNR_IN_FILENAME")
    protected boolean transformNrInFilename;

    /** Flag: add the partition number in the filename */
    @HopMetadataProperty(
        key = "haspartno",
        injectionKey = "INC_PARTNR_IN_FILENAME",
        injectionKeyDescription = "TextFileOutput.Injection.INC_PARTNR_IN_FILENAME")
    protected boolean partNrInFilename;

    /** Flag: add the date in the filename */
    @HopMetadataProperty(
        key = "add_date",
        injectionKey = "INC_DATE_IN_FILENAME",
        injectionKeyDescription = "TextFileOutput.Injection.INC_DATE_IN_FILENAME")
    protected boolean dateInFilename;

    /** Flag: add the time in the filename */
    @HopMetadataProperty(
        key = "add_time",
        injectionKey = "INC_TIME_IN_FILENAME",
        injectionKeyDescription = "TextFileOutput.Injection.INC_TIME_IN_FILENAME")
    protected boolean timeInFilename;

    /** Flag : Do not open new file when pipeline start */
    @HopMetadataProperty(
        key = "SpecifyFormat",
        injectionKey = "SPECIFY_DATE_FORMAT",
        injectionKeyDescription = "TextFileOutput.Injection.SPECIFY_DATE_FORMAT")
    private boolean specifyingFormat;

    /** The date format appended to the file name */
    @HopMetadataProperty(
        key = "date_time_format",
        injectionKey = "DATE_FORMAT",
        injectionKeyDescription = "TextFileOutput.Injection.DATE_FORMAT")
    private String dateTimeFormat;

    /** Flag: add the filenames to result filenames */
    @HopMetadataProperty(
        key = "add_to_result_filenames",
        defaultBoolean = true,
        injectionKey = "ADD_TO_RESULT",
        injectionKeyDescription = "TextFileOutput.Injection.ADD_TO_RESULT")
    private boolean addToResultFiles;

    /** Flag: pad fields to their specified length */
    @HopMetadataProperty(
        key = "pad",
        injectionKey = "RIGHT_PAD_FIELDS",
        injectionKeyDescription = "TextFileOutput.Injection.RIGHT_PAD_FIELDS")
    private boolean padded;

    /** Flag: Fast dump data without field formatting */
    @HopMetadataProperty(
        key = "fast_dump",
        injectionKey = "FAST_DATA_DUMP",
        injectionKeyDescription = "TextFileOutput.Injection.FAST_DATA_DUMP")
    private boolean fastDump;

    /**
     * if this value is larger than 0, the text file is split up into parts of this number of lines
     */
    @HopMetadataProperty(
        key = "splitevery",
        injectionKey = "SPLIT_EVERY",
        injectionKeyDescription = "TextFileOutput.Injection.SPLIT_EVERY")
    private String splitEveryRows;

    public FileSettings() {
      setSpecifyingFormat(false);
      setDateTimeFormat(null);
      fileName = "";
      doNotOpenNewFileInit = true;
      extension = "";
      transformNrInFilename = false;
      partNrInFilename = false;
      dateInFilename = false;
      timeInFilename = false;
      padded = false;
      fastDump = false;
      addToResultFiles = true;
      fileAppended = false;
    }

    public FileSettings(FileSettings f) {
      this();
      this.addToResultFiles = f.addToResultFiles;
      this.dateInFilename = f.dateInFilename;
      this.dateTimeFormat = f.dateTimeFormat;
      this.doNotOpenNewFileInit = f.doNotOpenNewFileInit;
      this.extension = f.extension;
      this.fastDump = f.fastDump;
      this.fileAppended = f.fileAppended;
      this.fileName = f.fileName;
      this.padded = f.padded;
      this.partNrInFilename = f.partNrInFilename;
      this.specifyingFormat = f.specifyingFormat;
      this.splitEveryRows = f.splitEveryRows;
      this.timeInFilename = f.timeInFilename;
      this.transformNrInFilename = f.transformNrInFilename;
    }
  }

  /** The file compression: None, Zip or Gzip */
  @HopMetadataProperty(
      key = "compression",
      injectionKey = "COMPRESSION",
      injectionKeyDescription = "TextFileOutput.Injection.COMPRESSION")
  private String fileCompression;

  /** Flag: create parent folder, default to true */
  @HopMetadataProperty(
      key = "create_parent_folder",
      defaultBoolean = true,
      injectionKey = "CREATE_PARENT_FOLDER",
      injectionKeyDescription = "TextFileOutput.Injection.CREATE_PARENT_FOLDER")
  private boolean createParentFolder;

  /** The separator to choose for the CSV file */
  @HopMetadataProperty(
      key = "separator",
      injectionKey = "SEPARATOR",
      injectionKeyDescription = "TextFileOutput.Injection.SEPARATOR")
  private String separator;

  /** The enclosure to use in case the separator is part of a field's value */
  @HopMetadataProperty(
      key = "enclosure",
      injectionKey = "ENCLOSURE",
      injectionKeyDescription = "TextFileOutput.Injection.ENCLOSURE")
  private String enclosure;

  /**
   * Setting to allow the enclosure to be always surrounding a String value, even when there is no
   * separator inside
   */
  @HopMetadataProperty(
      key = "enclosure_forced",
      injectionKey = "FORCE_ENCLOSURE",
      injectionKeyDescription = "TextFileOutput.Injection.FORCE_ENCLOSURE")
  private boolean enclosureForced;

  /**
   * Setting to allow for backwards compatibility where the enclosure did not show up at all if
   * Force Enclosure was not checked
   */
  @HopMetadataProperty(
      key = "enclosure_fix_disabled",
      defaultBoolean = true,
      injectionKey = "DISABLE_ENCLOSURE_FIX",
      injectionKeyDescription = "TextFileOutput.Injection.DISABLE_ENCLOSURE_FIX")
  private boolean enclosureFixDisabled;

  /** Add a header at the top of the file? */
  @HopMetadataProperty(
      key = "header",
      injectionKey = "HEADER",
      injectionKeyDescription = "TextFileOutput.Injection.HEADER")
  private boolean headerEnabled;

  /** Add a footer at the bottom of the file? */
  @HopMetadataProperty(
      key = "footer",
      injectionKey = "FOOTER",
      injectionKeyDescription = "TextFileOutput.Injection.FOOTER")
  private boolean footerEnabled;

  /**
   * The file format: DOS or Unix It could be injected using the key "FORMAT" see the setter {@link
   * TextFileOutputMeta#setFileFormat(String)}.
   */
  @HopMetadataProperty(
      key = "format",
      injectionKey = "FORMAT",
      injectionKeyDescription = "TextFileOutput.Injection.FORMAT")
  private String fileFormat;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "TextFileOutput.Injection.ENCODING")
  private String encoding;

  /**
   * The string to use for append to end line of the whole file: null or empty string means no line
   * needed
   */
  @HopMetadataProperty(
      key = "endedLine",
      injectionKey = "ADD_ENDING_LINE",
      injectionKeyDescription = "TextFileOutput.Injection.ADD_ENDING_LINE")
  private String endedLine;

  /* Specification if file name is in field */
  @HopMetadataProperty(
      key = "fileNameInField",
      injectionKey = "FILENAME_IN_FIELD",
      injectionKeyDescription = "TextFileOutput.Injection.FILENAME_IN_FIELD")
  private boolean fileNameInField;

  @HopMetadataProperty(
      key = "fileNameField",
      injectionKey = "FILENAME_FIELD",
      injectionKeyDescription = "TextFileOutput.Injection.FILENAME_FIELD")
  private String fileNameField;

  @HopMetadataProperty(
      key = "schema_definition",
      injectionKey = "SCHEMA_DEFINITION",
      injectionKeyDescription = "TextFileOutput.Injection.SCHEMA_DEFINITION",
      hopMetadataPropertyType = HopMetadataPropertyType.STATIC_SCHEMA_DEFINITION)
  private String schemaDefinition;

  @HopMetadataProperty(
      key = "ignore_fields",
      injectionKey = "IGNORE_FIELDS",
      injectionKeyDescription = "TextFileOutput.Injection.IGNORE_FIELDS")
  public boolean ignoreFields;

  @HopMetadataProperty(key = "file")
  private FileSettings fileSettings;

  /** The output fields */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionKeyDescription = "TextFileOutput.Injection.OUTPUT_FIELD",
      injectionGroupDescription = "TextFileOutput.Injection.OUTPUT_FIELDS")
  private List<TextFileField> outputFields;

  public TextFileOutputMeta() {
    super();
    fileSettings = new FileSettings();
    outputFields = new ArrayList<>();
    createParentFolder = true;
    separator = ";";
    enclosure = "\"";
    enclosureForced = false;
    enclosureFixDisabled = false;
    headerEnabled = true;
    footerEnabled = false;
    fileFormat = "DOS";
    setFileCompression(fileCompressionTypeCodes[FILE_COMPRESSION_TYPE_NONE]);
  }

  public TextFileOutputMeta(TextFileOutputMeta m) {
    this();
    this.createParentFolder = m.createParentFolder;
    this.enclosure = m.enclosure;
    this.enclosureFixDisabled = m.enclosureFixDisabled;
    this.enclosureForced = m.enclosureForced;
    this.encoding = m.encoding;
    this.endedLine = m.endedLine;
    this.fileCompression = m.fileCompression;
    this.fileFormat = m.fileFormat;
    this.fileNameField = m.fileNameField;
    this.fileNameInField = m.fileNameInField;
    this.footerEnabled = m.footerEnabled;
    this.headerEnabled = m.headerEnabled;
    this.ignoreFields = m.ignoreFields;
    this.schemaDefinition = m.schemaDefinition;
    this.separator = m.separator;
    this.fileSettings = new FileSettings(m.fileSettings);
    m.outputFields.forEach(
        field -> {
          this.outputFields.add(new TextFileField(field));
        });
  }

  @Override
  public Object clone() {
    return new TextFileOutputMeta(this);
  }

  /**
   * @param varSpace for variable substitution
   * @return At how many rows to split into another file.
   */
  public int getSplitEvery(IVariables varSpace) {
    return Const.toInt(
        varSpace == null
            ? fileSettings.splitEveryRows
            : varSpace.resolve(fileSettings.splitEveryRows),
        0);
  }

  /**
   * @return <i>1</i> if <i>isFooterEnabled()</i> and <i>0</i> otherwise
   */
  public int getFooterShift() {
    return isFooterEnabled() ? 1 : 0;
  }

  public String getNewLine() {
    return getNewLine(fileFormat);
  }

  public String getNewLine(String fileFormat) {
    String nl = System.lineSeparator();

    if (fileFormat != null) {
      if (fileFormat.equalsIgnoreCase("DOS")) {
        nl = "\r\n";
      } else if (fileFormat.equalsIgnoreCase("UNIX")) {
        nl = "\n";
      } else if (fileFormat.equalsIgnoreCase("CR")) {
        nl = "\r";
      } else if (fileFormat.equalsIgnoreCase("None")) {
        nl = "";
      }
    }

    return nl;
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
    for (TextFileField field : outputFields) {
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
        v.setOutputPaddingEnabled(getFileSettings().isPadded());
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
      for (TextFileField outputField : outputFields) {
        int idx = prev.indexOfValue(outputField.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + outputField.getName() + Const.CR;
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
   * @param definitions The definitions to use
   * @param iResourceNaming The resource naming to apply
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
      if (!fileNameInField && !Utils.isEmpty(fileSettings.fileName)) {
        FileObject fileObject =
            HopVfs.getFileObject(variables.resolve(fileSettings.fileName), variables);
        fileSettings.fileName = iResourceNaming.nameResource(fileObject, variables, true);
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public String[] getFiles(final IVariables variables) {
    return getFiles(variables, true);
  }

  private String[] getFiles(final IVariables variables, final boolean showSamples) {
    String realFileName = variables.resolve(fileSettings.fileName);
    String realExtension = variables.resolve(fileSettings.extension);
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

      if (fileSettings.isTransformNrInFilename()) {
        copies = 3;
      }

      if (fileSettings.isPartNrInFilename()) {
        parts = 3;
      }

      if (Const.toInt(fileSettings.getSplitEveryRows(), 0) != 0) {
        splits = 3;
      }

      int nr = copies * parts * splits;
      if (nr > 1) {
        nr++;
      }

      String[] strings = new String[nr];

      int i = 0;
      for (int transform = 0; transform < copies; transform++) {
        for (int part = 0; part < parts; part++) {
          for (int split = 0; split < splits; split++) {
            strings[i] =
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
        strings[i] = "...";
      }

      return strings;
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
      final String transformNr,
      final String partNr,
      final String splitNr,
      final boolean beamContext,
      final String transformId,
      final int bundleNr,
      final Date date,
      final boolean ziparchive,
      final boolean showSamples) {
    return buildFilename(
        realFileName,
        realExtension,
        transformNr,
        partNr,
        splitNr,
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
      final String transformNr,
      final String partNr,
      final String splitNr,
      final boolean beamContext,
      final String transformId,
      final int bundleNr,
      final Date date,
      final boolean zipArchive,
      final boolean showSamples,
      final TextFileOutputMeta meta) {
    return buildFilename(
        null,
        realFileName,
        realExtension,
        transformNr,
        partNr,
        splitNr,
        beamContext,
        transformId,
        bundleNr,
        date,
        zipArchive,
        showSamples,
        meta);
  }

  protected String buildFilename(
      final IVariables variables,
      final String realFileName,
      final String realExtension,
      final String transformNr,
      final String partNr,
      final String splitNr,
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

    Date now = date == null ? new Date() : date;

    if (meta.fileSettings.isSpecifyingFormat()
        && !Utils.isEmpty(meta.fileSettings.getDateTimeFormat())) {
      daf.applyPattern(meta.fileSettings.getDateTimeFormat());
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (meta.fileSettings.isDateInFilename()) {
        if (showSamples) {
          daf.applyPattern("yyyMMdd");
          String d = daf.format(now);
          retval += "_" + d;
        } else {
          retval += "_<yyyMMdd>";
        }
      }
      if (meta.fileSettings.isTimeInFilename()) {
        if (showSamples) {
          daf.applyPattern("HHmmss");
          String t = daf.format(now);
          retval += "_" + t;
        } else {
          retval += "_<HHmmss>";
        }
      }
    }
    if (meta.fileSettings.isTransformNrInFilename()) {
      retval += "_" + transformNr;
    }
    if (meta.fileSettings.isPartNrInFilename()) {
      retval += "_" + partNr;
    }
    if (meta.getSplitEvery(variables) > 0) {
      retval += "_" + splitNr;
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

  @Override
  public void convertLegacyXml(Node node) throws HopException {
    // See if there's an older "file/extention" node...
    //
    Node extentionNode = XmlHandler.getSubNode(node, "file", "extention");
    if (extentionNode != null) {
      fileSettings.setExtension(XmlHandler.getNodeValue(extentionNode));
    }
  }
}

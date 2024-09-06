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

package org.apache.hop.pipeline.transforms.propertyinput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "PropertyInput",
    image = "propertyinput.svg",
    name = "i18n::PropertyInput.Name",
    description = "i18n::PropertyInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::PropertyInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/propertyinput.html")
public class PropertyInputMeta extends BaseTransformMeta<PropertyInput, PropertyInputData> {
  private static final Class<?> PKG = PropertyInputMeta.class;

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  public static final String DEFAULT_ENCODING = "UTF-8";

  private static final String YES = "Y";

  @HopMetadataProperty(key = "encoding")
  private String encoding;

  @HopMetadataProperty(key = "file_type", storeWithCode = true)
  private FileType fileType;

  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(key = "include")
  private boolean includingFilename;

  /** Flag indicating that we should reset RowNum for each file */
  @HopMetadataProperty(key = "resetrownumber")
  private boolean resettingRowNumber;

  /** Flag do variable substitution for value */
  @HopMetadataProperty(key = "resolvevaluevariable")
  private boolean resolvingValueVariable;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(key = "include_field")
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key = "rownum")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key = "rownum_field")
  private String rowNumberField;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "limit")
  private long rowLimit;

  /** file name from previous fields */
  @HopMetadataProperty(key = "filefield")
  private boolean fileField;

  @HopMetadataProperty(key = "isaddresult")
  private boolean addResult;

  @HopMetadataProperty(key = "filename_Field")
  private String dynamicFilenameField;

  /** Flag indicating that a INI file section field should be included in the output */
  @HopMetadataProperty(key = "ini_section")
  private boolean includeIniSection;

  /** The name of the field in the output containing the INI file section */
  @HopMetadataProperty(key = "ini_section_field")
  private String iniSectionField;

  @HopMetadataProperty(key = "section")
  private String section;

  /** Additional fields */
  @HopMetadataProperty(key = "shortFileFieldName")
  private String shortFileFieldName;

  @HopMetadataProperty(key = "pathFieldName")
  private String pathFieldName;

  @HopMetadataProperty(key = "hiddenFieldName")
  private String hiddenFieldName;

  @HopMetadataProperty(key = "lastModificationTimeFieldName")
  private String lastModificationTimeFieldName;

  @HopMetadataProperty(key = "uriNameFieldName")
  private String uriNameFieldName;

  @HopMetadataProperty(key = "rootUriNameFieldName")
  private String rootUriNameFieldName;

  @HopMetadataProperty(key = "extensionFieldName")
  private String extensionFieldName;

  @HopMetadataProperty(key = "sizeFieldName")
  private String sizeFieldName;

  @HopMetadataProperty(key = "file")
  private List<PIFile> files;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<PIField> inputFields;

  public PropertyInputMeta() {
    super();
    files = new ArrayList<>();
    inputFields = new ArrayList<>();
  }

  public PropertyInputMeta(PropertyInputMeta m) {
    this();
    this.encoding = m.encoding;
    this.fileType = m.fileType;
    this.includingFilename = m.includingFilename;
    this.resettingRowNumber = m.resettingRowNumber;
    this.resolvingValueVariable = m.resolvingValueVariable;
    this.filenameField = m.filenameField;
    this.includeRowNumber = m.includeRowNumber;
    this.rowNumberField = m.rowNumberField;
    this.rowLimit = m.rowLimit;
    this.fileField = m.fileField;
    this.addResult = m.addResult;
    this.dynamicFilenameField = m.dynamicFilenameField;
    this.includeIniSection = m.includeIniSection;
    this.iniSectionField = m.iniSectionField;
    this.section = m.section;
    this.shortFileFieldName = m.shortFileFieldName;
    this.pathFieldName = m.pathFieldName;
    this.hiddenFieldName = m.hiddenFieldName;
    this.lastModificationTimeFieldName = m.lastModificationTimeFieldName;
    this.uriNameFieldName = m.uriNameFieldName;
    this.rootUriNameFieldName = m.rootUriNameFieldName;
    this.extensionFieldName = m.extensionFieldName;
    this.sizeFieldName = m.sizeFieldName;
    m.files.forEach(f -> this.files.add(new PIFile(f)));
    m.inputFields.forEach(f -> this.inputFields.add(new PIField(f)));
  }

  @Override
  public PropertyInputMeta clone() {
    return new PropertyInputMeta(this);
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

    fileType = FileType.PROPERTY;
    section = "";
    encoding = DEFAULT_ENCODING;
    includeIniSection = false;
    iniSectionField = "";
    resolvingValueVariable = false;
    addResult = true;
    fileField = false;
    includingFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    dynamicFilenameField = "";
    rowLimit = 0;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    for (PIField field : inputFields) {
      try {
        IValueMeta v = field.createValueMeta();
        v.setOrigin(name);
        if (v.getType() == IValueMeta.TYPE_NONE) {
          v.setTrimType(IValueMeta.TYPE_STRING);
        }
        r.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
    String realFilenameField = variables.resolve(filenameField);
    if (includingFilename && !Utils.isEmpty(realFilenameField)) {
      IValueMeta v = new ValueMetaString(realFilenameField);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    String realRowNumberField = variables.resolve(rowNumberField);
    if (includeRowNumber && !Utils.isEmpty(realRowNumberField)) {
      IValueMeta v = new ValueMetaInteger(realRowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    String realSectionField = variables.resolve(iniSectionField);
    if (includeIniSection && !Utils.isEmpty(realSectionField)) {
      IValueMeta v = new ValueMetaString(realSectionField);
      v.setLength(500);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    // Add additional fields

    if (StringUtils.isNotEmpty(getShortFileFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getShortFileFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getExtensionFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getExtensionFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getPathFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getPathFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getSizeFieldName())) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(getSizeFieldName()));
      v.setOrigin(name);
      v.setLength(9);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getHiddenFieldName())) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(getHiddenFieldName()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (StringUtils.isNotEmpty(getLastModificationTimeFieldName())) {
      IValueMeta v = new ValueMetaDate(variables.resolve(getLastModificationTimeFieldName()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getUriNameFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getUriNameFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (StringUtils.isNotEmpty(getRootUriNameFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getRootUriNameFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public String[] getFilesNames() {
    String[] names = new String[files.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = files.get(i).getName();
    }
    return names;
  }

  public String[] getFilesMasks() {
    String[] masks = new String[files.size()];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = files.get(i).getMask();
    }
    return masks;
  }

  public String[] getFilesExcludeMasks() {
    String[] masks = new String[files.size()];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = files.get(i).getExcludeMask();
    }
    return masks;
  }

  public String[] getFilesRequired() {
    String[] masks = new String[files.size()];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = files.get(i).isRequired() ? "Y" : "N";
    }
    return masks;
  }

  public FileInputList getFiles(IVariables variables) {
    boolean[] subDirectories = new boolean[files.size()]; // boolean arrays are defaulted to false.
    return FileInputList.createFileList(
        variables,
        getFilesNames(),
        getFilesMasks(),
        getFilesExcludeMasks(),
        getFilesRequired(),
        subDirectories);
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
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyInputMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    FileInputList fileInputList = getFiles(variables);

    if (fileInputList == null || fileInputList.getFiles().isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyInputMeta.CheckResult.NoFiles"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "PropertyInputMeta.CheckResult.FilesOk",
                  "" + fileInputList.getFiles().size()),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
   * @param variables the variable variables to use
   * @param definitions The definitions to use
   * @param iResourceNaming The naming system to use
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
      if (!fileField) {
        for (PIFile file : files) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(file.getName()));
          file.setName(
              iResourceNaming.nameResource(fileObject, variables, Utils.isEmpty(file.getMask())));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public enum FileType implements IEnumHasCodeAndDescription {
    PROPERTY("property", BaseMessages.getString(PKG, "PropertyInputMeta.FileType.Property")),
    INI("ini", BaseMessages.getString(PKG, "PropertyInputMeta.FileType.Ini"));
    private final String code;
    private final String description;

    FileType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(FileType.class);
    }

    public static FileType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(FileType.class, description, PROPERTY);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }
  }

  public static final class PIFile {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "filemask")
    private String mask;

    @HopMetadataProperty(key = "exclude_filemask")
    private String excludeMask;

    @HopMetadataProperty(key = "file_required")
    private boolean required;

    @HopMetadataProperty(key = "include_subfolders")
    private boolean includingSubFolders;

    public PIFile() {}

    public PIFile(PIFile f) {
      this();
      this.name = f.name;
      this.mask = f.mask;
      this.excludeMask = f.excludeMask;
      this.required = f.required;
      this.includingSubFolders = f.includingSubFolders;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets mask
     *
     * @return value of mask
     */
    public String getMask() {
      return mask;
    }

    /**
     * Sets mask
     *
     * @param mask value of mask
     */
    public void setMask(String mask) {
      this.mask = mask;
    }

    /**
     * Gets excludeMask
     *
     * @return value of excludeMask
     */
    public String getExcludeMask() {
      return excludeMask;
    }

    /**
     * Sets excludeMask
     *
     * @param excludeMask value of excludeMask
     */
    public void setExcludeMask(String excludeMask) {
      this.excludeMask = excludeMask;
    }

    /**
     * Gets required
     *
     * @return value of required
     */
    public boolean isRequired() {
      return required;
    }

    /**
     * Sets required
     *
     * @param required value of required
     */
    public void setRequired(boolean required) {
      this.required = required;
    }

    /**
     * Gets includingSubFolders
     *
     * @return value of includingSubFolders
     */
    public boolean isIncludingSubFolders() {
      return includingSubFolders;
    }

    /**
     * Sets includingSubFolders
     *
     * @param includingSubFolders value of includingSubFolders
     */
    public void setIncludingSubFolders(boolean includingSubFolders) {
      this.includingSubFolders = includingSubFolders;
    }
  }

  public enum KeyValue implements IEnumHasCode {
    KEY("key"),
    VALUE("value");
    private final String code;

    KeyValue(String code) {
      this.code = code;
    }

    public static String[] getCodes() {
      return IEnumHasCode.getCodes(KeyValue.class);
    }

    public static KeyValue lookupCode(String code) {
      return IEnumHasCode.lookupCode(KeyValue.class, code, KEY);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }
  }

  public static final class PIField {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "column", storeWithCode = true)
    private KeyValue column;

    @HopMetadataProperty(key = "type")
    private String type;

    @HopMetadataProperty(key = "format")
    private String format;

    @HopMetadataProperty(key = "length")
    private int length;

    @HopMetadataProperty(key = "precision")
    private int precision;

    @HopMetadataProperty(key = "currency")
    private String currency;

    @HopMetadataProperty(key = "decimal")
    private String decimal;

    @HopMetadataProperty(key = "group")
    private String group;

    @HopMetadataProperty(key = "trim_type", storeWithCode = true)
    private IValueMeta.TrimType trimType;

    @HopMetadataProperty(key = "repeat")
    private boolean repeating;

    public PIField() {
      trimType = IValueMeta.TrimType.NONE;
    }

    public PIField(PIField f) {
      this();
      this.name = f.name;
      this.column = f.column;
      this.type = f.type;
      this.format = f.format;
      this.length = f.length;
      this.precision = f.precision;
      this.currency = f.currency;
      this.decimal = f.decimal;
      this.group = f.group;
      this.trimType = f.trimType;
      this.repeating = f.repeating;
    }

    public PIField(String name) {
      this();
      this.name = name;
    }

    public int getHopType() {
      return ValueMetaFactory.getIdForValueMeta(type);
    }

    public IValueMeta createValueMeta() throws HopPluginException {
      IValueMeta v = ValueMetaFactory.createValueMeta(name, getHopType());
      v.setConversionMask(format);
      v.setLength(length, precision);
      v.setCurrencySymbol(currency);
      v.setDecimalSymbol(decimal);
      v.setGroupingSymbol(group);
      v.setTrimType(trimType.getType());
      return v;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets column
     *
     * @return value of column
     */
    public KeyValue getColumn() {
      return column;
    }

    /**
     * Sets column
     *
     * @param column value of column
     */
    public void setColumn(KeyValue column) {
      this.column = column;
    }

    /**
     * Gets type
     *
     * @return value of type
     */
    public String getType() {
      return type;
    }

    /**
     * Sets type
     *
     * @param type value of type
     */
    public void setType(String type) {
      this.type = type;
    }

    /**
     * Gets format
     *
     * @return value of format
     */
    public String getFormat() {
      return format;
    }

    /**
     * Sets format
     *
     * @param format value of format
     */
    public void setFormat(String format) {
      this.format = format;
    }

    /**
     * Gets length
     *
     * @return value of length
     */
    public int getLength() {
      return length;
    }

    /**
     * Sets length
     *
     * @param length value of length
     */
    public void setLength(int length) {
      this.length = length;
    }

    /**
     * Gets precision
     *
     * @return value of precision
     */
    public int getPrecision() {
      return precision;
    }

    /**
     * Sets precision
     *
     * @param precision value of precision
     */
    public void setPrecision(int precision) {
      this.precision = precision;
    }

    /**
     * Gets currency
     *
     * @return value of currency
     */
    public String getCurrency() {
      return currency;
    }

    /**
     * Sets currency
     *
     * @param currency value of currency
     */
    public void setCurrency(String currency) {
      this.currency = currency;
    }

    /**
     * Gets decimal
     *
     * @return value of decimal
     */
    public String getDecimal() {
      return decimal;
    }

    /**
     * Sets decimal
     *
     * @param decimal value of decimal
     */
    public void setDecimal(String decimal) {
      this.decimal = decimal;
    }

    /**
     * Gets group
     *
     * @return value of group
     */
    public String getGroup() {
      return group;
    }

    /**
     * Sets group
     *
     * @param group value of group
     */
    public void setGroup(String group) {
      this.group = group;
    }

    /**
     * Gets trimType
     *
     * @return value of trimType
     */
    public IValueMeta.TrimType getTrimType() {
      return trimType;
    }

    /**
     * Sets trimType
     *
     * @param trimType value of trimType
     */
    public void setTrimType(IValueMeta.TrimType trimType) {
      this.trimType = trimType;
    }

    /**
     * Gets repeating
     *
     * @return value of repeating
     */
    public boolean isRepeating() {
      return repeating;
    }

    /**
     * Sets repeating
     *
     * @param repeating value of repeating
     */
    public void setRepeating(boolean repeating) {
      this.repeating = repeating;
    }
  }

  /**
   * Gets encoding
   *
   * @return value of encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * Sets encoding
   *
   * @param encoding value of encoding
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  public FileType getFileType() {
    return fileType;
  }

  /**
   * Sets fileType
   *
   * @param fileType value of fileType
   */
  public void setFileType(FileType fileType) {
    this.fileType = fileType;
  }

  /**
   * Gets includingFilename
   *
   * @return value of includingFilename
   */
  public boolean isIncludingFilename() {
    return includingFilename;
  }

  /**
   * Sets includingFilename
   *
   * @param includingFilename value of includingFilename
   */
  public void setIncludingFilename(boolean includingFilename) {
    this.includingFilename = includingFilename;
  }

  /**
   * Gets resettingRowNumber
   *
   * @return value of resettingRowNumber
   */
  public boolean isResettingRowNumber() {
    return resettingRowNumber;
  }

  /**
   * Sets resettingRowNumber
   *
   * @param resettingRowNumber value of resettingRowNumber
   */
  public void setResettingRowNumber(boolean resettingRowNumber) {
    this.resettingRowNumber = resettingRowNumber;
  }

  /**
   * Gets resolvingValueVariable
   *
   * @return value of resolvingValueVariable
   */
  public boolean isResolvingValueVariable() {
    return resolvingValueVariable;
  }

  /**
   * Sets resolvingValueVariable
   *
   * @param resolvingValueVariable value of resolvingValueVariable
   */
  public void setResolvingValueVariable(boolean resolvingValueVariable) {
    this.resolvingValueVariable = resolvingValueVariable;
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /**
   * Sets filenameField
   *
   * @param filenameField value of filenameField
   */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets includeRowNumber
   *
   * @return value of includeRowNumber
   */
  public boolean isIncludeRowNumber() {
    return includeRowNumber;
  }

  /**
   * Sets includeRowNumber
   *
   * @param includeRowNumber value of includeRowNumber
   */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /**
   * Gets rowNumberField
   *
   * @return value of rowNumberField
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /**
   * Sets rowNumberField
   *
   * @param rowNumberField value of rowNumberField
   */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /**
   * Gets rowLimit
   *
   * @return value of rowLimit
   */
  public long getRowLimit() {
    return rowLimit;
  }

  /**
   * Sets rowLimit
   *
   * @param rowLimit value of rowLimit
   */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * Gets fileField
   *
   * @return value of fileField
   */
  public boolean isFileField() {
    return fileField;
  }

  /**
   * Sets fileField
   *
   * @param fileField value of fileField
   */
  public void setFileField(boolean fileField) {
    this.fileField = fileField;
  }

  /**
   * Gets isAddResult
   *
   * @return value of isAddResult
   */
  public boolean isAddResult() {
    return addResult;
  }

  /**
   * Sets isAddResult
   *
   * @param addResult value of isAddResult
   */
  public void setAddResult(boolean addResult) {
    this.addResult = addResult;
  }

  /**
   * Gets dynamicFilenameField
   *
   * @return value of dynamicFilenameField
   */
  public String getDynamicFilenameField() {
    return dynamicFilenameField;
  }

  /**
   * Sets dynamicFilenameField
   *
   * @param dynamicFilenameField value of dynamicFilenameField
   */
  public void setDynamicFilenameField(String dynamicFilenameField) {
    this.dynamicFilenameField = dynamicFilenameField;
  }

  /**
   * Gets includeIniSection
   *
   * @return value of includeIniSection
   */
  public boolean isIncludeIniSection() {
    return includeIniSection;
  }

  /**
   * Sets includeIniSection
   *
   * @param includeIniSection value of includeIniSection
   */
  public void setIncludeIniSection(boolean includeIniSection) {
    this.includeIniSection = includeIniSection;
  }

  /**
   * Gets iniSectionField
   *
   * @return value of iniSectionField
   */
  public String getIniSectionField() {
    return iniSectionField;
  }

  /**
   * Sets iniSectionField
   *
   * @param iniSectionField value of iniSectionField
   */
  public void setIniSectionField(String iniSectionField) {
    this.iniSectionField = iniSectionField;
  }

  /**
   * Gets section
   *
   * @return value of section
   */
  public String getSection() {
    return section;
  }

  /**
   * Sets section
   *
   * @param section value of section
   */
  public void setSection(String section) {
    this.section = section;
  }

  /**
   * Gets shortFileFieldName
   *
   * @return value of shortFileFieldName
   */
  public String getShortFileFieldName() {
    return shortFileFieldName;
  }

  /**
   * Sets shortFileFieldName
   *
   * @param shortFileFieldName value of shortFileFieldName
   */
  public void setShortFileFieldName(String shortFileFieldName) {
    this.shortFileFieldName = shortFileFieldName;
  }

  /**
   * Gets pathFieldName
   *
   * @return value of pathFieldName
   */
  public String getPathFieldName() {
    return pathFieldName;
  }

  /**
   * Sets pathFieldName
   *
   * @param pathFieldName value of pathFieldName
   */
  public void setPathFieldName(String pathFieldName) {
    this.pathFieldName = pathFieldName;
  }

  /**
   * Gets hiddenFieldName
   *
   * @return value of hiddenFieldName
   */
  public String getHiddenFieldName() {
    return hiddenFieldName;
  }

  /**
   * Sets hiddenFieldName
   *
   * @param hiddenFieldName value of hiddenFieldName
   */
  public void setHiddenFieldName(String hiddenFieldName) {
    this.hiddenFieldName = hiddenFieldName;
  }

  /**
   * Gets lastModificationTimeFieldName
   *
   * @return value of lastModificationTimeFieldName
   */
  public String getLastModificationTimeFieldName() {
    return lastModificationTimeFieldName;
  }

  /**
   * Sets lastModificationTimeFieldName
   *
   * @param lastModificationTimeFieldName value of lastModificationTimeFieldName
   */
  public void setLastModificationTimeFieldName(String lastModificationTimeFieldName) {
    this.lastModificationTimeFieldName = lastModificationTimeFieldName;
  }

  /**
   * Gets uriNameFieldName
   *
   * @return value of uriNameFieldName
   */
  public String getUriNameFieldName() {
    return uriNameFieldName;
  }

  /**
   * Sets uriNameFieldName
   *
   * @param uriNameFieldName value of uriNameFieldName
   */
  public void setUriNameFieldName(String uriNameFieldName) {
    this.uriNameFieldName = uriNameFieldName;
  }

  /**
   * Gets rootUriNameFieldName
   *
   * @return value of rootUriNameFieldName
   */
  public String getRootUriNameFieldName() {
    return rootUriNameFieldName;
  }

  /**
   * Sets rootUriNameFieldName
   *
   * @param rootUriNameFieldName value of rootUriNameFieldName
   */
  public void setRootUriNameFieldName(String rootUriNameFieldName) {
    this.rootUriNameFieldName = rootUriNameFieldName;
  }

  /**
   * Gets extensionFieldName
   *
   * @return value of extensionFieldName
   */
  public String getExtensionFieldName() {
    return extensionFieldName;
  }

  /**
   * Sets extensionFieldName
   *
   * @param extensionFieldName value of extensionFieldName
   */
  public void setExtensionFieldName(String extensionFieldName) {
    this.extensionFieldName = extensionFieldName;
  }

  /**
   * Gets sizeFieldName
   *
   * @return value of sizeFieldName
   */
  public String getSizeFieldName() {
    return sizeFieldName;
  }

  /**
   * Sets sizeFieldName
   *
   * @param sizeFieldName value of sizeFieldName
   */
  public void setSizeFieldName(String sizeFieldName) {
    this.sizeFieldName = sizeFieldName;
  }

  /**
   * Gets files
   *
   * @return value of files
   */
  public List<PIFile> getFiles() {
    return files;
  }

  /**
   * Sets files
   *
   * @param files value of files
   */
  public void setFiles(List<PIFile> files) {
    this.files = files;
  }

  /**
   * Gets inputFields
   *
   * @return value of inputFields
   */
  public List<PIField> getInputFields() {
    return inputFields;
  }

  /**
   * Sets inputFields
   *
   * @param inputFields value of inputFields
   */
  public void setInputFields(List<PIField> inputFields) {
    this.inputFields = inputFields;
  }
}

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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
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
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Transform(
    id = "TokenReplacementPlugin",
    image = "token.svg",
    name = "i18n::BaseTransform.TypeLongDesc.TokenReplacement",
    description = "i18n::BaseTransform.TypeTooltipDesc.TokenReplacement",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/tokenreplacement.html")
@InjectionSupported(
    localizationPrefix = "TokenReplacement.Injection.",
    groups = {"OUTPUT_FIELDS"})
public class TokenReplacementMeta extends BaseTransformMeta
    implements ITransformMeta<TokenReplacement, TokenReplacementData> {
  private static Class<?> PKG = TokenReplacementMeta.class; // For Translator

  public static final String INPUT_TYPE = "input_type";
  public static final String INPUT_FIELD_NAME = "input_field_name";
  public static final String INPUT_FILENAME = "input_filename";
  public static final String INPUT_FILENAME_IN_FIELD = "input_filename_in_field";
  public static final String INPUT_FILENAME_FIELD = "input_filename_field";
  public static final String ADD_INPUT_FILENAME_TO_RESULT = "add_input_filename_to_result";
  public static final String OUTPUT_TYPE = "output_type";
  public static final String OUTPUT_FIELD_NAME = "output_field_name";
  public static final String OUTPUT_FILENAME = "output_filename";
  public static final String OUTPUT_FILENAME_IN_FIELD = "output_filename_in_field";
  public static final String OUTPUT_FILENAME_FIELD = "output_filename_field";
  public static final String APPEND_OUTPUT_FILENAME = "append_output_filename";
  public static final String CREATE_PARENT_FOLDER = "create_parent_folder";
  public static final String INCLUDE_TRANSFORM_NR_IN_OUTPUT_FILENAME =
      "include_transform_nr_in_output_filename";
  public static final String INCLUDE_PART_NR_IN_OUTPUT_FILENAME =
      "include_part_nr_in_output_filename";
  public static final String INCLUDE_DATE_IN_OUTPUT_FILENAME = "include_date_in_output_filename";
  public static final String INCLUDE_TIME_IN_OUTPUT_FILENAME = "include_time_in_output_filename";
  public static final String SPECIFY_DATE_FORMAT_OUTPUT_FILENAME =
      "specify_date_format_output_filename";
  public static final String DATE_FORMAT_OUTPUT_FILENAME = "date_format_output_filename";
  public static final String ADD_OUTPUT_FILENAME_TO_RESULT = "add_output_filename_to_result";
  public static final String TOKEN_START_STRING = "token_start_string";
  public static final String TOKEN_END_STRING = "token_end_string";
  public static final String FIELD_NAME = "field_name";
  public static final String TOKEN_NAME = "token_name";
  public static final String INPUT_TEXT = "input_text";
  public static final String OUTPUT_FILE_ENCODING = "output_file_encoding";
  public static final String OUTPUT_SPLIT_EVERY = "output_split_every";
  public static final String OUTPUT_FILE_FORMAT = "output_file_format";

  public static final String[] INPUT_TYPES = {"text", "field", "file"};
  public static final String[] OUTPUT_TYPES = {"field", "file"};
  public static final String[] formatMapperLineTerminator =
      new String[] {"DOS", "UNIX", "CR", "None"};

  public static final String[] formatMapperLineTerminatorDescriptions =
      new String[] {
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.DOS"),
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.UNIX"),
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.CR"),
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.None")
      };

  @Injection(name = "INPUT_TYPE")
  private String inputType;

  @Injection(name = "INPUT_TEXT")
  private String inputText;

  @Injection(name = "INPUT_FIELD")
  private String inputFieldName;

  @Injection(name = "INPUT_FILENAME")
  private String inputFileName;

  @Injection(name = "INPUT_FILENAME_IN_FIELD")
  private boolean inputFileNameInField;

  @Injection(name = "INPUT_FILENAME_FIELD")
  private String inputFileNameField;

  @Injection(name = "ADD_INPUT_FILENAME_TO_RESULT")
  private boolean addInputFileNameToResult;

  @Injection(name = "OUTPUT_TYPE")
  private String outputType;

  @Injection(name = "OUTPUT_FIELD")
  private String outputFieldName;

  @Injection(name = "OUTPUT_FILENAME")
  private String outputFileName;

  @Injection(name = "OUTPUT_FILENAME_IN_FIELD")
  private boolean outputFileNameInField;

  @Injection(name = "OUTPUT_FILENAME_FIELD")
  private String outputFileNameField;

  @Injection(name = "APPEND_OUTPUT_FILE")
  private boolean appendOutputFileName;

  @Injection(name = "CREATE_PARENT_FOLDER")
  private boolean createParentFolder;

  @Injection(name = "OUTPUT_FORMAT")
  private String outputFileFormat;

  @Injection(name = "OUTPUT_ENCODING")
  private String outputFileEncoding;

  @Injection(name = "OUTPUT_SPLIT_EVERY")
  private int splitEvery;

  @Injection(name = "OUTPUT_INCLUDE_TRANSFORMNR")
  private boolean includeTransformNrInOutputFileName;

  @Injection(name = "OUTPUT_INCLUDE_PARTNR")
  private boolean includePartNrInOutputFileName;

  @Injection(name = "OUTPUT_INCLUDE_DATE")
  private boolean includeDateInOutputFileName;

  @Injection(name = "OUTPUT_INCLUDE_TIME")
  private boolean includeTimeInOutputFileName;

  @Injection(name = "OUTPUT_SPECIFY_DATE_FORMAT")
  private boolean specifyDateFormatOutputFileName;

  @Injection(name = "OUTPUT_DATE_FORMAT")
  private String dateFormatOutputFileName;

  @Injection(name = "ADD_OUTPUT_FILENAME_TO_RESULT")
  private boolean addOutputFileNameToResult;

  @Injection(name = "TOKEN_START_STRING")
  private String tokenStartString;

  @Injection(name = "TOKEN_END_STRING")
  private String tokenEndString;

  @InjectionDeep private TokenReplacementField[] tokenReplacementFields;

  public TokenReplacementMeta() {
    super(); // allocate BaseTransformMeta
    allocate(0);
  }

  public String getInputType() {
    return inputType;
  }

  public void setInputType(String inputType) {
    this.inputType = inputType;
  }

  public String getInputText() {
    return inputText;
  }

  public void setInputText(String inputText) {
    this.inputText = inputText;
  }

  public String getInputFieldName() {
    return inputFieldName;
  }

  public void setInputFieldName(String inputFieldName) {
    this.inputFieldName = inputFieldName;
  }

  public String getInputFileName() {
    return inputFileName;
  }

  public void setInputFileName(String inputFileName) {
    this.inputFileName = inputFileName;
  }

  public boolean isInputFileNameInField() {
    return inputFileNameInField;
  }

  public void setInputFileNameInField(boolean inputFileNameInField) {
    this.inputFileNameInField = inputFileNameInField;
  }

  public String getInputFileNameField() {
    return inputFileNameField;
  }

  public void setInputFileNameField(String inputFileNameField) {
    this.inputFileNameField = inputFileNameField;
  }

  public boolean isAddInputFileNameToResult() {
    return addInputFileNameToResult;
  }

  public void setAddInputFileNameToResult(boolean addInputFileNameToResult) {
    this.addInputFileNameToResult = addInputFileNameToResult;
  }

  public String getOutputType() {
    return outputType;
  }

  public void setOutputType(String outputType) {
    this.outputType = outputType;
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName(String outputFieldName) {
    this.outputFieldName = outputFieldName;
  }

  public String getOutputFileName() {
    return outputFileName;
  }

  public void setOutputFileName(String outputFileName) {
    this.outputFileName = outputFileName;
  }

  public boolean isOutputFileNameInField() {
    return outputFileNameInField;
  }

  public void setOutputFileNameInField(boolean outputFileNameInField) {
    this.outputFileNameInField = outputFileNameInField;
  }

  public String getOutputFileNameField() {
    return outputFileNameField;
  }

  public void setOutputFileNameField(String outputFileNameField) {
    this.outputFileNameField = outputFileNameField;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder(boolean createParentFolder) {
    this.createParentFolder = createParentFolder;
  }

  public String getOutputFileEncoding() {
    return Const.NVL(outputFileEncoding, Const.getEnvironmentVariable("file.encoding", "UTF-8"));
  }

  public void setOutputFileEncoding(String outputFileEncoding) {
    this.outputFileEncoding = outputFileEncoding;
  }

  public String getOutputFileFormat() {
    return outputFileFormat;
  }

  public String getOutputFileFormatString() {
    if (outputFileFormat.equals("DOS")) {
      return "\r\n";
    } else if (outputFileFormat.equals("UNIX")) {
      return "\n";
    } else if (outputFileFormat.equals("CR")) {
      return "\r";
    } else {
      return "";
    }
  }

  public void setOutputFileFormat(String outputFileFormat) {
    this.outputFileFormat = outputFileFormat;
  }

  public int getSplitEvery() {
    return splitEvery;
  }

  public void setSplitEvery(int splitEvery) {
    this.splitEvery = splitEvery;
  }

  public boolean isIncludeTransformNrInOutputFileName() {
    return includeTransformNrInOutputFileName;
  }

  public void setIncludeTransformNrInOutputFileName(boolean includeTransformNrInOutputFileName) {
    this.includeTransformNrInOutputFileName = includeTransformNrInOutputFileName;
  }

  public boolean isIncludePartNrInOutputFileName() {
    return includePartNrInOutputFileName;
  }

  public void setIncludePartNrInOutputFileName(boolean includePartNrInOutputFileName) {
    this.includePartNrInOutputFileName = includePartNrInOutputFileName;
  }

  public boolean isIncludeDateInOutputFileName() {
    return includeDateInOutputFileName;
  }

  public void setIncludeDateInOutputFileName(boolean includeDateInOutputFileName) {
    this.includeDateInOutputFileName = includeDateInOutputFileName;
  }

  public boolean isIncludeTimeInOutputFileName() {
    return includeTimeInOutputFileName;
  }

  public void setIncludeTimeInOutputFileName(boolean includeTimeInOutputFileName) {
    this.includeTimeInOutputFileName = includeTimeInOutputFileName;
  }

  public boolean isSpecifyDateFormatOutputFileName() {
    return specifyDateFormatOutputFileName;
  }

  public void setSpecifyDateFormatOutputFileName(boolean specifyDateFormatOutputFileName) {
    this.specifyDateFormatOutputFileName = specifyDateFormatOutputFileName;
  }

  public String getDateFormatOutputFileName() {
    return dateFormatOutputFileName;
  }

  public void setDateFormatOutputFileName(String dateFormatOutputFileName) {
    this.dateFormatOutputFileName = dateFormatOutputFileName;
  }

  public boolean isAddOutputFileNameToResult() {
    return addOutputFileNameToResult;
  }

  public void setAddOutputFileNameToResult(boolean addOutputFileNameToResult) {
    this.addOutputFileNameToResult = addOutputFileNameToResult;
  }

  public String getTokenStartString() {
    return tokenStartString;
  }

  public void setTokenStartString(String tokenStartString) {
    this.tokenStartString = tokenStartString;
  }

  public String getTokenEndString() {
    return tokenEndString;
  }

  public void setTokenEndString(String tokenEndString) {
    this.tokenEndString = tokenEndString;
  }

  public TokenReplacementField[] getTokenReplacementFields() {
    return tokenReplacementFields;
  }

  public void setTokenReplacementFields(TokenReplacementField[] tokenReplacementFields) {
    this.tokenReplacementFields = tokenReplacementFields;
  }

  public boolean isAppendOutputFileName() {
    return appendOutputFileName;
  }

  public void setAppendOutputFileName(boolean appendOutputFileName) {
    this.appendOutputFileName = appendOutputFileName;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {

      inputType = XmlHandler.getTagValue(transformNode, INPUT_TYPE);
      inputText = XmlHandler.getTagValue(transformNode, INPUT_TEXT);
      inputFieldName = XmlHandler.getTagValue(transformNode, INPUT_FIELD_NAME);
      inputFileName = XmlHandler.getTagValue(transformNode, INPUT_FILENAME);
      inputFileNameInField =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(XmlHandler.getTagValue(transformNode, INPUT_FILENAME_IN_FIELD), ""));
      inputFileNameField = XmlHandler.getTagValue(transformNode, INPUT_FILENAME_FIELD);
      addInputFileNameToResult =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, ADD_INPUT_FILENAME_TO_RESULT), ""));

      outputType = XmlHandler.getTagValue(transformNode, OUTPUT_TYPE);
      outputFieldName = XmlHandler.getTagValue(transformNode, OUTPUT_FIELD_NAME);
      outputFileName = XmlHandler.getTagValue(transformNode, OUTPUT_FILENAME);
      outputFileNameInField =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(XmlHandler.getTagValue(transformNode, OUTPUT_FILENAME_IN_FIELD), ""));
      outputFileNameField = XmlHandler.getTagValue(transformNode, OUTPUT_FILENAME_FIELD);
      appendOutputFileName =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(XmlHandler.getTagValue(transformNode, APPEND_OUTPUT_FILENAME), ""));
      outputFileFormat = XmlHandler.getTagValue(transformNode, OUTPUT_FILE_FORMAT);
      outputFileEncoding =
          Const.NVL(
              XmlHandler.getTagValue(transformNode, OUTPUT_FILE_ENCODING),
              Const.getEnvironmentVariable("file.encoding", "UTF-8"));
      splitEvery = Const.toInt(XmlHandler.getTagValue(transformNode, OUTPUT_SPLIT_EVERY), 0);
      createParentFolder =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(XmlHandler.getTagValue(transformNode, CREATE_PARENT_FOLDER), ""));
      includeTransformNrInOutputFileName =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, INCLUDE_TRANSFORM_NR_IN_OUTPUT_FILENAME ),
                      ""));
      includePartNrInOutputFileName =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, INCLUDE_PART_NR_IN_OUTPUT_FILENAME),
                      ""));
      includeDateInOutputFileName =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, INCLUDE_DATE_IN_OUTPUT_FILENAME), ""));
      includeTimeInOutputFileName =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, INCLUDE_TIME_IN_OUTPUT_FILENAME), ""));
      specifyDateFormatOutputFileName =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, SPECIFY_DATE_FORMAT_OUTPUT_FILENAME),
                      ""));
      dateFormatOutputFileName = XmlHandler.getTagValue(transformNode, DATE_FORMAT_OUTPUT_FILENAME);
      addOutputFileNameToResult =
          "Y"
              .equalsIgnoreCase(
                  Const.NVL(
                      XmlHandler.getTagValue(transformNode, ADD_OUTPUT_FILENAME_TO_RESULT), ""));

      tokenStartString = XmlHandler.getTagValue(transformNode, TOKEN_START_STRING);
      tokenEndString = XmlHandler.getTagValue(transformNode, TOKEN_END_STRING);

      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrfields = XmlHandler.countNodes(fields, "field");

      allocate(nrfields);

      for (int i = 0; i < nrfields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        tokenReplacementFields[i] = new TokenReplacementField();
        tokenReplacementFields[i].setName(XmlHandler.getTagValue(fnode, FIELD_NAME));
        tokenReplacementFields[i].setTokenName(XmlHandler.getTagValue(fnode, TOKEN_NAME));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "BaseTransformMeta.Exception.ErrorLoadingTransformMeta"), e);
    }
  }

  public void allocate(int nrfields) {
    tokenReplacementFields = new TokenReplacementField[nrfields];
  }

  public Object clone() {
    TokenReplacementMeta retval = (TokenReplacementMeta) super.clone();
    int nrfields = tokenReplacementFields.length;

    retval.allocate(nrfields);

    for (int i = 0; i < nrfields; i++) {
      retval.tokenReplacementFields[i] = (TokenReplacementField) tokenReplacementFields[i].clone();
    }

    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      TokenReplacementData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new TokenReplacement(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public TokenReplacementData getTransformData() {
    return new TokenReplacementData();
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder(800);

    retval.append("    " + XmlHandler.addTagValue(INPUT_TYPE, inputType));
    retval.append("    " + XmlHandler.addTagValue(INPUT_TEXT, inputText));
    retval.append("    " + XmlHandler.addTagValue(INPUT_FIELD_NAME, inputFieldName));
    retval.append("    " + XmlHandler.addTagValue(INPUT_FILENAME, inputFileName));
    retval.append("    " + XmlHandler.addTagValue(INPUT_FILENAME_IN_FIELD, inputFileNameInField));
    retval.append("    " + XmlHandler.addTagValue(INPUT_FILENAME_FIELD, inputFileNameField));
    retval.append(
        "    " + XmlHandler.addTagValue(ADD_INPUT_FILENAME_TO_RESULT, addInputFileNameToResult));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_TYPE, outputType));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_FIELD_NAME, outputFieldName));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_FILENAME, outputFileName));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_FILENAME_IN_FIELD, outputFileNameInField));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_FILENAME_FIELD, outputFileNameField));
    retval.append("    " + XmlHandler.addTagValue(APPEND_OUTPUT_FILENAME, appendOutputFileName));
    retval.append("    " + XmlHandler.addTagValue(CREATE_PARENT_FOLDER, createParentFolder));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_FILE_FORMAT, outputFileFormat));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_FILE_ENCODING, outputFileEncoding));
    retval.append("    " + XmlHandler.addTagValue(OUTPUT_SPLIT_EVERY, splitEvery));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
          INCLUDE_TRANSFORM_NR_IN_OUTPUT_FILENAME, includeTransformNrInOutputFileName));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                INCLUDE_PART_NR_IN_OUTPUT_FILENAME, includePartNrInOutputFileName));
    retval.append(
        "    "
            + XmlHandler.addTagValue(INCLUDE_DATE_IN_OUTPUT_FILENAME, includeDateInOutputFileName));
    retval.append(
        "    "
            + XmlHandler.addTagValue(INCLUDE_TIME_IN_OUTPUT_FILENAME, includeTimeInOutputFileName));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                SPECIFY_DATE_FORMAT_OUTPUT_FILENAME, specifyDateFormatOutputFileName));
    retval.append(
        "    " + XmlHandler.addTagValue(DATE_FORMAT_OUTPUT_FILENAME, dateFormatOutputFileName));
    retval.append(
        "    " + XmlHandler.addTagValue(ADD_OUTPUT_FILENAME_TO_RESULT, addOutputFileNameToResult));

    retval.append("    " + XmlHandler.addTagValue(TOKEN_START_STRING, tokenStartString));
    retval.append("    " + XmlHandler.addTagValue(TOKEN_END_STRING, tokenEndString));

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < tokenReplacementFields.length; i++) {
      TokenReplacementField field = tokenReplacementFields[i];

      if (field.getName() != null && field.getName().length() != 0) {
        retval.append("      <field>").append(Const.CR);
        retval.append("        ").append(XmlHandler.addTagValue(FIELD_NAME, field.getName()));
        retval.append("        ").append(XmlHandler.addTagValue(TOKEN_NAME, field.getTokenName()));
        retval.append("      </field>").append(Const.CR);
      }
    }
    retval.append("    </fields>").append(Const.CR);

    return retval.toString();
  }

  public void setDefault() {

    inputType = "Text";
    inputFileNameInField = false;
    addInputFileNameToResult = false;

    outputType = "Field";
    outputFileNameInField = false;
    appendOutputFileName = false;
    createParentFolder = false;
    includeTransformNrInOutputFileName = false;
    includePartNrInOutputFileName = false;
    includeDateInOutputFileName = false;
    includeTimeInOutputFileName = false;
    specifyDateFormatOutputFileName = false;
    addOutputFileNameToResult = false;
    outputFileEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
    outputFileFormat = Const.isWindows() ? "DOS" : "UNIX";
    splitEvery = 0;

    tokenStartString = "${";
    tokenEndString = "}";
  }

  public String buildFilename(
      String fileName, IVariables variables, int transformnr, String partnr, int splitnr) {
    return buildFilename(fileName, variables, transformnr, partnr, splitnr, this);
  }

  public String buildFilename(
      String filename,
      IVariables variables,
      int transformnr,
      String partnr,
      int splitnr,
      TokenReplacementMeta meta) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realFileName = variables.resolve(filename);
    String extension = "";
    String retval = "";
    if (realFileName.contains(".")) {
      retval = realFileName.substring(0, realFileName.lastIndexOf("."));
      extension = realFileName.substring(realFileName.lastIndexOf(".") + 1);
    } else {
      retval = realFileName;
    }

    Date now = new Date();

    if (meta.isSpecifyDateFormatOutputFileName()
        && !Utils.isEmpty(meta.getDateFormatOutputFileName())) {
      daf.applyPattern(meta.getDateFormatOutputFileName());
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (meta.isIncludeDateInOutputFileName()) {
        daf.applyPattern("yyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (meta.isIncludeTimeInOutputFileName()) {
        daf.applyPattern("HHmmss");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }
    if (meta.isIncludeTransformNrInOutputFileName()) {
      retval += "_" + transformnr;
    }
    if (meta.isIncludePartNrInOutputFileName()) {
      retval += "_" + partnr;
    }

    if (meta.getSplitEvery() > 0) {
      retval += "_" + splitnr;
    }

    if (extension != null && extension.length() != 0) {
      retval += "." + extension;
    }
    return retval;
  }

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
                  PKG, "TokenReplacementMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for (TokenReplacementField tokenReplacementField : tokenReplacementFields) {
        int idx = prev.indexOfValue(tokenReplacementField.getName());
        if (idx < 0) {
          error_message += "\t\t" + tokenReplacementField.getName() + Const.CR;
          error_found = true;
        }
      }
      if (error_found) {
        error_message =
            BaseMessages.getString(
                PKG, "TokenReplacementMeta.CheckResult.FieldsNotFound", error_message);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, error_message, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TokenReplacementMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // Make sure token replacement is populated!
    if (Utils.isEmpty(tokenStartString) || Utils.isEmpty(tokenEndString)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "TokenReplacementMeta.CheckResult.ExpectedTokenReplacementError"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "TokenReplacementMeta.CheckResult.ExpectedTokenReplacementOk"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TokenReplacementMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TokenReplacementMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "TokenReplacementMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // change the case insensitive flag too

    if (outputType.equalsIgnoreCase("field")) {
      IValueMeta v = new ValueMetaString(variables.resolve(outputFieldName));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  public static final String getOutputFileFormatDescription(String code) {
    int index = Const.indexOfString( code, formatMapperLineTerminator);
    if (index<0) {
      index=3;
    }
    return formatMapperLineTerminatorDescriptions[index];
  }
}

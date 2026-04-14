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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "TokenReplacementPlugin",
    image = "token.svg",
    name = "i18n::TokenReplacement.Name",
    description = "i18n::TokenReplacement.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::TokenReplacementMeta.keyword",
    documentationUrl = "/pipeline/transforms/tokenreplacement.html")
@Getter
@Setter
public class TokenReplacementMeta
    extends BaseTransformMeta<TokenReplacement, TokenReplacementData> {
  private static final Class<?> PKG = TokenReplacementMeta.class;

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

  public static final String CONST_FIELD = "field";
  public static final String[] INPUT_TYPES = {"text", CONST_FIELD, "file"};
  public static final String[] OUTPUT_TYPES = {CONST_FIELD, "file"};
  public static final String[] formatMapperLineTerminator =
      new String[] {"DOS", "UNIX", "CR", "None"};

  public static final String[] formatMapperLineTerminatorDescriptions =
      new String[] {
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.DOS"),
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.UNIX"),
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.CR"),
        BaseMessages.getString(PKG, "TokenReplacementDialog.Format.None")
      };
  public static final String CONST_FILE_ENCODING = "file.encoding";

  @HopMetadataProperty(
      key = "input_type",
      injectionKey = "INPUT_TYPE",
      injectionKeyDescription = "TokenReplacement.Injection.INPUT_TYPE")
  private String inputType;

  @HopMetadataProperty(
      key = "input_text",
      injectionKey = "INPUT_TEXT",
      injectionKeyDescription = "TokenReplacement.Injection.INPUT_TEXT")
  private String inputText;

  @HopMetadataProperty(
      key = "input_field_name",
      injectionKey = "INPUT_FIELD",
      injectionKeyDescription = "TokenReplacement.Injection.INPUT_FIELD")
  private String inputFieldName;

  @HopMetadataProperty(
      key = "input_filename",
      injectionKey = "INPUT_FILENAME",
      injectionKeyDescription = "TokenReplacement.Injection.INPUT_FILENAME")
  private String inputFileName;

  @HopMetadataProperty(
      key = "input_filename_in_field",
      injectionKey = "INPUT_FILENAME_IN_FIELD",
      injectionKeyDescription = "TokenReplacement.Injection.INPUT_FILENAME_IN_FIELD")
  private boolean inputFileNameInField;

  @HopMetadataProperty(
      key = "input_filename_field",
      injectionKey = "INPUT_FILENAME_FIELD",
      injectionKeyDescription = "TokenReplacement.Injection.INPUT_FILENAME_FIELD")
  private String inputFileNameField;

  @HopMetadataProperty(
      key = "add_input_filename_to_result",
      injectionKey = "ADD_INPUT_FILENAME_TO_RESULT",
      injectionKeyDescription = "TokenReplacement.Injection.ADD_INPUT_FILENAME_TO_RESULT")
  private boolean addInputFileNameToResult;

  @HopMetadataProperty(
      key = "output_type",
      injectionKey = "OUTPUT_TYPE",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_TYPE")
  private String outputType;

  @HopMetadataProperty(
      key = "output_field_name",
      injectionKey = "OUTPUT_FIELD",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_FIELD")
  private String outputFieldName;

  @HopMetadataProperty(
      key = "output_filename",
      injectionKey = "OUTPUT_FILENAME",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_FILENAME")
  private String outputFileName;

  @HopMetadataProperty(
      key = "output_filename_in_field",
      injectionKey = "OUTPUT_FILENAME_IN_FIELD",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_FILENAME_IN_FIELD")
  private boolean outputFileNameInField;

  @HopMetadataProperty(
      key = "output_filename_field",
      injectionKey = "OUTPUT_FILENAME_FIELD",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_FILENAME_FIELD")
  private String outputFileNameField;

  @HopMetadataProperty(
      key = "append_output_filename",
      injectionKey = "APPEND_OUTPUT_FILE",
      injectionKeyDescription = "TokenReplacement.Injection.APPEND_OUTPUT_FILE")
  private boolean appendOutputFileName;

  @HopMetadataProperty(
      key = "create_parent_folder",
      injectionKey = "CREATE_PARENT_FOLDER",
      injectionKeyDescription = "TokenReplacement.Injection.CREATE_PARENT_FOLDER")
  private boolean createParentFolder;

  @HopMetadataProperty(
      key = "output_file_format",
      injectionKey = "OUTPUT_FORMAT",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_FORMAT")
  private String outputFileFormat;

  @HopMetadataProperty(
      key = "output_file_encoding",
      injectionKey = "OUTPUT_ENCODING",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_ENCODING")
  private String outputFileEncoding;

  @HopMetadataProperty(
      key = "output_split_every",
      injectionKey = "OUTPUT_SPLIT_EVERY",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_SPLIT_EVERY")
  private int splitEvery;

  @HopMetadataProperty(
      key = "include_transform_nr_in_output_filename",
      injectionKey = "OUTPUT_INCLUDE_TRANSFORMNR",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_INCLUDE_TRANSFORMNR")
  private boolean includeTransformNrInOutputFileName;

  @HopMetadataProperty(
      key = "include_part_nr_in_output_filename",
      injectionKey = "OUTPUT_INCLUDE_PARTNR",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_INCLUDE_PARTNR")
  private boolean includePartNrInOutputFileName;

  @HopMetadataProperty(
      key = "include_date_in_output_filename",
      injectionKey = "OUTPUT_INCLUDE_DATE",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_INCLUDE_DATE")
  private boolean includeDateInOutputFileName;

  @HopMetadataProperty(
      key = "include_time_in_output_filename",
      injectionKey = "OUTPUT_INCLUDE_TIME",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_INCLUDE_TIME")
  private boolean includeTimeInOutputFileName;

  @HopMetadataProperty(
      key = "specify_date_format_output_filename",
      injectionKey = "OUTPUT_SPECIFY_DATE_FORMAT",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_SPECIFY_DATE_FORMAT")
  private boolean specifyDateFormatOutputFileName;

  @HopMetadataProperty(
      key = "date_format_output_filename",
      injectionKey = "OUTPUT_DATE_FORMAT",
      injectionKeyDescription = "TokenReplacement.Injection.OUTPUT_DATE_FORMAT")
  private String dateFormatOutputFileName;

  @HopMetadataProperty(
      key = "add_output_filename_to_result",
      injectionKey = "ADD_OUTPUT_FILENAME_TO_RESULT",
      injectionKeyDescription = "TokenReplacement.Injection.ADD_OUTPUT_FILENAME_TO_RESULT")
  private boolean addOutputFileNameToResult;

  @HopMetadataProperty(
      key = "token_start_string",
      injectionKey = "TOKEN_START_STRING",
      injectionKeyDescription = "TokenReplacement.Injection.TOKEN_START_STRING")
  private String tokenStartString;

  @HopMetadataProperty(
      key = "token_end_string",
      injectionKey = "TOKEN_END_STRING",
      injectionKeyDescription = "TokenReplacement.Injection.TOKEN_END_STRING")
  private String tokenEndString;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "OUTPUT_FIELDS",
      injectionGroupDescription = "TokenReplacement.Injection.OUTPUT_FIELDS")
  private List<TokenReplacementField> tokenReplacementFields;

  public TokenReplacementMeta() {
    super();
    tokenReplacementFields = new ArrayList<>();
    inputType = "Text";
    outputType = "Field";
    outputFileEncoding = Const.getEnvironmentVariable(CONST_FILE_ENCODING, Const.UTF_8);
    outputFileFormat = Const.isWindows() ? "DOS" : "UNIX";
    splitEvery = 0;
    tokenStartString = "${";
    tokenEndString = "}";
  }

  public TokenReplacementMeta(TokenReplacementMeta m) {
    this();
    this.inputType = m.inputType;
    this.inputText = m.inputText;
    this.inputFieldName = m.inputFieldName;
    this.inputFileName = m.inputFileName;
    this.inputFileNameInField = m.inputFileNameInField;
    this.inputFileNameField = m.inputFileNameField;
    this.addInputFileNameToResult = m.addInputFileNameToResult;
    this.outputType = m.outputType;
    this.outputFieldName = m.outputFieldName;
    this.outputFileName = m.outputFileName;
    this.outputFileNameInField = m.outputFileNameInField;
    this.outputFileNameField = m.outputFileNameField;
    this.appendOutputFileName = m.appendOutputFileName;
    this.createParentFolder = m.createParentFolder;
    this.outputFileFormat = m.outputFileFormat;
    this.outputFileEncoding = m.outputFileEncoding;
    this.splitEvery = m.splitEvery;
    this.includeTransformNrInOutputFileName = m.includeTransformNrInOutputFileName;
    this.includePartNrInOutputFileName = m.includePartNrInOutputFileName;
    this.includeDateInOutputFileName = m.includeDateInOutputFileName;
    this.includeTimeInOutputFileName = m.includeTimeInOutputFileName;
    this.specifyDateFormatOutputFileName = m.specifyDateFormatOutputFileName;
    this.dateFormatOutputFileName = m.dateFormatOutputFileName;
    this.addOutputFileNameToResult = m.addOutputFileNameToResult;
    this.tokenStartString = m.tokenStartString;
    this.tokenEndString = m.tokenEndString;
    m.tokenReplacementFields.forEach(f -> tokenReplacementFields.add(new TokenReplacementField(f)));
  }

  public String getOutputFileEncoding() {
    return Const.NVL(
        outputFileEncoding, Const.getEnvironmentVariable(CONST_FILE_ENCODING, Const.UTF_8));
  }

  public String getOutputFileFormatString() {
    return switch (outputFileFormat) {
      case "DOS" -> "\r\n";
      case "UNIX" -> "\n";
      case "CR" -> "\r";
      default -> "";
    };
  }

  @Override
  public Object clone() {
    return new TokenReplacementMeta(this);
  }

  public String buildFilename(
      String fileName, IVariables variables, int transformnr, String partnr, int splitnr) {
    return buildFilename(fileName, variables, transformnr, partnr, splitnr, this);
  }

  public String buildFilename(
      String filename,
      IVariables variables,
      int copyNr,
      String partitionId,
      int splitNr,
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
      retval += "_" + copyNr;
    }
    if (meta.isIncludePartNrInOutputFileName()) {
      retval += "_" + partitionId;
    }

    if (meta.getSplitEvery() > 0) {
      retval += "_" + splitNr;
    }

    if (!Utils.isEmpty(extension)) {
      retval += "." + extension;
    }
    return retval;
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
                  PKG, "TokenReplacementMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (TokenReplacementField tokenReplacementField : tokenReplacementFields) {
        int idx = prev.indexOfValue(tokenReplacementField.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + tokenReplacementField.getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "TokenReplacementMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
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

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    // change the case-insensitive flag too

    if (outputType.equalsIgnoreCase(CONST_FIELD)) {
      IValueMeta v = new ValueMetaString(variables.resolve(outputFieldName));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  public static final String getOutputFileFormatDescription(String code) {
    int index = Const.indexOfString(code, formatMapperLineTerminator);
    if (index < 0) {
      index = 3;
    }
    return formatMapperLineTerminatorDescriptions[index];
  }
}

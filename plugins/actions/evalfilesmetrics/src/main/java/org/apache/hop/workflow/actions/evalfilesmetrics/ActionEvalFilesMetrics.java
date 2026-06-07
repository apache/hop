/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.workflow.actions.evalfilesmetrics;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'evaluate files metrics' action. */
@Action(
    id = "EVAL_FILES_METRICS",
    name = "i18n::ActionEvalFilesMetrics.Name",
    description = "i18n::ActionEvalFilesMetrics.Description",
    image = "EvalFilesMetrics.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionEvalFilesMetrics.keyword",
    documentationUrl = "/workflow/actions/evalfilesmetrics.html")
@Getter
@Setter
@SuppressWarnings("java:S1104")
public class ActionEvalFilesMetrics extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionEvalFilesMetrics.class;

  public static final BigDecimal ONE = new BigDecimal(1);

  private static final String CONST_CAN_NOT_FIND_FIELD =
      "ActionEvalFilesMetrics.Error.CanNotFindField";
  private static final String CONST_COMPARE_WITH_VALUE =
      "ActionEvalFilesMetrics.Log.CompareWithValue";

  public static final String YES = "Y";
  public static final String NO = "N";

  public static final String[] NO_YES =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  @HopMetadataProperty(key = "scale", storeWithCode = true)
  private Scale scale;

  @HopMetadataProperty(key = "source_files", storeWithCode = true)
  private SourceFilesType sourceFilesType;

  @HopMetadataProperty(key = "evaluation_type", storeWithCode = true)
  private EvaluationType evaluationType;

  @HopMetadataProperty(key = "comparevalue")
  private String compareValue;

  @HopMetadataProperty(key = "minvalue")
  private String minValue;

  @HopMetadataProperty(key = "maxvalue")
  private String maxValue;

  @HopMetadataProperty(key = "successnumbercondition", storeWithCode = true)
  private SuccesConditionType successConditionType;

  @HopMetadataProperty(key = "result_filenames_wildcard")
  private String resultFilenamesWildcard;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<SourceFile> sourceFiles;

  @HopMetadataProperty(key = "Result_field_file")
  private String resultFieldFile;

  @HopMetadataProperty(key = "Result_field_wildcard")
  private String resultFieldWildcard;

  @HopMetadataProperty(key = "Result_field_includesubfolders")
  private String resultFieldIncludeSubFolders;

  private BigDecimal realCompareValue;
  private BigDecimal realMinValue;
  private BigDecimal realMaxValue;
  private BigDecimal realEvaluationValue;
  private BigDecimal realFilesCount;
  private long nrErrors;

  public ActionEvalFilesMetrics(String n) {
    super(n, "");
    scale = Scale.BYTES;
    sourceFilesType = SourceFilesType.FILES;
    evaluationType = EvaluationType.SIZE;
    successConditionType = SuccesConditionType.GREATER;
  }

  public ActionEvalFilesMetrics() {
    this("");
    sourceFiles = new ArrayList<>();
  }

  public ActionEvalFilesMetrics(ActionEvalFilesMetrics a) {
    super(a);
    this.scale = a.scale;
    this.sourceFilesType = a.sourceFilesType;
    this.evaluationType = a.evaluationType;
    this.compareValue = a.compareValue;
    this.minValue = a.minValue;
    this.maxValue = a.maxValue;
    this.successConditionType = a.successConditionType;
    this.resultFilenamesWildcard = a.resultFilenamesWildcard;
    this.resultFieldFile = a.resultFieldFile;
    this.resultFieldWildcard = a.resultFieldWildcard;
    this.resultFieldIncludeSubFolders = a.resultFieldIncludeSubFolders;
    this.realCompareValue = a.realCompareValue;
    this.realMinValue = a.realMinValue;
    this.realMaxValue = a.realMaxValue;
    this.realEvaluationValue = a.realEvaluationValue;
    this.realFilesCount = a.realFilesCount;
    this.nrErrors = a.nrErrors;
    this.sourceFiles = new ArrayList<>();
    a.sourceFiles.forEach(f -> this.sourceFiles.add(new SourceFile(f)));
  }

  @Override
  public Object clone() {
    return new ActionEvalFilesMetrics(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setNrErrors(1);
    result.setResult(false);

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow;

    try {
      initMetrics();
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Error.Init", e.toString()));
      return result;
    }

    // Get source and destination files, also wildcard
    List<SourceFile> vSourceFiles = new ArrayList<>();
    sourceFiles.forEach(f -> vSourceFiles.add(new SourceFile(f)));

    switch (getSourceFilesType()) {
      case PREVIOUS_RESULT:
        // Filenames are retrieved from previous result rows
        //
        String realResultFieldFile = resolve(getResultFieldFile());
        String realResultFieldWildcard = resolve(getResultFieldWildcard());
        String realResultFieldIncludeSubfolders = resolve(getResultFieldIncludeSubFolders());

        int indexOfResultFieldFile;
        if (Utils.isEmpty(realResultFieldFile)) {
          logError(
              BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Error.ResultFieldsFileMissing"));
          return result;
        }

        int indexOfResultFieldWildcard = -1;
        int indexOfResultFieldIncludeSubfolders = -1;

        // as such we must get rows
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionEvalFilesMetrics.Log.ArgFromPrevious.Found",
                  (rows != null ? rows.size() : 0) + ""));
        }

        if (rows != null && !rows.isEmpty()) {
          // We get rows
          RowMetaAndData firstRow = rows.getFirst();
          indexOfResultFieldFile = firstRow.getRowMeta().indexOfValue(realResultFieldFile);
          if (indexOfResultFieldFile == -1) {
            logError(BaseMessages.getString(PKG, CONST_CAN_NOT_FIND_FIELD, realResultFieldFile));
            return result;
          }
          if (StringUtils.isNotEmpty(realResultFieldWildcard)) {
            indexOfResultFieldWildcard =
                firstRow.getRowMeta().indexOfValue(realResultFieldWildcard);
            if (indexOfResultFieldWildcard == -1) {
              logError(
                  BaseMessages.getString(PKG, CONST_CAN_NOT_FIND_FIELD, realResultFieldWildcard));
              return result;
            }
          }
          if (StringUtils.isNotEmpty(realResultFieldIncludeSubfolders)) {
            indexOfResultFieldIncludeSubfolders =
                firstRow.getRowMeta().indexOfValue(realResultFieldIncludeSubfolders);
            if (indexOfResultFieldIncludeSubfolders == -1) {
              logError(
                  BaseMessages.getString(
                      PKG, CONST_CAN_NOT_FIND_FIELD, realResultFieldIncludeSubfolders));
              return result;
            }
          }

          for (int iteration = 0;
              iteration < rows.size() && !parentWorkflow.isStopped();
              iteration++) {

            resultRow = rows.get(iteration);

            // Get source and destination file names, also wildcard
            String vSourceFileFolderPrevious = resultRow.getString(indexOfResultFieldFile, null);
            String vWildcardPrevious = null;
            if (indexOfResultFieldWildcard > -1) {
              vWildcardPrevious = resultRow.getString(indexOfResultFieldWildcard, null);
            }
            String vincludeSubFoldersPrevious = NO;
            if (indexOfResultFieldIncludeSubfolders > -1) {
              vincludeSubFoldersPrevious =
                  resultRow.getString(indexOfResultFieldIncludeSubfolders, NO);
            }

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionEvalFilesMetrics.Log.ProcessingRow",
                      vSourceFileFolderPrevious,
                      vWildcardPrevious));
            }

            processFileFolder(
                vSourceFileFolderPrevious,
                vWildcardPrevious,
                vincludeSubFoldersPrevious,
                parentWorkflow);
          }
        }

        break;
      case FILENAMES_RESULT:
        List<ResultFile> resultFiles = result.getResultFilesList();
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionEvalFilesMetrics.Log.ResultFilenames.Found",
                  (resultFiles != null ? resultFiles.size() : 0) + ""));
        }

        if (!Utils.isEmpty(resultFiles)) {
          // Let's check wildcard
          Pattern pattern = null;
          String realPattern = resolve(getResultFilenamesWildcard());
          if (!Utils.isEmpty(realPattern)) {
            pattern = Pattern.compile(realPattern);
          }

          for (Iterator<ResultFile> it = resultFiles.iterator();
              it.hasNext() && !parentWorkflow.isStopped(); ) {
            ResultFile resultFile = it.next();
            FileObject file = resultFile.getFile();
            try {
              if (file != null && file.exists()) {
                boolean getIt = true;
                if (pattern != null) {
                  Matcher matcher = pattern.matcher(file.getName().getBaseName());
                  getIt = matcher.matches();
                }
                if (getIt) {
                  getFileSize(file);
                }
              }
            } catch (Exception e) {
              incrementErrors();
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionEvalFilesMetrics.Error.GettingFileFromResultFilenames",
                      file.toString(),
                      e.toString()));
            } finally {
              if (file != null) {
                try {
                  file.close();
                } catch (Exception e) {
                  /* Ignore */
                }
              }
            }
          }
        }
        break;
      default:
        // static files/folders
        // from grid entered by user
        if (!vSourceFiles.isEmpty()) {
          for (SourceFile vSourceFile : vSourceFiles) {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionEvalFilesMetrics.Log.ProcessingRow",
                      vSourceFile.getSourceFileFolder(),
                      vSourceFile.getSourceWildcard()));
            }

            processFileFolder(
                vSourceFile.getSourceFileFolder(),
                vSourceFile.getSourceWildcard(),
                vSourceFile.getSourceIncludeSubfolders(),
                parentWorkflow);
          }
        } else {
          logError(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Error.FilesGridEmpty"));
          return result;
        }
        break;
    }

    result.setResult(isSuccess());
    result.setNrErrors(getNrErrors());
    displayResults();

    return result;
  }

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionEvalFilesMetrics.Log.Info.FilesCount",
              String.valueOf(getRealFilesCount())));
      if (evaluationType == EvaluationType.SIZE) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionEvalFilesMetrics.Log.Info.FilesSize",
                String.valueOf(getRealEvaluationValue())));
      }
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionEvalFilesMetrics.Log.Info.NrErrors", String.valueOf(getNrErrors())));
      logDetailed("=======================================");
    }
  }

  private boolean isSuccess() {
    boolean retval = false;

    switch (successConditionType) {
      case EQUAL: // equal
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  CONST_COMPARE_WITH_VALUE,
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realCompareValue)));
        }
        retval = (getRealEvaluationValue().compareTo(realCompareValue) == 0);
        break;
      case DIFFERENT: // different
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  CONST_COMPARE_WITH_VALUE,
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realCompareValue)));
        }
        retval = (getRealEvaluationValue().compareTo(realCompareValue) != 0);
        break;
      case SMALLER: // smaller
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  CONST_COMPARE_WITH_VALUE,
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realCompareValue)));
        }
        retval = (getRealEvaluationValue().compareTo(realCompareValue) < 0);
        break;
      case SMALLER_EQUAL: // smaller or equal
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  CONST_COMPARE_WITH_VALUE,
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realCompareValue)));
        }
        retval = (getRealEvaluationValue().compareTo(realCompareValue) <= 0);
        break;
      case GREATER: // greater
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  CONST_COMPARE_WITH_VALUE,
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realCompareValue)));
        }
        retval = (getRealEvaluationValue().compareTo(realCompareValue) > 0);
        break;
      case GREATER_EQUAL: // greater or equal
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  CONST_COMPARE_WITH_VALUE,
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realCompareValue)));
        }
        retval = (getRealEvaluationValue().compareTo(realCompareValue) >= 0);
        break;
      case BETWEEN: // between min and max
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionEvalFilesMetrics.Log.CompareWithValues",
                  String.valueOf(realEvaluationValue),
                  String.valueOf(realMinValue),
                  String.valueOf(realMaxValue)));
        }
        retval =
            (getRealEvaluationValue().compareTo(realMinValue) >= 0
                && getRealEvaluationValue().compareTo(realMaxValue) <= 0);
        break;
      default:
        break;
    }

    return retval;
  }

  private void initMetrics() {
    realEvaluationValue = new BigDecimal(0);
    realFilesCount = new BigDecimal(0);
    nrErrors = 0;

    if (successConditionType == SuccesConditionType.BETWEEN) {
      realMinValue = new BigDecimal(resolve(getMinValue()));
      realMaxValue = new BigDecimal(resolve(getMaxValue()));
    } else {
      realCompareValue = new BigDecimal(resolve(getCompareValue()));
    }

    if (evaluationType == EvaluationType.SIZE) {
      int multyply = 1;
      switch (getScale()) {
        case KBYTES:
          multyply = 1024;
          break;
        case MBYTES:
          multyply = 1048576;
          break;
        case GBYTES:
          multyply = 1073741824;
          break;
        default:
          break;
      }

      if (successConditionType == SuccesConditionType.BETWEEN) {
        realMinValue = realMinValue.multiply(BigDecimal.valueOf(multyply));
        realMaxValue = realMaxValue.multiply(BigDecimal.valueOf(multyply));
      } else {
        realCompareValue = realCompareValue.multiply(BigDecimal.valueOf(multyply));
      }
    }
  }

  private void incrementErrors() {
    nrErrors++;
  }

  private void processFileFolder(
      String sourcefilefoldername,
      String wildcard,
      String includeSubfolders,
      IWorkflowEngine<WorkflowMeta> parentWorkflow) {

    FileObject sourcefilefolder = null;
    FileObject currentFile = null;

    // Get real source file and wildcard
    String realSourceFilefoldername = resolve(sourcefilefoldername);
    if (Utils.isEmpty(realSourceFilefoldername)) {
      // Filename is empty!
      logError(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.log.FileFolderEmpty"));
      incrementErrors();
      return;
    }
    String realWildcard = resolve(wildcard);
    final boolean includeSubFolders = YES.equalsIgnoreCase(includeSubfolders);

    try {
      sourcefilefolder = HopVfs.getFileObject(realSourceFilefoldername, getVariables());

      if (sourcefilefolder.exists()) {
        // File exists
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionEvalFilesMetrics.Log.FileExists", sourcefilefolder.toString()));
        }

        if (sourcefilefolder.getType() == FileType.FILE) {
          // We deals here with a file
          // let's get file size
          getFileSize(sourcefilefolder);

        } else if (sourcefilefolder.getType() == FileType.FOLDER) {
          // We have a folder
          // we will fetch and extract files
          FileObject[] fileObjects =
              sourcefilefolder.findFiles(
                  new AllFileSelector() {
                    @Override
                    public boolean traverseDescendents(FileSelectInfo info) {
                      return info.getDepth() == 0 || includeSubFolders;
                    }

                    @Override
                    public boolean includeFile(FileSelectInfo info) {
                      FileObject fileObject = info.getFile();
                      try {
                        if (fileObject == null) {
                          return false;
                        }
                        if (fileObject.getType() != FileType.FILE) {
                          return false;
                        }
                      } catch (Exception ex) {
                        // Upon error don't process the file.
                        return false;
                      } finally {
                        if (fileObject != null) {
                          try {
                            fileObject.close();
                          } catch (IOException ex) {
                            /* Ignore */
                          }
                        }
                      }
                      return true;
                    }
                  });

          if (fileObjects != null) {
            for (int j = 0; j < fileObjects.length && !parentWorkflow.isStopped(); j++) {
              // Fetch files in list one after one ...
              currentFile = fileObjects[j];

              if (!currentFile.getParent().toString().equals(sourcefilefolder.toString())) {
                // Not in the Base Folder..Only if include sub folders
                if (includeSubFolders
                    && getFileWildcard(currentFile.getName().getBaseName(), realWildcard)) {
                  getFileSize(currentFile);
                }
              } else {
                // In the base folder
                if (getFileWildcard(currentFile.getName().getBaseName(), realWildcard)) {
                  getFileSize(currentFile);
                }
              }
            }
          }
        } else {
          incrementErrors();
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionEvalFilesMetrics.Error.UnknowFileFormat",
                  sourcefilefolder.toString()));
        }
      } else {
        incrementErrors();
        logError(
            BaseMessages.getString(
                PKG, "ActionEvalFilesMetrics.Error.SourceFileNotExists", realSourceFilefoldername));
      }
    } catch (Exception e) {
      incrementErrors();
      logError(
          BaseMessages.getString(
              PKG,
              "ActionEvalFilesMetrics.Error.Exception.Processing",
              realSourceFilefoldername,
              e.getMessage()));

    } finally {
      if (sourcefilefolder != null) {
        try {
          sourcefilefolder.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (currentFile != null) {
        try {
          currentFile.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
  }

  private void getFileSize(FileObject file) {
    try {
      // Count every processed file (both SIZE and COUNT evaluation types); this running total is
      // surfaced in the logs. The @HopMetadataProperty rewrite dropped this increment, leaving the
      // reported file count stuck at 0.
      realFilesCount = realFilesCount.add(ONE);
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionEvalFilesMetrics.Log.GetFile",
                file.toString(),
                String.valueOf(getRealFilesCount())));
      }
      if (evaluationType == EvaluationType.SIZE) {
        BigDecimal fileSize = BigDecimal.valueOf(file.getContent().getSize());
        realEvaluationValue = realEvaluationValue.add(fileSize);
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionEvalFilesMetrics.Log.AddedFileSize",
                  String.valueOf(fileSize),
                  file.toString()));
        }
      } else {
        realEvaluationValue = realEvaluationValue.add(ONE);
      }
    } catch (Exception e) {
      incrementErrors();
      logError(
          BaseMessages.getString(
              PKG, "ActionEvalFilesMetrics.Error.GettingFileSize", file.toString(), e.toString()));
    }
  }

  /**
   * @param selectedFile The selected file
   * @param wildcard The wildcard
   * @return True if the selected file matches the wildcard
   */
  private boolean getFileWildcard(String selectedFile, String wildcard) {
    Pattern pattern;
    boolean getIt = true;

    if (!Utils.isEmpty(wildcard)) {
      pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
      Matcher matcher = pattern.matcher(selectedFile);
      getIt = matcher.matches();
    }
    return getIt;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    boolean res =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "arguments",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < sourceFiles.size(); i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + (i++) + "]", remarks, ctx);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Getter
  public enum Scale implements IEnumHasCodeAndDescription {
    BYTES("bytes", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Bytes.Label")),
    KBYTES("kbytes", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.KBytes.Label")),
    MBYTES("mbytes", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.MBytes.Label")),
    GBYTES("gbytes", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.GBytes.Label")),
    ;
    final String code;
    final String description;

    Scale(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(Scale.class);
    }

    public static Scale lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(Scale.class, description, BYTES);
    }
  }

  @Getter
  public enum SuccesConditionType implements IEnumHasCodeAndDescription {
    EQUAL("equal", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenEqual.Label")),
    DIFFERENT(
        "different",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenDifferent.Label")),
    SMALLER(
        "smaller",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenSmallThan.Label")),
    SMALLER_EQUAL(
        "smallequal",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenSmallOrEqualThan.Label")),
    GREATER(
        "greater",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenGreaterThan.Label")),
    GREATER_EQUAL(
        "greaterequal",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenGreaterOrEqualThan.Label")),
    BETWEEN("between", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessBetween.Label")),
    IN_LIST(
        "inlist", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenInList.Label")),
    NOT_IN_LIST(
        "notinlist",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessWhenNotInList.Label")),
    ;
    final String code;
    final String description;

    SuccesConditionType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SuccesConditionType.class);
    }

    public static SuccesConditionType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SuccesConditionType.class, description, EQUAL);
    }
  }

  @Getter
  public enum EvaluationType implements IEnumHasCodeAndDescription {
    SIZE("size", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.EvaluationType.Size.Label")),
    COUNT(
        "count", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.EvaluationType.Count.Label"));
    final String code;
    final String description;

    EvaluationType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(EvaluationType.class);
    }

    public static EvaluationType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(EvaluationType.class, description, SIZE);
    }
  }

  @Getter
  public enum SourceFilesType implements IEnumHasCodeAndDescription {
    FILES("files", BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SourceFiles.Files.Label")),
    FILENAMES_RESULT(
        "filenamesresult",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SourceFiles.FilenamesResult.Label")),
    PREVIOUS_RESULT(
        "previousresult",
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SourceFiles.PreviousResult.Label")),
    ;
    final String code;
    final String description;

    SourceFilesType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SourceFilesType.class);
    }

    public static SourceFilesType lookupDescriptions(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SourceFilesType.class, description, FILES);
    }
  }

  @Getter
  @Setter
  public static final class SourceFile {
    @HopMetadataProperty(key = "source_filefolder")
    private String sourceFileFolder;

    @HopMetadataProperty(key = "wildcard")
    private String sourceWildcard;

    @HopMetadataProperty(key = "include_subFolders")
    private String sourceIncludeSubfolders;

    public SourceFile() {}

    public SourceFile(SourceFile f) {
      this();
      this.sourceFileFolder = f.sourceFileFolder;
      this.sourceWildcard = f.sourceWildcard;
      this.sourceIncludeSubfolders = f.sourceIncludeSubfolders;
    }
  }
}

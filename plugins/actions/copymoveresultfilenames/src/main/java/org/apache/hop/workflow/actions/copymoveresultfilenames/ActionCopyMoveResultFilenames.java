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

package org.apache.hop.workflow.actions.copymoveresultfilenames;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.util.FileObjectUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
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

/**
 * This defines a 'copymoveresultfilenames' action. Its main use would be to copy or move files in
 * the result filenames to a destination folder. that can be used to control the flow in ETL cycles.
 */
@Action(
    id = "COPY_MOVE_RESULT_FILENAMES",
    name = "i18n::ActionCopyMoveResultFilenames.Name",
    description = "i18n::ActionCopyMoveResultFilenames.Description",
    image = "CopyMoveResultFilenames.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionCopyMoveResultFilenames.keyword",
    documentationUrl = "/workflow/actions/processresultfilenames.html")
@Getter
@Setter
public class ActionCopyMoveResultFilenames extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCopyMoveResultFilenames.class;
  public static final String SUCCESS_IF_AT_LEAST_X_FILES = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  @Getter
  public enum ActionType implements IEnumHasCodeAndDescription {
    COPY("copy", BaseMessages.getString(PKG, "ActionCopyMoveResultFilenames.Copy.Label")),
    MOVE("move", BaseMessages.getString(PKG, "ActionCopyMoveResultFilenames.Move.Label")),
    DELETE("delete", BaseMessages.getString(PKG, "ActionCopyMoveResultFilenames.Delete.Label")),
    ;
    final String code;
    final String description;

    ActionType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(ActionType.class);
    }

    public ActionType lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(ActionType.class, description, COPY);
    }
  }

  @HopMetadataProperty(key = "foldername")
  private String folderName;

  @HopMetadataProperty(key = "specify_wildcard")
  private boolean specifyWildcard;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "wildcardexclude")
  private String wildcardExclude;

  @HopMetadataProperty(key = "destination_folder")
  private String destinationFolder;

  @HopMetadataProperty(key = "nr_errors_less_than")
  private String nrErrorsLessThan;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  @HopMetadataProperty(key = "add_date")
  private boolean addDate;

  @HopMetadataProperty(key = "add_time")
  private boolean addTime;

  @HopMetadataProperty(key = "SpecifyFormat")
  private boolean specifyFormat;

  @HopMetadataProperty(key = "date_time_format")
  private String dateTimeFormat;

  @HopMetadataProperty(key = "AddDateBeforeExtension")
  private boolean addDateBeforeExtension;

  @HopMetadataProperty(key = "action", storeWithCode = true)
  private ActionType action;

  @HopMetadataProperty(key = "OverwriteFile")
  private boolean overwriteFile;

  @HopMetadataProperty(key = "CreateDestinationFolder")
  private boolean createDestinationFolder;

  @HopMetadataProperty(key = "RemovedSourceFilename")
  boolean removedSourceFilename;

  @HopMetadataProperty(key = "AddDestinationFilename")
  boolean addDestinationFilename;

  private int nrErrors = 0;
  private int nrSuccess = 0;
  private boolean successConditionBroken = false;
  private int limitFiles = 0;
  private Pattern wildcardPattern;
  private Pattern wildcardExcludePattern;

  public ActionCopyMoveResultFilenames(String n) {
    super(n, "");
    removedSourceFilename = true;
    addDestinationFilename = true;
    nrErrorsLessThan = "10";
    action = ActionType.COPY;
    successCondition = SUCCESS_IF_NO_ERRORS;
  }

  public ActionCopyMoveResultFilenames() {
    this("");
  }

  public ActionCopyMoveResultFilenames(ActionCopyMoveResultFilenames a) {
    super(a);
    this.folderName = a.folderName;
    this.specifyWildcard = a.specifyWildcard;
    this.wildcard = a.wildcard;
    this.wildcardExclude = a.wildcardExclude;
    this.destinationFolder = a.destinationFolder;
    this.nrErrorsLessThan = a.nrErrorsLessThan;
    this.successCondition = a.successCondition;
    this.addDate = a.addDate;
    this.addTime = a.addTime;
    this.specifyFormat = a.specifyFormat;
    this.dateTimeFormat = a.dateTimeFormat;
    this.addDateBeforeExtension = a.addDateBeforeExtension;
    this.action = a.action;
    this.overwriteFile = a.overwriteFile;
    this.createDestinationFolder = a.createDestinationFolder;
    this.removedSourceFilename = a.removedSourceFilename;
    this.addDestinationFilename = a.addDestinationFilename;

    this.nrErrors = 0;
    this.nrSuccess = 0;
    this.successConditionBroken = false;
    this.limitFiles = 0;
    this.wildcardPattern = null;
    this.wildcardExcludePattern = null;
  }

  @Override
  public Object clone() {
    return new ActionCopyMoveResultFilenames(this);
  }

  @Override
  public Result execute(Result result, int nr) {
    result.setNrErrors(1);
    result.setResult(false);

    boolean deleteFile = getAction() == ActionType.DELETE;

    String realdestinationFolder = null;
    if (!deleteFile) {
      realdestinationFolder = resolve(getDestinationFolder());

      if (!createDestinationFolder(realdestinationFolder)) {
        return result;
      }
    }
    if (!Utils.isEmpty(wildcard)) {
      wildcardPattern = Pattern.compile(resolve(wildcard));
    }
    if (!Utils.isEmpty(wildcardExclude)) {
      wildcardExcludePattern = Pattern.compile(resolve(wildcardExclude));
    }

    nrErrors = 0;
    limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);
    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;

    FileObject file = null;

    try {
      int size = result.getResultFiles().size();
      if (isBasic()) {
        logBasic(
            BaseMessages.getString(PKG, "ActionCopyMoveResultFilenames.log.FilesFound", "" + size));
      }

      List<ResultFile> resultFiles = result.getResultFilesList();
      if (!Utils.isEmpty(resultFiles)) {
        for (Iterator<ResultFile> it = resultFiles.iterator();
            it.hasNext() && !parentWorkflow.isStopped(); ) {
          if (successConditionBroken) {
            logError(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyMoveResultFilenames.Error.SuccessConditionbroken",
                    "" + nrErrors));
            throw new HopException(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyMoveResultFilenames.Error.SuccessConditionbroken",
                    "" + nrErrors));
          }

          ResultFile resultFile = it.next();
          file = resultFile.getFile();
          if (file != null && file.exists()) {
            if ((!specifyWildcard
                    || (checkFileWildcard(file.getName().getBaseName(), wildcardPattern, true)
                        && !checkFileWildcard(
                            file.getName().getBaseName(), wildcardExcludePattern, false)
                        && specifyWildcard))
                && !processFile(file, realdestinationFolder, result, parentWorkflow, deleteFile)) {
              // Update Errors
              updateErrors();
            }

          } else {
            logError(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyMoveResultFilenames.log.ErrorCanNotFindFile",
                    file == null ? "" : file.toString()));
            // Update Errors
            updateErrors();
          }
        } // end for
      }
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionCopyMoveResultFilenames.Error", e.toString()));
    } finally {
      if (file != null) {
        try {
          file.close();
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
    // Success Condition
    result.setNrErrors(nrErrors);
    result.setNrLinesWritten(nrSuccess);
    if (getSuccessStatus()) {
      result.setResult(true);
    }

    return result;
  }

  private void updateErrors() {
    nrErrors++;
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    return (nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
  }

  private boolean getSuccessStatus() {
    return (nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrSuccess >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
  }

  private boolean createDestinationFolder(String folderName) {
    try (FileObject folder = HopVfs.getFileObject(folderName)) {
      if (!folder.exists()) {
        logError(
            BaseMessages.getString(
                PKG, "ActionCopyMoveResultFilenames.Log.FolderNotExists", folderName));
        if (isCreateDestinationFolder()) {
          folder.createFolder();
        } else {
          return false;
        }
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.Log.FolderCreated", folderName));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.Log.FolderExists", folderName));
        }
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionCopyMoveResultFilenames.Log.CanNotCreatedFolder",
              folderName,
              e.toString()));
    }
    return false;
  }

  private boolean processFile(
      FileObject sourceFile,
      String destinationFolder,
      Result result,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      boolean deleteFile) {
    boolean retval = false;

    try {
      if (deleteFile) {
        // delete file
        if (sourceFile.delete()) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCopyMoveResultFilenames.log.DeletedFile", sourceFile.toString()));
          }

          // Remove source file from result files list
          result.getResultFiles().remove(sourceFile.toString());
          nrSuccess++;
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyMoveResultFilenames.RemovedFileFromResult",
                    sourceFile.toString()));
          }

        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.CanNotDeletedFile", sourceFile.toString()));
        }
      } else {
        // return destination short filename
        String shortFilename = getDestinationFilename(sourceFile.getName().getBaseName());
        // build full destination filename
        String destinationFilename = destinationFolder + Const.FILE_SEPARATOR + shortFilename;
        FileObject destinationFile = HopVfs.getFileObject(destinationFilename);
        boolean fileExists = destinationFile.exists();
        if (fileExists && isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.Log.FileExists", destinationFilename));
        }
        if (!fileExists || isOverwriteFile()) {
          if (getAction() == ActionType.COPY) {
            // Copy file
            FileObjectUtils.writeContent(sourceFile, destinationFile);
            nrSuccess++;
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.log.CopiedFile",
                      sourceFile.toString(),
                      destinationFolder));
            }
          } else {
            // Move file
            sourceFile.moveTo(destinationFile);
            nrSuccess++;
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.log.MovedFile",
                      sourceFile.toString(),
                      destinationFolder));
            }
          }
          if (isRemovedSourceFilename()) {
            // Remove source file from result files list
            result.getResultFiles().remove(sourceFile.toString());
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.RemovedFileFromResult",
                      sourceFile.toString()));
            }
          }
          if (isAddDestinationFilename()) {
            // Add destination filename to Resultfilenames ...
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    HopVfs.getFileObject(destinationFile.toString()),
                    parentWorkflow.getWorkflowName(),
                    toString());
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.AddedFileToResult",
                      destinationFile.toString()));
            }
          }
        }
      }
      retval = true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionCopyMoveResultFilenames.Log.ErrorProcessing", e.toString()));
    }

    return retval;
  }

  private String getDestinationFilename(String shortSourceFilename) {
    String shortfilename = shortSourceFilename;
    int stringLength = shortSourceFilename.length();
    int lastIndexOfDot = shortfilename.lastIndexOf('.');
    if (isAddDateBeforeExtension()) {
      shortfilename = shortfilename.substring(0, lastIndexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (isSpecifyFormat() && !Utils.isEmpty(getDateTimeFormat())) {
      daf.applyPattern(getDateTimeFormat());
      String dt = daf.format(now);
      shortfilename += dt;
    } else {
      if (isAddDate()) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        shortfilename += "_" + d;
      }
      if (isAddTime()) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        shortfilename += "_" + t;
      }
    }
    if (isAddDateBeforeExtension()) {
      shortfilename += shortSourceFilename.substring(lastIndexOfDot, stringLength);
    }

    return shortfilename;
  }

  /**
   * @param selectedFile the selected file
   * @param pattern The pattern
   * @param include Include
   * @return True if the selectedFile matches the wildcard
   */
  private boolean checkFileWildcard(String selectedFile, Pattern pattern, boolean include) {
    boolean getIt = include;
    if (pattern != null) {
      Matcher matcher = pattern.matcher(selectedFile);
      getIt = matcher.matches();
    }
    return getIt;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx,
        ActionValidatorUtils.notNullValidator(),
        ActionValidatorUtils.fileDoesNotExistValidator());
    ActionValidatorUtils.andValidator().validate(this, "filename", remarks, ctx);
  }
}

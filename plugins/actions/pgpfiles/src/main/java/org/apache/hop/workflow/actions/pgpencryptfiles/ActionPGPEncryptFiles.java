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

package org.apache.hop.workflow.actions.pgpencryptfiles;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
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
import org.jspecify.annotations.Nullable;

/** This defines a 'PGP decrypt files' action. */
@Action(
    id = "PGP_ENCRYPT_FILES",
    name = "i18n::ActionPGPEncryptFiles.Name",
    description = "i18n::ActionPGPEncryptFiles.Description",
    image = "PGPEncryptFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileEncryption",
    keywords = "i18n::ActionPGPEncryptFiles.keyword",
    documentationUrl = "/workflow/actions/pgpencryptfiles.html")
@Getter
@Setter
@SuppressWarnings("java:S1104")
public class ActionPGPEncryptFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionPGPEncryptFiles.class;

  public static final int ACTION_TYPE_ENCRYPT = 0;
  public static final int ACTION_TYPE_SIGN = 1;
  public static final int ACTION_TYPE_SIGN_AND_ENCRYPT = 2;
  public static final String CONST_SPACES_LONG = "          ";
  public static final String CONST_ACTION_PGPENCRYPT_FILES_ERROR_SUCCESS_CONDITIONBROKEN =
      "ActionPGPEncryptFiles.Error.SuccessConditionbroken";
  public static final String CONST_ACTION_PGPENCRYPT_FILES_ERROR_GETTING_FILENAME =
      "ActionPGPEncryptFiles.Error.GettingFilename";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_DO_NOTHING = "do_nothing";
  public static final String CONST_ACTION_PGPENCRYPT_FILES_LOG_FILE_ENCRYPTED =
      "ActionPGPEncryptFiles.Log.FileEncrypted";

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty(key = "add_result_filesname")
  private boolean addResultFileNames;

  @HopMetadataProperty(key = "destination_is_a_file")
  private boolean destinationIsAFile;

  @HopMetadataProperty(key = "create_destination_folder")
  private boolean createDestinationFolder;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<PgpFile> pgpFiles;

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

  @HopMetadataProperty(key = "DoNotKeepFolderStructure")
  private boolean doNotKeepFolderStructure;

  @HopMetadataProperty(key = "iffileexists")
  private String ifFileExists;

  @HopMetadataProperty(key = "destinationFolder")
  private String destinationFolder;

  @HopMetadataProperty(key = "ifmovedfileexists")
  private String ifMovedFileExists;

  @HopMetadataProperty(key = "moved_date_time_format")
  private String movedDateTimeFormat;

  @HopMetadataProperty(key = "AddMovedDateBeforeExtension")
  private boolean addMovedDateBeforeExtension;

  @HopMetadataProperty(key = "add_moved_date")
  private boolean addMovedDate;

  @HopMetadataProperty(key = "add_moved_time")
  private boolean addMovedTime;

  @HopMetadataProperty(key = "SpecifyMoveFormat")
  private boolean specifyMoveFormat;

  @HopMetadataProperty(key = "create_move_to_folder")
  private boolean createMoveToFolder;

  @HopMetadataProperty(key = "gpglocation")
  private String gpgLocation;

  @HopMetadataProperty(key = "asciiMode")
  private boolean asciiMode;

  private SimpleDateFormat daf;
  private GPG gpg;

  private int nrErrors = 0;
  private int nrSuccess = 0;
  private boolean successConditionBroken = false;
  private boolean successConditionBrokenExit = false;
  private int limitFiles = 0;

  public ActionPGPEncryptFiles(String n) {
    super(n, "");
    pgpFiles = new ArrayList<>();
    ifMovedFileExists = CONST_DO_NOTHING;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
    ifFileExists = CONST_DO_NOTHING;
  }

  public ActionPGPEncryptFiles() {
    this("");
  }

  public ActionPGPEncryptFiles(ActionPGPEncryptFiles a) {
    super(a);
    this.argFromPrevious = a.argFromPrevious;
    this.includeSubFolders = a.includeSubFolders;
    this.addResultFileNames = a.addResultFileNames;
    this.destinationIsAFile = a.destinationIsAFile;
    this.createDestinationFolder = a.createDestinationFolder;
    this.nrErrorsLessThan = a.nrErrorsLessThan;
    this.successCondition = a.successCondition;
    this.addDate = a.addDate;
    this.addTime = a.addTime;
    this.specifyFormat = a.specifyFormat;
    this.dateTimeFormat = a.dateTimeFormat;
    this.addDateBeforeExtension = a.addDateBeforeExtension;
    this.doNotKeepFolderStructure = a.doNotKeepFolderStructure;
    this.ifFileExists = a.ifFileExists;
    this.destinationFolder = a.destinationFolder;
    this.ifMovedFileExists = a.ifMovedFileExists;
    this.movedDateTimeFormat = a.movedDateTimeFormat;
    this.addMovedDateBeforeExtension = a.addMovedDateBeforeExtension;
    this.addMovedDate = a.addMovedDate;
    this.addMovedTime = a.addMovedTime;
    this.specifyMoveFormat = a.specifyMoveFormat;
    this.createMoveToFolder = a.createMoveToFolder;
    this.gpgLocation = a.gpgLocation;
    this.asciiMode = a.asciiMode;
    this.daf = a.daf;
    this.gpg = a.gpg;
    this.nrErrors = a.nrErrors;
    this.nrSuccess = a.nrSuccess;
    this.successConditionBroken = a.successConditionBroken;
    this.successConditionBrokenExit = a.successConditionBrokenExit;
    this.limitFiles = a.limitFiles;
    this.pgpFiles = new ArrayList<>();
    if (a.pgpFiles != null) {
      a.pgpFiles.forEach(f -> this.pgpFiles.add(new PgpFile(f)));
    }
  }

  @Override
  public Object clone() {
    return new ActionPGPEncryptFiles(this);
  }

  @Override
  public Result execute(Result result, int nr) {
    result.setNrErrors(1);
    result.setResult(false);

    try {
      nrErrors = 0;
      nrSuccess = 0;
      successConditionBroken = false;
      successConditionBrokenExit = false;
      limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);

      if (includeSubFolders && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Log.IncludeSubFoldersOn"));
      }

      String moveToFolder = resolve(destinationFolder);
      // Get source and destination files, also wildcard
      List<PgpFile> vPgpFiles = new ArrayList<>();
      pgpFiles.forEach(f -> vPgpFiles.add(new PgpFile(f)));

      if (ifFileExists.equals("move_file")) {
        if (Utils.isEmpty(moveToFolder)) {
          logError(
              toString(),
              BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Log.Error.MoveToFolderMissing"));
          return result;
        }
        Result createFolderResult = createMoveToFolder(result, moveToFolder);
        if (createFolderResult != null) {
          return createFolderResult;
        }
      }

      gpg = new GPG(resolve(gpgLocation), getLogChannel(), getVariables());

      List<RowMetaAndData> rows = result.getRows();
      if (argFromPrevious && rows != null) {
        Result previousRowsResult = processPreviousResultRows(result, rows, moveToFolder);
        if (previousRowsResult != null) {
          return previousRowsResult;
        }
      } else {
        Result pgpResult = processPgpFiles(result, vPgpFiles, moveToFolder);
        if (pgpResult != null) {
          return pgpResult;
        }
      }
    } catch (Exception e) {
      updateErrors();
      logError(BaseMessages.getString("ActionPGPEncryptFiles.Error", e.getMessage()));
    }

    // Success Condition
    result.setNrErrors(nrErrors);
    result.setNrLinesWritten(nrSuccess);
    if (getSuccessStatus()) {
      result.setResult(true);
    }

    displayResults();

    return result;
  }

  private @Nullable Result processPgpFiles(
      Result result, List<PgpFile> vPgpFiles, String moveToFolder) {
    for (PgpFile vPgpFile : vPgpFiles) {
      if (parentWorkflow.isStopped()) {
        break;
      }
      // Success condition broken?
      if (successConditionBroken) {
        if (!successConditionBrokenExit) {
          logError(
              BaseMessages.getString(
                  PKG, CONST_ACTION_PGPENCRYPT_FILES_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
          successConditionBrokenExit = true;
        }
        result.setNrErrors(nrErrors);
        displayResults();
        return result;
      }

      if (!Utils.isEmpty(vPgpFile.getSourceFileFolder())
          && !Utils.isEmpty(vPgpFile.getDestinationFileFolder())) {
        // ok we can process this file/folder
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionPGPEncryptFiles.Log.ProcessingRow",
                  vPgpFile.getSourceFileFolder(),
                  vPgpFile.getDestinationFileFolder(),
                  vPgpFile.getWildcard()));
        }

        if (!processFileFolder(
            vPgpFile.getActionType(),
            vPgpFile.getSourceFileFolder(),
            vPgpFile.getUserId(),
            vPgpFile.getDestinationFileFolder(),
            vPgpFile.getWildcard(),
            parentWorkflow,
            result,
            moveToFolder)) {
          // Update Errors
          updateErrors();
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionPGPEncryptFiles.Log.IgnoringRow",
                  vPgpFile.getSourceFileFolder(),
                  vPgpFile.getDestinationFileFolder(),
                  vPgpFile.getWildcard()));
        }
      }
    }
    return null;
  }

  private @Nullable Result processPreviousResultRows(
      Result result, List<RowMetaAndData> rows, String moveToFolder) throws HopValueException {
    RowMetaAndData resultRow;
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionPGPEncryptFiles.Log.ArgFromPrevious.Found",
              (rows != null ? rows.size() : 0) + ""));
    }
    if (rows == null) {
      return null;
    }
    for (RowMetaAndData row : rows) {
      // Success condition broken?
      if (successConditionBroken) {
        if (!successConditionBrokenExit) {
          logError(
              BaseMessages.getString(
                  PKG, CONST_ACTION_PGPENCRYPT_FILES_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
          successConditionBrokenExit = true;
        }
        result.setNrErrors(nrErrors);
        displayResults();
        return result;
      }

      resultRow = row;

      // Get source and destination file names, also wildcard
      PgpFile previousPgpFile = new PgpFile();
      previousPgpFile.setActionType(ActionType.lookupWithCode(resultRow.getString(0, null)));
      previousPgpFile.setSourceFileFolder(resultRow.getString(1, null));
      previousPgpFile.setWildcard(resolve(resultRow.getString(2, null)));
      previousPgpFile.setUserId(resultRow.getString(3, null));
      previousPgpFile.setDestinationFileFolder(resultRow.getString(4, null));

      if (!Utils.isEmpty(previousPgpFile.getSourceFileFolder())
          && !Utils.isEmpty(previousPgpFile.getDestinationFileFolder())) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionPGPEncryptFiles.Log.ProcessingRow",
                  previousPgpFile.getSourceFileFolder(),
                  previousPgpFile.getDestinationFileFolder(),
                  previousPgpFile.getWildcard()));
        }

        if (!processFileFolder(
            previousPgpFile.getActionType(),
            previousPgpFile.getSourceFileFolder(),
            previousPgpFile.getUserId(),
            previousPgpFile.getDestinationFileFolder(),
            previousPgpFile.getWildcard(),
            parentWorkflow,
            result,
            moveToFolder)) {
          // The process fail
          // Update Errors
          updateErrors();
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionPGPEncryptFiles.Log.IgnoringRow",
                  previousPgpFile.getSourceFileFolder(),
                  previousPgpFile.getDestinationFileFolder(),
                  previousPgpFile.getWildcard()));
        }
      }
    }
    return null;
  }

  private @Nullable Result createMoveToFolder(Result result, String moveToFolder) {
    try (FileObject folder = HopVfs.getFileObject(moveToFolder, getVariables())) {
      if (!folder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionPGPEncryptFiles.Log.Error.FolderMissing", moveToFolder));
        }
        if (createMoveToFolder) {
          folder.createFolder();
        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionPGPEncryptFiles.Log.Error.FolderMissing", moveToFolder));
          return result;
        }
      }
      if (!folder.getType().equals(FileType.FOLDER)) {
        logError(
            BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Log.Error.NotFolder", moveToFolder));
        return result;
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPEncryptFiles.Log.Error.GettingMoveToFolder",
              moveToFolder,
              e.getMessage()));
      return result;
    }
    return null;
  }

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionPGPEncryptFiles.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionPGPEncryptFiles.Log.Info.FilesInSuccess", "" + nrSuccess));
      logDetailed("=======================================");
    }
  }

  private boolean getSuccessStatus() {
    return (nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrSuccess >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
  }

  private boolean processFileFolder(
      ActionType actionType,
      String sourceFileFolderName,
      String userId,
      String destinationFileFolderName,
      String wildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result,
      String moveToFolder) {
    boolean entrystatus = false;
    FileObject sourceFileFolder = null;
    FileObject destinationFileFolder = null;
    FileObject moveToFolderFolder = null;
    FileObject currentFile = null;

    // Get real source, destination file and wildcard
    String realSourceFileFolderName = resolve(sourceFileFolderName);
    String realUserId = resolve(userId);
    String realDestinationFileFolderName = resolve(destinationFileFolderName);
    String realWildcard = resolve(wildcard);

    try {
      sourceFileFolder = HopVfs.getFileObject(realSourceFileFolderName, getVariables());
      destinationFileFolder = HopVfs.getFileObject(realDestinationFileFolderName, getVariables());
      if (!Utils.isEmpty(moveToFolder)) {
        moveToFolderFolder = HopVfs.getFileObject(moveToFolder, getVariables());
      }

      if (sourceFileFolder.exists()) {

        // Check if destination folder/parent folder exists !
        // If user wanted and if destination folder does not exist
        // Apache Hop will create it
        if (createDestinationFolder(destinationFileFolder)) {
          // Basic Tests
          if (sourceFileFolder.getType().equals(FileType.FOLDER) && destinationIsAFile) {
            // Source is a folder, destination is a file
            // WARNING !!! CAN NOT MOVE FOLDER TO FILE !!!
            logError(
                BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Log.Forbidden"),
                BaseMessages.getString(
                    PKG,
                    "ActionPGPEncryptFiles.Log.CanNotMoveFolderToFile",
                    realSourceFileFolderName,
                    realDestinationFileFolderName));

            // Update Errors
            updateErrors();
          } else {
            if (destinationFileFolder.getType().equals(FileType.FOLDER)
                && sourceFileFolder.getType().equals(FileType.FILE)) {
              // Source is a file, destination is a folder
              // return destination short filename
              sourceFileFolder.getName().getBaseName();
              String shortFileName;

              try {
                shortFileName = getDestinationFilename(sourceFileFolder.getName().getBaseName());
              } catch (Exception e) {
                logError(
                    BaseMessages.getString(
                        PKG,
                        CONST_ACTION_PGPENCRYPT_FILES_ERROR_GETTING_FILENAME,
                        sourceFileFolder.getName().getBaseName(),
                        e.toString()));
                return entrystatus;
              }
              // Move the file to the destination folder

              String destinationFileNameFull =
                  destinationFileFolder.toString() + Const.FILE_SEPARATOR + shortFileName;
              FileObject destinationFile =
                  HopVfs.getFileObject(destinationFileNameFull, getVariables());

              entrystatus =
                  encryptFile(
                      actionType,
                      shortFileName,
                      sourceFileFolder,
                      realUserId,
                      destinationFile,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);

            } else if (sourceFileFolder.getType().equals(FileType.FILE) && destinationIsAFile) {

              // Source is a file, destination is a file

              FileObject destinationfile =
                  HopVfs.getFileObject(realDestinationFileFolderName, getVariables());

              // return destination short filename
              destinationfile.getName().getBaseName();
              String shortFileName;
              try {
                shortFileName = getDestinationFilename(destinationfile.getName().getBaseName());
              } catch (Exception e) {
                logError(
                    BaseMessages.getString(
                        PKG,
                        CONST_ACTION_PGPENCRYPT_FILES_ERROR_GETTING_FILENAME,
                        sourceFileFolder.getName().getBaseName(),
                        e.toString()));
                return entrystatus;
              }

              String destinationFileNameFull =
                  destinationFileFolder.getParent().toString()
                      + Const.FILE_SEPARATOR
                      + shortFileName;
              destinationfile = HopVfs.getFileObject(destinationFileNameFull, getVariables());

              entrystatus =
                  encryptFile(
                      actionType,
                      shortFileName,
                      sourceFileFolder,
                      realUserId,
                      destinationfile,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);

            } else {
              // Both source and destination are folders
              if (isDetailed()) {
                logDetailed("  ");
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionPGPEncryptFiles.Log.FetchFolder", sourceFileFolder.toString()));
              }

              FileObject[] fileObjects =
                  sourceFileFolder.findFiles(
                      new AllFileSelector() {
                        @Override
                        public boolean traverseDescendents(FileSelectInfo info) {
                          return info.getDepth() == 0 || includeSubFolders;
                        }

                        @Override
                        public boolean includeFile(FileSelectInfo info) {
                          try (FileObject fileObject = info.getFile()) {
                            if (fileObject == null) {
                              return false;
                            }
                          } catch (Exception ex) {
                            // Upon error don't process the file.
                            return false;
                          }
                          return true;
                        }
                      });

              if (fileObjects != null) {
                for (int j = 0; j < fileObjects.length && !parentWorkflow.isStopped(); j++) {
                  // Success condition broken?
                  if (successConditionBroken) {
                    if (!successConditionBrokenExit) {
                      logError(
                          BaseMessages.getString(
                              PKG,
                              CONST_ACTION_PGPENCRYPT_FILES_ERROR_SUCCESS_CONDITIONBROKEN,
                              "" + nrErrors));
                      successConditionBrokenExit = true;
                    }
                    return false;
                  }
                  // Fetch files in list one after one ...
                  currentFile = fileObjects[j];
                  if (!encryptOneFile(
                      actionType,
                      currentFile,
                      sourceFileFolder,
                      realUserId,
                      realDestinationFileFolderName,
                      realWildcard,
                      parentWorkflow,
                      result,
                      moveToFolderFolder)) {
                    // Update Errors
                    updateErrors();
                  }
                }
              }
            }
          }
          entrystatus = true;
        } else {
          // Destination Folder or Parent folder is missing
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionPGPEncryptFiles.Error.DestinationFolderNotFound",
                  realDestinationFileFolderName));
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ActionPGPEncryptFiles.Error.SourceFileNotExists", realSourceFileFolderName));
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPEncryptFiles.Error.Exception.MoveProcess",
              realSourceFileFolderName,
              destinationFileFolder == null ? "" : destinationFileFolder.toString(),
              e.getMessage()));
      // Update Errors
      updateErrors();
    } finally {
      if (sourceFileFolder != null) {
        try {
          sourceFileFolder.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (destinationFileFolder != null) {
        try {
          destinationFileFolder.close();
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
      if (moveToFolderFolder != null) {
        try {
          moveToFolderFolder.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return entrystatus;
  }

  private boolean encryptFile(
      ActionType actionType,
      String shortFileName,
      FileObject sourceFileName,
      String userId,
      FileObject destinationFileName,
      FileObject moveToFolderFolder,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {

    FileObject destinationFile = null;
    boolean success = false;
    try {
      if (!destinationFileName.exists()) {

        doJob(actionType, sourceFileName, userId, destinationFileName);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  CONST_ACTION_PGPENCRYPT_FILES_LOG_FILE_ENCRYPTED,
                  sourceFileName.getName().toString(),
                  destinationFileName.getName().toString()));
        }

        // add filename to result filename
        if (addResultFileNames
            && !ifFileExists.equals("fail")
            && !ifFileExists.equals(CONST_DO_NOTHING)) {
          addFileToResultFilenames(destinationFileName.toString(), result, parentWorkflow);
        }

        updateSuccess();

      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionPGPEncryptFiles.Log.FileExists", destinationFileName.toString()));
        }
        switch (ifFileExists) {
          case "overwrite_file" -> {
            doJob(actionType, sourceFileName, userId, destinationFileName);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionPGPEncryptFiles.Log.FileOverwrite",
                      destinationFileName.getName().toString()));
            }

            // add filename to result filename
            if (addResultFileNames
                && !ifFileExists.equals("fail")
                && !ifFileExists.equals(CONST_DO_NOTHING)) {
              addFileToResultFilenames(destinationFileName.toString(), result, parentWorkflow);
            }

            updateSuccess();
          }
          case "unique_name" -> {
            String shortFilename = shortFileName;

            // return destination short filename
            try {
              shortFilename = getMoveDestinationFilename(shortFilename, "ddMMyyyy_HHmmssSSS");
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG, CONST_ACTION_PGPENCRYPT_FILES_ERROR_GETTING_FILENAME, shortFilename),
                  e);
              return success;
            }

            String moveToFileNameFull =
                destinationFileName.getParent().toString() + Const.FILE_SEPARATOR + shortFilename;
            destinationFile = HopVfs.getFileObject(moveToFileNameFull, getVariables());

            doJob(actionType, sourceFileName, userId, destinationFileName);
            if (isDetailed()) {
              logDetailed(
                  toString(),
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_PGPENCRYPT_FILES_LOG_FILE_ENCRYPTED,
                      sourceFileName.getName().toString(),
                      destinationFile.getName().toString()));
            }

            // add filename to result filename
            if (addResultFileNames
                && !ifFileExists.equals("fail")
                && !ifFileExists.equals(CONST_DO_NOTHING)) {
              addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
            }

            updateSuccess();
          }
          case "delete_file" -> {
            destinationFileName.delete();
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionPGPEncryptFiles.Log.FileDeleted",
                      destinationFileName.getName().toString()));
            }
          }
          case "move_file" -> {
            String shortFilename = shortFileName;
            // return destination short filename
            try {
              shortFilename = getMoveDestinationFilename(shortFilename, null);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG, CONST_ACTION_PGPENCRYPT_FILES_ERROR_GETTING_FILENAME, shortFilename),
                  e);
              return success;
            }

            String moveToFileNameFull =
                moveToFolderFolder.toString() + Const.FILE_SEPARATOR + shortFilename;
            destinationFile = HopVfs.getFileObject(moveToFileNameFull, getVariables());
            if (!destinationFile.exists()) {
              sourceFileName.moveTo(destinationFile);
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        CONST_ACTION_PGPENCRYPT_FILES_LOG_FILE_ENCRYPTED,
                        sourceFileName.getName().toString(),
                        destinationFile.getName().toString()));
              }

              // add filename to result filename
              if (addResultFileNames
                  && !ifFileExists.equals("fail")
                  && !ifFileExists.equals(CONST_DO_NOTHING)) {
                addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
              }

            } else {
              switch (ifMovedFileExists) {
                case "overwrite_file" -> {
                  sourceFileName.moveTo(destinationFile);
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            "ActionPGPEncryptFiles.Log.FileOverwrite",
                            destinationFile.getName().toString()));
                  }

                  // add filename to result filename
                  if (addResultFileNames
                      && !ifFileExists.equals("fail")
                      && !ifFileExists.equals(CONST_DO_NOTHING)) {
                    addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
                  }

                  updateSuccess();
                }
                case "unique_name" -> {
                  SimpleDateFormat daf = new SimpleDateFormat();
                  Date now = new Date();
                  daf.applyPattern("ddMMyyyy_HHmmssSSS");
                  String dt = daf.format(now);
                  shortFilename += "_" + dt;

                  String destinationfilenamefull =
                      moveToFolderFolder.toString() + Const.FILE_SEPARATOR + shortFilename;
                  destinationFile = HopVfs.getFileObject(destinationfilenamefull, getVariables());

                  sourceFileName.moveTo(destinationFile);
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            CONST_ACTION_PGPENCRYPT_FILES_LOG_FILE_ENCRYPTED,
                            destinationFile.getName().toString()));
                  }

                  // add filename to result filename
                  if (addResultFileNames
                      && !ifFileExists.equals("fail")
                      && !ifFileExists.equals(CONST_DO_NOTHING)) {
                    addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
                  }

                  updateSuccess();
                }
                case "fail" ->
                    // Update Errors
                    updateErrors();
              }
            }
          }
          case "fail" ->
              // Update Errors
              updateErrors();
          default -> {}
        }
      }
    } catch (Exception e) {
      updateErrors();
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPEncryptFiles.Error.Exception.MoveProcessError",
              sourceFileName.toString(),
              destinationFileName.toString(),
              e.getMessage()));
    } finally {
      if (destinationFile != null) {
        try {
          destinationFile.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return success;
  }

  private boolean encryptOneFile(
      ActionType actionType,
      FileObject currentFile,
      FileObject sourceFileFolder,
      String userId,
      String realDestinationFileFolderName,
      String realWildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result,
      FileObject moveToFolderFolder) {
    boolean entryStatus = false;
    FileObject filename = null;

    try {
      if (!currentFile.toString().equals(sourceFileFolder.toString())) {
        // Pass over the Base folder itself

        // return destination short filename
        String sourceShortFileName = currentFile.getName().getBaseName();
        String shortFileName;
        try {
          shortFileName = getDestinationFilename(sourceShortFileName);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_ACTION_PGPENCRYPT_FILES_ERROR_GETTING_FILENAME,
                  currentFile.getName().getBaseName(),
                  e.toString()));
          return entryStatus;
        }

        int lenCurrent = sourceShortFileName.length();
        String shortFilenameFromBaseFolder = shortFileName;
        if (!isDoNotKeepFolderStructure()) {
          shortFilenameFromBaseFolder =
              currentFile.toString().substring(sourceFileFolder.toString().length());
        }
        shortFilenameFromBaseFolder =
            shortFilenameFromBaseFolder.substring(
                    0, shortFilenameFromBaseFolder.length() - lenCurrent)
                + shortFileName;

        // Built destination filename
        filename =
            HopVfs.getFileObject(
                realDestinationFileFolderName + Const.FILE_SEPARATOR + shortFilenameFromBaseFolder,
                getVariables());

        if (!currentFile.getParent().toString().equals(sourceFileFolder.toString())) {

          // Not in the Base Folder... Only if include sub folders
          if (includeSubFolders
              && currentFile.getType() != FileType.FOLDER
              && getFileWildcard(sourceShortFileName, realWildcard)) {
            // Folders... only if include subfolders
            entryStatus =
                encryptFile(
                    actionType,
                    shortFileName,
                    currentFile,
                    userId,
                    filename,
                    moveToFolderFolder,
                    parentWorkflow,
                    result);
          }
        } else {
          // In the Base Folder...
          // Folders... only if include subfolders
          if (currentFile.getType() != FileType.FOLDER
              && getFileWildcard(sourceShortFileName, realWildcard)) {
            // file...Check if exists
            entryStatus =
                encryptFile(
                    actionType,
                    shortFileName,
                    currentFile,
                    userId,
                    filename,
                    moveToFolderFolder,
                    parentWorkflow,
                    result);
          }
        }
      }
      entryStatus = true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Log.Error", e.toString()));
    } finally {
      if (filename != null) {
        try {
          filename.close();

        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return entryStatus;
  }

  private void updateErrors() {
    nrErrors++;
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ((nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }
    return retval;
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private void addFileToResultFilenames(
      String fileAddEntry, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    try {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              HopVfs.getFileObject(fileAddEntry, getVariables()),
              parentWorkflow.getWorkflowName(),
              toString());
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

      if (isDebug()) {
        logDebug(" ------ ");
        logDebug(
            BaseMessages.getString(
                PKG, "ActionPGPEncryptFiles.Log.FileAddedToResultFilesName", fileAddEntry));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionPGPEncryptFiles.Error.AddingToFilenameResult"),
          fileAddEntry + "" + e.getMessage());
    }
  }

  private boolean createDestinationFolder(FileObject fileFolder) {
    FileObject folder = null;
    try {
      if (destinationIsAFile) {
        folder = fileFolder.getParent();
      } else {
        folder = fileFolder;
      }

      if (!folder.exists()) {
        if (createDestinationFolder) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionPGPEncryptFiles.Log.FolderNotExist", folder.getName().toString()));
          }
          folder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionPGPEncryptFiles.Log.FolderWasCreated",
                    folder.getName().toString()));
          }
        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionPGPEncryptFiles.Log.FolderNotExist", folder.getName().toString()));
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPEncryptFiles.Log.CanNotCreateParentFolder",
              folder == null ? "" : folder.getName().toString()),
          e);

    } finally {
      if (folder != null) {
        try {
          folder.close();
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
    return false;
  }

  /**********************************************************
   *
   * @param selectedFile The selected file
   * @param wildcard The wildcard
   * @return True if the selectedFile matches the wildcard
   **********************************************************/
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

  private String getDestinationFilename(String shortSourceFileName) {
    String shortFileName = shortSourceFileName;
    int stringLength = shortSourceFileName.length();
    int lastindexOfDot = shortFileName.lastIndexOf('.');
    if (lastindexOfDot == -1) {
      lastindexOfDot = stringLength;
    }

    if (isAddDateBeforeExtension()) {
      shortFileName = shortFileName.substring(0, lastindexOfDot);
    }

    if (daf == null) {
      daf = new SimpleDateFormat();
    }
    Date now = new Date();

    if (isSpecifyFormat() && !Utils.isEmpty(getDateTimeFormat())) {
      daf.applyPattern(getDateTimeFormat());
      String dt = daf.format(now);
      shortFileName += dt;
    } else {
      if (isAddDate()) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        shortFileName += "_" + d;
      }
      if (isAddTime()) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        shortFileName += "_" + t;
      }
    }
    if (isAddDateBeforeExtension()) {
      shortFileName += shortSourceFileName.substring(lastindexOfDot, stringLength);
    }

    return shortFileName;
  }

  private String getMoveDestinationFilename(String shortSourceFileName, String dateFormat)
      throws Exception {
    String shortfilename = shortSourceFileName;
    int stringLength = shortSourceFileName.length();
    int lastindexOfDot = shortfilename.lastIndexOf('.');
    if (lastindexOfDot == -1) {
      lastindexOfDot = stringLength;
    }

    if (isAddMovedDateBeforeExtension()) {
      shortfilename = shortfilename.substring(0, lastindexOfDot);
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
    Date now = new Date();

    if (dateFormat != null) {
      simpleDateFormat.applyPattern(dateFormat);
      String dt = simpleDateFormat.format(now);
      shortfilename += dt;
    } else {

      if (isSpecifyMoveFormat() && !Utils.isEmpty(getMovedDateTimeFormat())) {
        simpleDateFormat.applyPattern(getMovedDateTimeFormat());
        String dt = simpleDateFormat.format(now);
        shortfilename += dt;
      } else {
        if (isAddMovedDate()) {
          simpleDateFormat.applyPattern("yyyyMMdd");
          String d = simpleDateFormat.format(now);
          shortfilename += "_" + d;
        }
        if (isAddMovedTime()) {
          simpleDateFormat.applyPattern("HHmmssSSS");
          String t = simpleDateFormat.format(now);
          shortfilename += "_" + t;
        }
      }
    }
    if (isAddMovedDateBeforeExtension()) {
      shortfilename += shortSourceFileName.substring(lastindexOfDot, stringLength);
    }

    return shortfilename;
  }

  public void doJob(
      ActionType actionType, FileObject sourceFile, String userID, FileObject destinationFile)
      throws HopException {

    switch (actionType) {
      case SIGN:
        gpg.signFile(sourceFile, userID, destinationFile, isAsciiMode());
        break;
      case SIGN_AND_ENCRYPT:
        gpg.signAndEncryptFile(sourceFile, userID, destinationFile, isAsciiMode());
        break;
      default:
        gpg.encryptFile(sourceFile, userID, destinationFile, isAsciiMode());
        break;
    }
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

    for (int i = 0; i < pgpFiles.size(); i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  @Getter
  public enum ActionType implements IEnumHasCodeAndDescription {
    ENCRYPT(
        "encrypt", BaseMessages.getString(PKG, "ActionPGPEncryptFiles.ActionsType.Encrypt.Label")),
    SIGN("sign", BaseMessages.getString(PKG, "ActionPGPEncryptFiles.ActionsType.Sign.Label")),
    SIGN_AND_ENCRYPT(
        "signandencrypt",
        BaseMessages.getString(PKG, "ActionPGPEncryptFiles.ActionsType.SignAndEncrypt.Label"));
    final String code;
    final String description;

    ActionType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static ActionType lookupWithCode(String code) {
      return IEnumHasCode.lookupCode(ActionType.class, code, ENCRYPT);
    }

    public static ActionType lookupWithDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(ActionType.class, description, ENCRYPT);
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(ActionType.class);
    }
  }

  @Getter
  @Setter
  public static class PgpFile {
    @HopMetadataProperty(key = "action_type", storeWithCode = true)
    private ActionType actionType;

    @HopMetadataProperty(key = "source_filefolder")
    public String sourceFileFolder;

    @HopMetadataProperty(key = "userid")
    public String userId;

    @HopMetadataProperty(key = "destination_filefolder")
    public String destinationFileFolder;

    @HopMetadataProperty(key = "wildcard")
    public String wildcard;

    public PgpFile() {}

    public PgpFile(PgpFile f) {
      this();
      this.actionType = f.actionType;
      this.sourceFileFolder = f.sourceFileFolder;
      this.userId = f.userId;
      this.destinationFileFolder = f.destinationFileFolder;
      this.wildcard = f.wildcard;
    }
  }
}

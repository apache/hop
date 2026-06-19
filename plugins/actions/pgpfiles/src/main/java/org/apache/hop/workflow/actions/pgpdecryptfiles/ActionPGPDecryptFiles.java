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

package org.apache.hop.workflow.actions.pgpdecryptfiles;

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
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.actions.pgpencryptfiles.GPG;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'PGP decrypt files' action. */
@Action(
    id = "PGP_DECRYPT_FILES",
    name = "i18n::ActionPGPDecryptFiles.Name",
    description = "i18n::ActionPGPDecryptFiles.Description",
    image = "PGPDecryptFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileEncryption",
    keywords = "i18n::ActionPGPDecryptFiles.keyword",
    documentationUrl = "/workflow/actions/pgpdecryptfiles.html")
@Getter
@Setter
@SuppressWarnings("java:S1104")
public class ActionPGPDecryptFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionPGPDecryptFiles.class;

  public static final String CONST_ACTION_PGP_DECRYPT_FILES_ERROR_SUCCESS_CONDITION_BROKEN =
      "ActionPGPDecryptFiles.Error.SuccessConditionbroken";
  public static final String CONST_ACTION_PGP_DECRYPT_FILES_ERROR_GETTING_FILENAME =
      "ActionPGPDecryptFiles.Error.GettingFilename";
  public static final String CONST_ACTION_PGP_DECRYPT_FILES_LOG_FILE_DECRYPTED =
      "ActionPGPDecryptFiles.Log.FileDecrypted";

  public static final String CONST_DO_NOTHING = "do_nothing";
  public static final String CONST_MOVE_FILE = "move_file";
  public static final String CONST_OVERWRITE_FILE = "overwrite_file";
  public static final String CONST_UNIQUE_NAME = "unique_name";
  public static final String CONST_DELETE_FILE = "delete_file";
  public static final String CONST_FAIL = "fail";

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  @HopMetadataProperty(key = "arg_from_previous")
  public boolean argFromPrevious;

  @HopMetadataProperty(key = "include_subfolders")
  public boolean includeSubFolders;

  @HopMetadataProperty(key = "add_result_filesname")
  public boolean addResultFilenames;

  @HopMetadataProperty(key = "destination_is_a_file")
  public boolean destinationIsAFile;

  @HopMetadataProperty(key = "create_destination_folder")
  public boolean createDestinationFolder;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  List<FileToDecrypt> filesToDecrypt;

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
  public boolean createMoveToFolder;

  @HopMetadataProperty(key = "gpglocation")
  private String gpgLocation;

  private SimpleDateFormat daf;
  private GPG gpg;
  private int nrErrors = 0;
  private int nrSuccess = 0;
  private boolean successConditionBroken = false;
  private boolean successConditionBrokenExit = false;
  private int limitFiles = 0;

  public ActionPGPDecryptFiles(String n) {
    super(n, "");
    filesToDecrypt = new ArrayList<>();
    ifMovedFileExists = CONST_DO_NOTHING;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
    ifFileExists = CONST_DO_NOTHING;
  }

  public ActionPGPDecryptFiles() {
    this("");
  }

  public ActionPGPDecryptFiles(ActionPGPDecryptFiles a) {
    super(a);
    this.argFromPrevious = a.argFromPrevious;
    this.includeSubFolders = a.includeSubFolders;
    this.addResultFilenames = a.addResultFilenames;
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
    this.filesToDecrypt = new ArrayList<>();
    a.filesToDecrypt.forEach(f -> filesToDecrypt.add(new FileToDecrypt(f)));

    this.daf = null;
    this.gpg = null;
    this.nrErrors = 0;
    this.nrSuccess = 0;
    this.successConditionBroken = false;
    this.successConditionBrokenExit = false;
    this.limitFiles = 0;
  }

  @Override
  public Object clone() {
    return new ActionPGPDecryptFiles(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow;
    result.setNrErrors(1);
    result.setResult(false);

    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;
    limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);

    if (includeSubFolders && isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionPGPDecryptFiles.Log.IncludeSubFoldersOn"));
    }

    String moveToFolder = resolve(destinationFolder);

    if (CONST_MOVE_FILE.equals(ifFileExists)) {
      if (Utils.isEmpty(moveToFolder)) {
        logError(
            BaseMessages.getString(PKG, "ActionPGPDecryptFiles.Log.Error.MoveToFolderMissing"));
        return result;
      }
      try (FileObject folder = HopVfs.getFileObject(moveToFolder, getVariables())) {
        if (!folder.exists()) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionPGPDecryptFiles.Log.Error.FolderMissing", moveToFolder));
          }
          if (createMoveToFolder) {
            folder.createFolder();
          } else {
            logError(
                BaseMessages.getString(
                    PKG, "ActionPGPDecryptFiles.Log.Error.FolderMissing", moveToFolder));
            return result;
          }
        }
        if (!folder.getType().equals(FileType.FOLDER)) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionPGPDecryptFiles.Log.Error.NotFolder", moveToFolder));
          return result;
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionPGPDecryptFiles.Log.Error.GettingMoveToFolder",
                moveToFolder,
                e.getMessage()));
        return result;
      }
    }

    gpg = new GPG(resolve(gpgLocation), getLogChannel(), getVariables());

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionPGPDecryptFiles.Log.ArgFromPrevious.Found",
              (rows != null ? rows.size() : 0) + ""));
    }
    if (argFromPrevious && rows != null) {
      for (RowMetaAndData row : rows) {
        // Success condition broken?
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG,
                    CONST_ACTION_PGP_DECRYPT_FILES_ERROR_SUCCESS_CONDITION_BROKEN,
                    "" + nrErrors));
            successConditionBrokenExit = true;
          }
          result.setNrErrors(nrErrors);
          displayResults();
          return result;
        }

        resultRow = row;

        // Get source and destination file names, also wildcard
        String vSourceFileFolderPrevious = resultRow.getString(0, null);
        String vWildcardPrevious = resolve(resultRow.getString(1, null));
        String vPassPhrasePrevious =
            Encr.decryptPasswordOptionallyEncrypted(resultRow.getString(2, null));
        String vDestinationFileFolderPrevious = resultRow.getString(3, null);

        if (!Utils.isEmpty(vSourceFileFolderPrevious)
            && !Utils.isEmpty(vDestinationFileFolderPrevious)) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionPGPDecryptFiles.Log.ProcessingRow",
                    vSourceFileFolderPrevious,
                    vDestinationFileFolderPrevious,
                    vWildcardPrevious));
          }

          if (!processFileFolder(
              vSourceFileFolderPrevious,
              vPassPhrasePrevious,
              vDestinationFileFolderPrevious,
              vWildcardPrevious,
              parentWorkflow,
              result,
              moveToFolder)) {
            // The move process fail
            // Update Errors
            updateErrors();
          }
        } else {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionPGPDecryptFiles.Log.IgnoringRow",
                    vSourceFileFolderPrevious,
                    vDestinationFileFolderPrevious,
                    vWildcardPrevious));
          }
        }
      }
    } else if (!filesToDecrypt.isEmpty()) {
      for (FileToDecrypt fileToDecrypt : filesToDecrypt) {
        if (parentWorkflow.isStopped()) {
          break;
        }
        // Success condition broken?
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG,
                    CONST_ACTION_PGP_DECRYPT_FILES_ERROR_SUCCESS_CONDITION_BROKEN,
                    "" + nrErrors));
            successConditionBrokenExit = true;
          }
          result.setNrErrors(nrErrors);
          displayResults();
          return result;
        }

        if (!Utils.isEmpty(fileToDecrypt.getSourceFileFolder())
            && !Utils.isEmpty(fileToDecrypt.getDestinationFileFolder())) {
          // ok we can process this file/folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionPGPDecryptFiles.Log.ProcessingRow",
                    fileToDecrypt.getSourceFileFolder(),
                    fileToDecrypt.getDestinationFileFolder(),
                    fileToDecrypt.getWildcard()));
          }

          if (!processFileFolder(
              fileToDecrypt.getSourceFileFolder(),
              Encr.decryptPasswordOptionallyEncrypted(resolve(fileToDecrypt.getPassphrase())),
              fileToDecrypt.getDestinationFileFolder(),
              fileToDecrypt.getWildcard(),
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
                    "ActionPGPDecryptFiles.Log.IgnoringRow",
                    fileToDecrypt.getSourceFileFolder(),
                    fileToDecrypt.getDestinationFileFolder(),
                    fileToDecrypt.getWildcard()));
          }
        }
      }
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

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionPGPDecryptFiles.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionPGPDecryptFiles.Log.Info.FilesInSuccess", "" + nrSuccess));
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
      String sourceFileFolderName,
      String passPhrase,
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
                BaseMessages.getString(PKG, "ActionPGPDecryptFiles.Log.Forbidden"),
                BaseMessages.getString(
                    PKG,
                    "ActionPGPDecryptFiles.Log.CanNotMoveFolderToFile",
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
                        CONST_ACTION_PGP_DECRYPT_FILES_ERROR_GETTING_FILENAME,
                        sourceFileFolder.getName().getBaseName(),
                        e.toString()));
                return entrystatus;
              }
              // Move the file to the destination folder

              String destinationFileNameFull =
                  destinationFileFolder + Const.FILE_SEPARATOR + shortFileName;
              FileObject destinationFile =
                  HopVfs.getFileObject(destinationFileNameFull, getVariables());

              entrystatus =
                  decryptFile(
                      shortFileName,
                      sourceFileFolder,
                      passPhrase,
                      destinationFile,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);

            } else if (sourceFileFolder.getType().equals(FileType.FILE) && destinationIsAFile) {
              // Source is a file, destination is a file

              FileObject destinationFile =
                  HopVfs.getFileObject(realDestinationFileFolderName, getVariables());

              // return destination short filename
              destinationFile.getName().getBaseName();
              String shortFileName;
              try {
                shortFileName = getDestinationFilename(destinationFile.getName().getBaseName());
              } catch (Exception e) {
                logError(
                    BaseMessages.getString(
                        PKG,
                        CONST_ACTION_PGP_DECRYPT_FILES_ERROR_GETTING_FILENAME,
                        sourceFileFolder.getName().getBaseName(),
                        e.toString()));
                return entrystatus;
              }

              String destinationFileNameFull =
                  destinationFileFolder.getParent().toString()
                      + Const.FILE_SEPARATOR
                      + shortFileName;
              destinationFile = HopVfs.getFileObject(destinationFileNameFull, getVariables());

              entrystatus =
                  decryptFile(
                      shortFileName,
                      sourceFileFolder,
                      passPhrase,
                      destinationFile,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);
            } else {
              // Both source and destination are folders
              if (isDetailed()) {
                logDetailed("  ");
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionPGPDecryptFiles.Log.FetchFolder", sourceFileFolder.toString()));
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
                              CONST_ACTION_PGP_DECRYPT_FILES_ERROR_SUCCESS_CONDITION_BROKEN,
                              "" + nrErrors));
                      successConditionBrokenExit = true;
                    }
                    return false;
                  }
                  // Fetch files in list one after one ...
                  currentFile = fileObjects[j];

                  if (!decryptOneFile(
                      currentFile,
                      sourceFileFolder,
                      passPhrase,
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
                  "ActionPGPDecryptFiles.Error.DestinationFolderNotFound",
                  realDestinationFileFolderName));
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ActionPGPDecryptFiles.Error.SourceFileNotExists", realSourceFileFolderName));
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPDecryptFiles.Error.Exception.MoveProcess",
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

  private boolean decryptFile(
      String shortFileName,
      FileObject sourceFileName,
      String passPhrase,
      FileObject destinationFileName,
      FileObject moveToFolderFolder,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {

    FileObject destinationFile = null;
    boolean returnValue = false;
    try {
      if (!destinationFileName.exists()) {
        gpg.decryptFile(sourceFileName, passPhrase, destinationFileName);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  CONST_ACTION_PGP_DECRYPT_FILES_LOG_FILE_DECRYPTED,
                  sourceFileName.getName().toString(),
                  destinationFileName.getName().toString()));
        }

        // add filename to result filename
        if (addResultFilenames
            && !ifFileExists.equals("fail")
            && !ifFileExists.equals(CONST_DO_NOTHING)) {
          addFileToResultFilenames(destinationFileName.toString(), result, parentWorkflow);
        }

        updateSuccess();

      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionPGPDecryptFiles.Log.FileExists", destinationFileName.toString()));
        }
        switch (ifFileExists) {
          case CONST_OVERWRITE_FILE -> {
            gpg.decryptFile(sourceFileName, passPhrase, destinationFileName);

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionPGPDecryptFiles.Log.FileOverwrite",
                      destinationFileName.getName().toString()));
            }

            // add filename to result filename
            if (addResultFilenames
                && !ifFileExists.equals("fail")
                && !ifFileExists.equals(CONST_DO_NOTHING)) {
              addFileToResultFilenames(destinationFileName.toString(), result, parentWorkflow);
            }

            updateSuccess();
          }
          case CONST_UNIQUE_NAME -> {
            String shortFilename = shortFileName;

            // return destination short filename
            try {
              shortFilename = getMoveDestinationFilename(shortFilename, "ddMMyyyy_HHmmssSSS");
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG, CONST_ACTION_PGP_DECRYPT_FILES_ERROR_GETTING_FILENAME, shortFilename),
                  e);
              return returnValue;
            }

            String movetofilenamefull =
                destinationFileName.getParent().toString() + Const.FILE_SEPARATOR + shortFilename;
            destinationFile = HopVfs.getFileObject(movetofilenamefull, getVariables());

            gpg.decryptFile(sourceFileName, passPhrase, destinationFile);

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_PGP_DECRYPT_FILES_LOG_FILE_DECRYPTED,
                      sourceFileName.getName().toString(),
                      destinationFile.getName().toString()));
            }

            // add filename to result filename
            if (addResultFilenames
                && !ifFileExists.equals("fail")
                && !ifFileExists.equals(CONST_DO_NOTHING)) {
              addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
            }

            updateSuccess();
          }
          case CONST_DELETE_FILE -> {
            destinationFileName.delete();
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionPGPDecryptFiles.Log.FileDeleted",
                      destinationFileName.getName().toString()));
            }
          }
          case CONST_MOVE_FILE -> {
            String shortFilename = shortFileName;
            // return destination short filename
            try {
              shortFilename = getMoveDestinationFilename(shortFilename, null);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG, CONST_ACTION_PGP_DECRYPT_FILES_ERROR_GETTING_FILENAME, shortFilename),
                  e);
              return returnValue;
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
                        CONST_ACTION_PGP_DECRYPT_FILES_LOG_FILE_DECRYPTED,
                        sourceFileName.getName().toString(),
                        destinationFile.getName().toString()));
              }

              // add filename to result filename
              if (addResultFilenames
                  && !ifFileExists.equals("fail")
                  && !ifFileExists.equals(CONST_DO_NOTHING)) {
                addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
              }

            } else {
              switch (ifMovedFileExists) {
                case CONST_OVERWRITE_FILE -> {
                  sourceFileName.moveTo(destinationFile);
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            "ActionPGPDecryptFiles.Log.FileOverwrite",
                            destinationFile.getName().toString()));
                  }

                  // add filename to result filename
                  if (addResultFilenames
                      && !ifFileExists.equals("fail")
                      && !ifFileExists.equals(CONST_DO_NOTHING)) {
                    addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
                  }

                  updateSuccess();
                }
                case CONST_UNIQUE_NAME -> {
                  SimpleDateFormat daf = new SimpleDateFormat();
                  Date now = new Date();
                  daf.applyPattern("ddMMyyyy_HHmmssSSS");
                  String dt = daf.format(now);
                  shortFilename += "_" + dt;

                  String destinationFileNameFull =
                      moveToFolderFolder + Const.FILE_SEPARATOR + shortFilename;
                  destinationFile = HopVfs.getFileObject(destinationFileNameFull, getVariables());

                  sourceFileName.moveTo(destinationFile);
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            CONST_ACTION_PGP_DECRYPT_FILES_LOG_FILE_DECRYPTED,
                            destinationFile.getName().toString()));
                  }

                  // add filename to result filename
                  if (addResultFilenames
                      && !ifFileExists.equals(CONST_FAIL)
                      && !ifFileExists.equals(CONST_DO_NOTHING)) {
                    addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
                  }

                  updateSuccess();
                }
                case CONST_FAIL ->
                    // Update Errors
                    updateErrors();
                default -> {}
              }
            }
          }
          case CONST_FAIL ->
              // Update Errors
              updateErrors();
          default -> {
            // Nothing to do.
          }
        }
      }
    } catch (Exception e) {
      updateErrors();
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPDecryptFiles.Error.Exception.MoveProcessError",
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
    return returnValue;
  }

  private boolean decryptOneFile(
      FileObject currentFile,
      FileObject sourceFileFolder,
      String passPhrase,
      String realDestinationFileFolderName,
      String realWildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result,
      FileObject moveToFolderFolder) {
    boolean entryStatus = false;
    FileObject fileObject = null;

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
                  CONST_ACTION_PGP_DECRYPT_FILES_ERROR_GETTING_FILENAME,
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
        fileObject =
            HopVfs.getFileObject(
                realDestinationFileFolderName + Const.FILE_SEPARATOR + shortFilenameFromBaseFolder,
                getVariables());

        if (!currentFile.getParent().toString().equals(sourceFileFolder.toString())) {

          // Not in the Base Folder... Only if include sub folders
          if (includeSubFolders
              && currentFile.getType() != FileType.FOLDER
              && getFileWildcard(sourceShortFileName, realWildcard)) {
            // Folders..only if include subfolders
            entryStatus =
                decryptFile(
                    shortFileName,
                    currentFile,
                    passPhrase,
                    fileObject,
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
                decryptFile(
                    shortFileName,
                    currentFile,
                    passPhrase,
                    fileObject,
                    moveToFolderFolder,
                    parentWorkflow,
                    result);
          }
        }
      }
      entryStatus = true;

    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionPGPDecryptFiles.Log.Error", e.toString()));
    } finally {
      if (fileObject != null) {
        try {
          fileObject.close();

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
    return (nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
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
                PKG, "ActionPGPDecryptFiles.Log.FileAddedToResultFilesName", fileAddEntry));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionPGPDecryptFiles.Error.AddingToFilenameResult"),
          fileAddEntry + e.getMessage(),
          e);
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
                    PKG, "ActionPGPDecryptFiles.Log.FolderNotExist", folder.getName().toString()));
          }
          folder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionPGPDecryptFiles.Log.FolderWasCreated",
                    folder.getName().toString()));
          }
        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionPGPDecryptFiles.Log.FolderNotExist", folder.getName().toString()));
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionPGPDecryptFiles.Log.CanNotCreateParentFolder",
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
   * @return True if the selected file matches the wildcard
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
    int lastIndexOfDot = shortFileName.lastIndexOf('.');
    if (lastIndexOfDot == -1) {
      lastIndexOfDot = stringLength;
    }

    if (isAddDateBeforeExtension()) {
      shortFileName = shortFileName.substring(0, lastIndexOfDot);
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
      shortFileName += shortSourceFileName.substring(lastIndexOfDot, stringLength);
    }

    return shortFileName;
  }

  private String getMoveDestinationFilename(String shortSourceFileName, String dateFormat) {
    String shortFileName = shortSourceFileName;
    int stringLength = shortSourceFileName.length();
    int lastIndexOfDot = shortFileName.lastIndexOf('.');
    if (lastIndexOfDot == -1) {
      lastIndexOfDot = stringLength;
    }

    if (isAddMovedDateBeforeExtension()) {
      shortFileName = shortFileName.substring(0, lastIndexOfDot);
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
    Date now = new Date();

    if (dateFormat != null) {
      simpleDateFormat.applyPattern(dateFormat);
      String dt = simpleDateFormat.format(now);
      shortFileName += dt;
    } else {

      if (isSpecifyMoveFormat() && !Utils.isEmpty(getMovedDateTimeFormat())) {
        simpleDateFormat.applyPattern(getMovedDateTimeFormat());
        String dt = simpleDateFormat.format(now);
        shortFileName += dt;
      } else {
        if (isAddMovedDate()) {
          simpleDateFormat.applyPattern("yyyyMMdd");
          String d = simpleDateFormat.format(now);
          shortFileName += "_" + d;
        }
        if (isAddMovedTime()) {
          simpleDateFormat.applyPattern("HHmmssSSS");
          String t = simpleDateFormat.format(now);
          shortFileName += "_" + t;
        }
      }
    }
    if (isAddMovedDateBeforeExtension()) {
      shortFileName += shortSourceFileName.substring(lastIndexOfDot, stringLength);
    }

    return shortFileName;
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
    boolean validationSuccess =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "arguments",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!validationSuccess) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < filesToDecrypt.size(); i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  @Getter
  @Setter
  public static class FileToDecrypt {
    @HopMetadataProperty(key = "source_filefolder")
    public String sourceFileFolder;

    @HopMetadataProperty(key = "passphrase", password = true)
    public String passphrase;

    @HopMetadataProperty(key = "destination_filefolder")
    public String destinationFileFolder;

    @HopMetadataProperty(key = "wildcard")
    public String wildcard;

    public FileToDecrypt() {}

    public FileToDecrypt(FileToDecrypt f) {
      this();
      this.sourceFileFolder = f.sourceFileFolder;
      this.passphrase = f.passphrase;
      this.destinationFileFolder = f.destinationFileFolder;
      this.wildcard = f.wildcard;
    }
  }
}

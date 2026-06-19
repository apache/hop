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

package org.apache.hop.workflow.actions.movefiles;

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
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
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
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'move files' action. */
@Action(
    id = "MOVE_FILES",
    name = "i18n::ActionMoveFiles.Name",
    description = "i18n::ActionMoveFiles.Description",
    image = "MoveFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionMoveFiles.keyword",
    documentationUrl = "/workflow/actions/movefiles.html")
@Getter
@Setter
@SuppressWarnings("java:S1104")
public class ActionMoveFiles extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMoveFiles.class;
  public static final String CONST_SPACES_LONG = "          ";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_DO_NOTHING = "do_nothing";
  public static final String CONST_ACTION_MOVE_FILES_ERROR_GETTING_FILENAME =
      "ActionMoveFiles.Error.GettingFilename";
  public static final String CONST_ACTION_MOVE_FILES_LOG_FILE_MOVED =
      "ActionMoveFiles.Log.FileMoved";
  public static final String CONST_ACTION_MOVE_FILES_ERROR_SUCCESS_CONDITIONBROKEN =
      "ActionMoveFiles.Error.SuccessConditionbroken";

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";
  public static final String MOVE_FILE = "move_file";
  public static final String DELETE_FILE = "delete_file";
  public static final String FAIL = "fail";

  @HopMetadataProperty(key = "move_empty_folders")
  private boolean moveEmptyFolders;

  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubfolders;

  @HopMetadataProperty(key = "add_result_filesname")
  private boolean addResultFilenames;

  @HopMetadataProperty(key = "destination_is_a_file")
  private boolean destinationIsAFile;

  @HopMetadataProperty(key = "create_destination_folder")
  private boolean createDestinationFolder;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<FileToMove> filesToMove;

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

  @HopMetadataProperty(key = "simulate")
  private boolean simulate;

  int nrErrors = 0;
  int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  public ActionMoveFiles(String n) {
    super(n, "");
    filesToMove = new ArrayList<>();
    ifMovedFileExists = CONST_DO_NOTHING;
    moveEmptyFolders = true;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
  }

  public ActionMoveFiles() {
    this("");
  }

  public ActionMoveFiles(ActionMoveFiles a) {
    super(a);
    this.moveEmptyFolders = a.moveEmptyFolders;
    this.argFromPrevious = a.argFromPrevious;
    this.includeSubfolders = a.includeSubfolders;
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
    this.simulate = a.simulate;
    this.filesToMove = new ArrayList<>();
    a.filesToMove.forEach(f -> filesToMove.add(new FileToMove(f)));

    this.nrErrors = 0;
    this.nrSuccess = 0;
    this.successConditionBroken = false;
    this.successConditionBrokenExit = false;
    this.limitFiles = 0;
  }

  @Override
  public Object clone() {
    return new ActionMoveFiles(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;
    result.setNrErrors(1);
    result.setResult(false);

    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;
    limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);

    if (isDetailed()) {
      if (simulate) {
        logDetailed(BaseMessages.getString(PKG, "ActionMoveFiles.Log.SimulationOn"));
      }
      if (includeSubfolders) {
        logDetailed(BaseMessages.getString(PKG, "ActionMoveFiles.Log.IncludeSubFoldersOn"));
      }
    }

    String moveToFolder = resolve(destinationFolder);
    // Get source and destination files, also wildcard
    List<FileToMove> vFilesToMove = new ArrayList<>();
    for (FileToMove fileToMove : filesToMove) {
      vFilesToMove.add(new FileToMove(fileToMove));
    }

    if (MOVE_FILE.equals(ifFileExists)) {
      if (Utils.isEmpty(moveToFolder)) {
        logError(BaseMessages.getString(PKG, "ActionMoveFiles.Log.Error.MoveToFolderMissing"));
        return result;
      }
      try (FileObject folder = HopVfs.getFileObject(moveToFolder, getVariables())) {
        if (!folder.exists()) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionMoveFiles.Log.Error.FolderMissing", moveToFolder));
          }
          if (createMoveToFolder) {
            folder.createFolder();
          } else {
            logError(
                BaseMessages.getString(
                    PKG, "ActionMoveFiles.Log.Error.FolderMissing", moveToFolder));
            return result;
          }
        }
        if (!folder.getType().equals(FileType.FOLDER)) {
          logError(
              BaseMessages.getString(PKG, "ActionMoveFiles.Log.Error.NotFolder", moveToFolder));
          return result;
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionMoveFiles.Log.Error.GettingMoveToFolder",
                moveToFolder,
                e.getMessage()));
        return result;
      }
    }

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionMoveFiles.Log.ArgFromPrevious.Found",
              (rows != null ? rows.size() : 0) + ""));
    }
    if (argFromPrevious && rows != null) {
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        // Success condition broken?
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG, CONST_ACTION_MOVE_FILES_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
            successConditionBrokenExit = true;
          }
          result.setNrErrors(nrErrors);
          displayResults();
          return result;
        }

        resultRow = rows.get(iteration);

        // Get source and destination file names, also wildcard
        String vSourceFileFolderPrevious = resultRow.getString(0, null);
        String vDestinationFileFolderPrevious = resultRow.getString(1, null);
        String vWildcardPrevious = resultRow.getString(2, null);

        if (!Utils.isEmpty(vSourceFileFolderPrevious)
            && !Utils.isEmpty(vDestinationFileFolderPrevious)) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionMoveFiles.Log.ProcessingRow",
                    vSourceFileFolderPrevious,
                    vDestinationFileFolderPrevious,
                    vWildcardPrevious));
          }

          if (!processFileFolder(
              vSourceFileFolderPrevious,
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
                    "ActionMoveFiles.Log.IgnoringRow",
                    vFilesToMove.get(iteration).getSourceFileFolder(),
                    vFilesToMove.get(iteration).getDestinationFileFolder(),
                    vFilesToMove.get(iteration).getWildcard()));
          }
        }
      }
    } else if (!vFilesToMove.isEmpty()) {
      for (FileToMove vFileToMove : vFilesToMove) {
        if (parentWorkflow.isStopped()) {
          break;
        }
        // Success condition broken?
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG, CONST_ACTION_MOVE_FILES_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
            successConditionBrokenExit = true;
          }
          result.setNrErrors(nrErrors);
          displayResults();
          return result;
        }

        if (!Utils.isEmpty(vFileToMove.getSourceFileFolder())
            && !Utils.isEmpty(vFileToMove.getDestinationFileFolder())) {
          // ok we can process this file/folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionMoveFiles.Log.ProcessingRow",
                    vFileToMove.getSourceFileFolder(),
                    vFileToMove.getDestinationFileFolder(),
                    vFileToMove.getWildcard()));
          }

          if (!processFileFolder(
              vFileToMove.getSourceFileFolder(),
              vFileToMove.getDestinationFileFolder(),
              vFileToMove.getWildcard(),
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
                    "ActionMoveFiles.Log.IgnoringRow",
                    vFileToMove.getSourceFileFolder(),
                    vFileToMove.getDestinationFileFolder(),
                    vFileToMove.getWildcard()));
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
          BaseMessages.getString(PKG, "ActionMoveFiles.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(PKG, "ActionMoveFiles.Log.Info.FilesInSuccess", "" + nrSuccess));
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
      String destinationFileFolderName,
      String wildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result,
      String moveToFolder) {
    boolean entryStatus = false;
    FileObject sourceFileFolder = null;
    FileObject destinationFileFolder = null;
    FileObject moveToFolderFolder = null;
    FileObject currentFile = null;

    // Get real source, destination file and wildcard
    String realSourceFilefoldername = resolve(sourceFileFolderName);
    String realDestinationFilefoldername = resolve(destinationFileFolderName);
    String realWildcard = resolve(wildcard);

    try {
      sourceFileFolder = HopVfs.getFileObject(realSourceFilefoldername, getVariables());
      destinationFileFolder = HopVfs.getFileObject(realDestinationFilefoldername, getVariables());
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
                BaseMessages.getString(PKG, "ActionMoveFiles.Log.Forbidden"),
                BaseMessages.getString(
                    PKG,
                    "ActionMoveFiles.Log.CanNotMoveFolderToFile",
                    realSourceFilefoldername,
                    realDestinationFilefoldername));

            // Update Errors
            updateErrors();
          } else {
            if (destinationFileFolder.getType().equals(FileType.FOLDER)
                && sourceFileFolder.getType().equals(FileType.FILE)) {
              // Source is a file, destination is a folder
              // return destination short filename
              String shortFilename = sourceFileFolder.getName().getBaseName();

              try {
                shortFilename = getDestinationFilename(shortFilename);
              } catch (Exception e) {
                logError(
                    BaseMessages.getString(
                        PKG,
                        BaseMessages.getString(
                            PKG,
                            CONST_ACTION_MOVE_FILES_ERROR_GETTING_FILENAME,
                            sourceFileFolder.getName().getBaseName(),
                            e.toString())));
                return entryStatus;
              }
              // Move the file to the destination folder

              String destinationFilenameFull =
                  HopVfs.getFilename(destinationFileFolder) + Const.FILE_SEPARATOR + shortFilename;
              FileObject destinationFile =
                  HopVfs.getFileObject(destinationFilenameFull, getVariables());

              createFolderIfNotExists(destinationFileFolder);

              entryStatus =
                  moveFile(
                      shortFilename,
                      sourceFileFolder,
                      destinationFile,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);
              return entryStatus;
            } else if (sourceFileFolder.getType().equals(FileType.FILE) && destinationIsAFile) {
              // Source is a file, destination is a file

              FileObject destinationfile =
                  HopVfs.getFileObject(realDestinationFilefoldername, getVariables());

              // return destination short filename
              String shortfilename = destinationfile.getName().getBaseName();
              try {
                shortfilename = getDestinationFilename(shortfilename);
              } catch (Exception e) {
                logError(
                    BaseMessages.getString(
                        PKG,
                        BaseMessages.getString(
                            PKG,
                            CONST_ACTION_MOVE_FILES_ERROR_GETTING_FILENAME,
                            sourceFileFolder.getName().getBaseName(),
                            e.toString())));
                return entryStatus;
              }

              if (destinationfile.getName().getURI().startsWith("azfs")) {
                // Special handling for "azfs" URIs
                destinationfile = HopVfs.getFileObject(destinationFileFolderName, getVariables());
              } else {
                String destinationfilenamefull =
                    HopVfs.getFilename(destinationfile.getParent())
                        + Const.FILE_SEPARATOR
                        + shortfilename;
                destinationfile = HopVfs.getFileObject(destinationfilenamefull, getVariables());
              }

              entryStatus =
                  moveFile(
                      shortfilename,
                      sourceFileFolder,
                      destinationfile,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);
              return entryStatus;
            } else {
              // Both source and destination are folders
              if (isDetailed()) {
                logDetailed("  ");
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionMoveFiles.Log.FetchFolder", sourceFileFolder.toString()));
              }

              FileObject[] fileObjects =
                  sourceFileFolder.findFiles(
                      new AllFileSelector() {
                        @Override
                        public boolean traverseDescendents(FileSelectInfo info) {
                          return true;
                        }

                        @Override
                        public boolean includeFile(FileSelectInfo info) {
                          FileObject fileObject = info.getFile();
                          try {
                            if (fileObject == null) {
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
                  // Success condition broken?
                  if (successConditionBroken) {
                    if (!successConditionBrokenExit) {
                      logError(
                          BaseMessages.getString(
                              PKG,
                              CONST_ACTION_MOVE_FILES_ERROR_SUCCESS_CONDITIONBROKEN,
                              "" + nrErrors));
                      successConditionBrokenExit = true;
                    }
                    return false;
                  }
                  // Fetch files in list one after one ...
                  currentFile = fileObjects[j];

                  if (!moveOneFile(
                      currentFile,
                      sourceFileFolder,
                      realDestinationFilefoldername,
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
          entryStatus = true;
        } else {
          // Destination Folder or Parent folder is missing
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionMoveFiles.Error.DestinationFolderNotFound",
                  realDestinationFilefoldername));
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ActionMoveFiles.Error.SourceFileNotExists", realSourceFilefoldername));
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionMoveFiles.Error.Exception.MoveProcess",
              realSourceFilefoldername,
              destinationFileFolder == null ? "" : destinationFileFolder.toString(),
              e.getMessage()));
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
    return entryStatus;
  }

  private boolean moveFile(
      String shortFilename,
      FileObject sourceFileFolder,
      FileObject destinationFilename,
      FileObject movetoFolderFolder,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {

    FileObject destinationFile = null;
    boolean retVal = false;
    try {
      if (!destinationFilename.exists()) {

        if (includeSubfolders) {
          // Check if
          FileObject destinationFilePath =
              HopVfs.getFileObject(
                  destinationFilename.getName().getParent().toString(), getVariables());
          if (!destinationFilePath.exists()) destinationFilePath.createFolder();
        }

        if (!simulate) {
          Long moved = trackBytesMoved(sourceFileFolder, result);
          destinationFilename.createFile();
          sourceFileFolder.moveTo(destinationFilename);
          emitMoveLineage(parentWorkflow, sourceFileFolder, destinationFilename, moved);
        }

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  CONST_ACTION_MOVE_FILES_LOG_FILE_MOVED,
                  sourceFileFolder.getName().toString(),
                  destinationFilename.getName().toString()));
        }

        // add filename to result filename
        if (addResultFilenames) {
          addFileToResultFilenames(destinationFilename.toString(), result, parentWorkflow);
        }

        updateSuccess();
        retVal = true;

      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionMoveFiles.Log.FileExists", destinationFilename.toString()));
        }

        switch (ifFileExists) {
          case "overwrite_file" -> {
            if (!simulate) {
              Long moved = trackBytesMoved(sourceFileFolder, result);
              sourceFileFolder.moveTo(destinationFilename);
              emitMoveLineage(parentWorkflow, sourceFileFolder, destinationFilename, moved);
            }
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionMoveFiles.Log.FileOverwrite",
                      destinationFilename.getName().toString()));
            }

            // add filename to result filename
            if (addResultFilenames) {
              addFileToResultFilenames(destinationFilename.toString(), result, parentWorkflow);
            }

            updateSuccess();
            retVal = true;
          }
          case "unique_name" -> {
            String shortDestinationFilename = shortFilename;

            // return destination short filename
            try {
              shortDestinationFilename =
                  getMoveDestinationFilename(shortDestinationFilename, "ddMMyyyy_HHmmssSSS");
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      BaseMessages.getString(
                          PKG,
                          CONST_ACTION_MOVE_FILES_ERROR_GETTING_FILENAME,
                          shortDestinationFilename)),
                  e);
              return retVal;
            }

            String movetofilenamefull =
                destinationFilename.getParent().toString()
                    + Const.FILE_SEPARATOR
                    + shortDestinationFilename;
            destinationFile = HopVfs.getFileObject(movetofilenamefull, getVariables());

            if (!simulate) {
              Long moved = trackBytesMoved(sourceFileFolder, result);
              sourceFileFolder.moveTo(destinationFile);
              emitMoveLineage(parentWorkflow, sourceFileFolder, destinationFile, moved);
            }
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_MOVE_FILES_LOG_FILE_MOVED,
                      sourceFileFolder.getName().toString(),
                      destinationFile.getName().toString()));
            }

            // add filename to result filename
            if (addResultFilenames) {
              addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
            }

            updateSuccess();
            retVal = true;
          }
          case DELETE_FILE -> {
            if (!simulate) {
              Long sz = fileContentSizeOrNull(sourceFileFolder);
              sourceFileFolder.delete();
              emitDeleteLineage(parentWorkflow, sourceFileFolder, sz);
            }
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionMoveFiles.Log.FileDeleted",
                      destinationFilename.getName().toString()));
            }
            updateSuccess();
            retVal = true;
          }
          case "move_file" -> {
            String shortDestinationFilename = shortFilename;
            // return destination short filename
            try {
              shortDestinationFilename = getMoveDestinationFilename(shortDestinationFilename, null);
            } catch (Exception e) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      BaseMessages.getString(
                          PKG,
                          CONST_ACTION_MOVE_FILES_ERROR_GETTING_FILENAME,
                          shortDestinationFilename)),
                  e);
              return retVal;
            }

            String moveToFilenameFull =
                movetoFolderFolder.toString() + Const.FILE_SEPARATOR + shortDestinationFilename;
            destinationFile = HopVfs.getFileObject(moveToFilenameFull, getVariables());
            if (!destinationFile.exists()) {
              if (!simulate) {
                Long moved = trackBytesMoved(sourceFileFolder, result);
                sourceFileFolder.moveTo(destinationFile);
                emitMoveLineage(parentWorkflow, sourceFileFolder, destinationFile, moved);
              }
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        CONST_ACTION_MOVE_FILES_LOG_FILE_MOVED,
                        sourceFileFolder.getName().toString(),
                        destinationFile.getName().toString()));
              }

              // add filename to result filename
              if (addResultFilenames) {
                addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
              }

            } else {
              switch (ifMovedFileExists) {
                case "overwrite_file" -> {
                  if (!simulate) {
                    Long moved = trackBytesMoved(sourceFileFolder, result);
                    sourceFileFolder.moveTo(destinationFile);
                    emitMoveLineage(parentWorkflow, sourceFileFolder, destinationFile, moved);
                  }
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            "ActionMoveFiles.Log.FileOverwrite",
                            destinationFile.getName().toString()));
                  }

                  // add filename to result filename
                  if (addResultFilenames) {
                    addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
                  }

                  updateSuccess();
                  retVal = true;
                }
                case "unique_name" -> {
                  SimpleDateFormat daf = new SimpleDateFormat();
                  Date now = new Date();
                  daf.applyPattern("ddMMyyyy_HHmmssSSS");
                  String dt = daf.format(now);
                  shortDestinationFilename += "_" + dt;

                  String destinationFilenameFull =
                      movetoFolderFolder.toString()
                          + Const.FILE_SEPARATOR
                          + shortDestinationFilename;
                  destinationFile = HopVfs.getFileObject(destinationFilenameFull, getVariables());

                  if (!simulate) {
                    Long moved = trackBytesMoved(sourceFileFolder, result);
                    sourceFileFolder.moveTo(destinationFile);
                    emitMoveLineage(parentWorkflow, sourceFileFolder, destinationFile, moved);
                  }
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            CONST_ACTION_MOVE_FILES_LOG_FILE_MOVED,
                            destinationFile.getName().toString()));
                  }

                  // add filename to result filename
                  if (addResultFilenames) {
                    addFileToResultFilenames(destinationFile.toString(), result, parentWorkflow);
                  }

                  updateSuccess();
                  retVal = true;
                }
                case FAIL ->
                    // Update Errors
                    updateErrors();
              }
            }
          }
          case FAIL ->
              // Update Errors
              updateErrors();
          case CONST_DO_NOTHING -> retVal = true;
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionMoveFiles.Error.Exception.MoveProcessError",
              sourceFileFolder.toString(),
              destinationFilename.toString(),
              e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
      updateErrors();
    } finally {
      if (destinationFile != null) {
        try {
          destinationFile.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return retVal;
  }

  private boolean moveOneFile(
      FileObject currentFile,
      FileObject sourceFileFolder,
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
        String sourceShortFilename = currentFile.getName().getBaseName();
        String shortDestinationFilename = sourceShortFilename;
        try {
          shortDestinationFilename = getDestinationFilename(sourceShortFilename);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG,
                  BaseMessages.getString(
                      PKG,
                      CONST_ACTION_MOVE_FILES_ERROR_GETTING_FILENAME,
                      currentFile.getName().getBaseName(),
                      e.toString())));
          return entryStatus;
        }

        int lenCurrent = sourceShortFilename.length();
        String shortFilenameFromBaseFolder = shortDestinationFilename;
        if (!isDoNotKeepFolderStructure()) {
          shortFilenameFromBaseFolder =
              currentFile.toString().substring(sourceFileFolder.toString().length());
        }
        shortFilenameFromBaseFolder =
            shortFilenameFromBaseFolder.substring(
                    0, shortFilenameFromBaseFolder.length() - lenCurrent)
                + shortDestinationFilename;

        // Built destination filename
        filename =
            HopVfs.getFileObject(
                realDestinationFileFolderName + Const.FILE_SEPARATOR + shortFilenameFromBaseFolder,
                getVariables());

        if (!currentFile.getParent().toString().equals(sourceFileFolder.toString())) {
          // Not in the Base Folder..Only if include sub folders
          if (includeSubfolders) {
            // Folders... only if include subfolders
            if (currentFile.getType() == FileType.FOLDER) {
              if (includeSubfolders && moveEmptyFolders && Utils.isEmpty(realWildcard)) {
                entryStatus =
                    moveFile(
                        shortDestinationFilename,
                        currentFile,
                        filename,
                        moveToFolderFolder,
                        parentWorkflow,
                        result);
              }
            } else {
              if (getFileWildcard(sourceShortFilename, realWildcard)) {
                entryStatus =
                    moveFile(
                        shortDestinationFilename,
                        currentFile,
                        filename,
                        moveToFolderFolder,
                        parentWorkflow,
                        result);
              }
            }
          }
        } else {
          // In the Base Folder...
          // Folders..only if include subfolders
          if (currentFile.getType() == FileType.FOLDER) {
            if (includeSubfolders && moveEmptyFolders && Utils.isEmpty(realWildcard)) {
              entryStatus =
                  moveFile(
                      shortDestinationFilename,
                      currentFile,
                      filename,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);
            }
          } else {
            // file...Check if exists
            if (getFileWildcard(sourceShortFilename, realWildcard)) {
              entryStatus =
                  moveFile(
                      shortDestinationFilename,
                      currentFile,
                      filename,
                      moveToFolderFolder,
                      parentWorkflow,
                      result);
            }
          }
        }
      }
      entryStatus = true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionMoveFiles.Log.Error", e.toString()));
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
        logDebug(
            BaseMessages.getString(
                PKG, "ActionMoveFiles.Log.FileAddedToResultFilenames", fileAddEntry));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionMoveFiles.Error.AddingToFilenameResult"),
          fileAddEntry + e.getMessage(),
          e);
    }
  }

  /**
   * Adds the source file size to the result data-volume counters (when {@link
   * org.apache.hop.core.Const#HOP_METRIC_DATA_VOLUME} is enabled upstream) and returns that size
   * for lineage.
   */
  private Long trackBytesMoved(FileObject sourceFile, Result result) {
    try {
      if (sourceFile.getType().hasContent()) {
        long size = sourceFile.getContent().getSize();
        result.setBytesReadThisAction(result.getBytesReadThisAction() + size);
        result.setBytesWrittenThisAction(result.getBytesWrittenThisAction() + size);
        return size;
      }
    } catch (Exception e) {
      logDebug("Could not get size of source file: " + sourceFile);
    }
    return null;
  }

  private static Long fileContentSizeOrNull(FileObject f) {
    try {
      if (f != null && f.exists() && f.getType().hasContent()) {
        return f.getContent().getSize();
      }
    } catch (Exception ignored) {
      // ignore
    }
    return null;
  }

  private void emitMoveLineage(
      IWorkflowEngine<WorkflowMeta> workflow,
      FileObject source,
      FileObject destination,
      Long bytesTransferred) {
    if (simulate || workflow == null) {
      return;
    }
    LineageFileIoEmitter.emitWorkflowActionFileIo(
        workflow, this, FileIoOperation.MOVE, source, destination, bytesTransferred, true, null);
  }

  private void emitDeleteLineage(
      IWorkflowEngine<WorkflowMeta> workflow, FileObject source, Long bytesTransferred) {
    if (simulate || workflow == null) {
      return;
    }
    LineageFileIoEmitter.emitWorkflowActionFileIo(
        workflow, this, FileIoOperation.DELETE, source, null, bytesTransferred, true, null);
  }

  private boolean createDestinationFolder(FileObject filefolder) {
    FileObject folder = null;
    try {
      if (destinationIsAFile) {
        folder = filefolder.getParent();
      } else {
        folder = filefolder;
      }

      if (!folder.exists()) {
        if (createDestinationFolder) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionMoveFiles.Log.FolderNotExist", folder.getName().toString()));
          }
          folder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionMoveFiles.Log.FolderWasCreated", folder.getName().toString()));
          }
        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionMoveFiles.Log.FolderNotExist", folder.getName().toString()));
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionMoveFiles.Log.CanNotCreateParentFolder",
              (folder == null ? "" : folder.getName().toString())),
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
   * @param selectedfile The selected file
   * @param wildcard The wildcard
   * @return True if the selected file matches the wildcard
   **********************************************************/
  private boolean getFileWildcard(String selectedfile, String wildcard) {
    Pattern pattern = null;
    boolean getIt = true;

    if (!Utils.isEmpty(wildcard)) {
      pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
      Matcher matcher = pattern.matcher(selectedfile);
      getIt = matcher.matches();
    }

    return getIt;
  }

  private String getDestinationFilename(String shortSourceFilename) {
    String shortFilename = shortSourceFilename;
    int stringLength = shortSourceFilename.length();
    int lastIndexOfDot = shortFilename.lastIndexOf('.');
    if (lastIndexOfDot == -1) {
      lastIndexOfDot = stringLength;
    }

    if (isAddDateBeforeExtension()) {
      shortFilename = shortFilename.substring(0, lastIndexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (isSpecifyFormat() && !Utils.isEmpty(getDateTimeFormat())) {
      daf.applyPattern(getDateTimeFormat());
      String dt = daf.format(now);
      shortFilename += dt;
    } else {
      if (isAddDate()) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        shortFilename += "_" + d;
      }
      if (isAddTime()) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        shortFilename += "_" + t;
      }
    }
    if (isAddDateBeforeExtension()) {
      shortFilename += shortSourceFilename.substring(lastIndexOfDot, stringLength);
    }

    return shortFilename;
  }

  private String getMoveDestinationFilename(String shortSourceFilename, String dateFormat) {
    String shortFilename = shortSourceFilename;
    int stringLength = shortSourceFilename.length();
    int lastIndexOfDot = shortFilename.lastIndexOf('.');
    if (lastIndexOfDot == -1) {
      lastIndexOfDot = stringLength;
    }

    if (isAddMovedDateBeforeExtension()) {
      shortFilename = shortFilename.substring(0, lastIndexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (dateFormat != null) {
      daf.applyPattern(dateFormat);
      String dt = daf.format(now);
      shortFilename += dt;
    } else {
      if (isSpecifyMoveFormat() && !Utils.isEmpty(getMovedDateTimeFormat())) {
        daf.applyPattern(getMovedDateTimeFormat());
        String dt = daf.format(now);
        shortFilename += dt;
      } else {
        if (isAddMovedDate()) {
          daf.applyPattern("yyyyMMdd");
          String d = daf.format(now);
          shortFilename += "_" + d;
        }
        if (isAddMovedTime()) {
          daf.applyPattern("HHmmssSSS");
          String t = daf.format(now);
          shortFilename += "_" + t;
        }
      }
    }
    if (isAddMovedDateBeforeExtension()) {
      shortFilename += shortSourceFilename.substring(lastIndexOfDot, stringLength);
    }

    return shortFilename;
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

    for (int i = 0; i < filesToMove.size(); i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  /**
   * Ensures that the given FileObject represents an existing folder.
   *
   * @param folder the FileObject representing the target folder
   * @throws FileSystemException if the path exists as a file or the folder creation fails
   */
  private void createFolderIfNotExists(FileObject folder) throws FileSystemException {
    // If the path already exists, it's a folder (directory)
    if (folder.exists() && folder.getType().hasChildren()) {
      return;
    }

    // Ensure parent folder exists before creating this one
    FileObject parent = folder.getParent();
    if (parent != null && !parent.exists()) {
      parent.createFolder();
    }

    folder.createFolder();
  }

  @Getter
  @Setter
  public static final class FileToMove {
    @HopMetadataProperty(key = "source_filefolder")
    private String sourceFileFolder;

    @HopMetadataProperty(key = "destination_filefolder")
    private String destinationFileFolder;

    @HopMetadataProperty(key = "wildcard")
    private String wildcard;

    public FileToMove() {}

    public FileToMove(FileToMove f) {
      this();
      this.sourceFileFolder = f.sourceFileFolder;
      this.destinationFileFolder = f.destinationFileFolder;
      this.wildcard = f.wildcard;
    }
  }
}

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
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

/** This defines a 'move files' action. */
@Action(
    id = "MOVE_FILES",
    name = "i18n::ActionMoveFiles.Name",
    description = "i18n::ActionMoveFiles.Description",
    image = "MoveFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionMoveFiles.keyword",
    documentationUrl = "/workflow/actions/movefiles.html")
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

  public boolean moveEmptyFolders;
  public boolean argFromPrevious;
  public boolean includeSubfolders;
  public boolean addResultFilenames;
  public boolean destinationIsAFile;
  public boolean createDestinationFolder;
  public String[] sourceFileFolder;
  public String[] destinationFileFolder;
  public String[] wildcard;
  private String nrErrorsLessThan;

  private String successCondition;
  public static final String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private boolean addDate;
  private boolean addTime;
  private boolean specifyFormat;
  private String dateTimeFormat;
  private boolean addDateBeforeExtension;
  private boolean doNotKeepFolderStructure;
  private String ifFileExists;
  private String destinationFolder;
  private String ifMovedFileExists;
  private String movedDateTimeFormat;
  private boolean addMovedDateBeforeExtension;
  private boolean addMovedDate;
  private boolean addMovedTime;
  private boolean specifyMoveFormat;
  public boolean createMoveToFolder;
  public boolean simulate;

  int nrErrors = 0;
  int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  public ActionMoveFiles(String n) {
    super(n, "");
    simulate = false;
    createMoveToFolder = false;
    specifyMoveFormat = false;
    addMovedDate = false;
    addMovedTime = false;
    addMovedDateBeforeExtension = false;
    movedDateTimeFormat = null;
    ifMovedFileExists = CONST_DO_NOTHING;
    destinationFolder = null;
    doNotKeepFolderStructure = false;
    moveEmptyFolders = true;
    argFromPrevious = false;
    sourceFileFolder = null;
    destinationFileFolder = null;
    wildcard = null;
    includeSubfolders = false;
    addResultFilenames = false;
    destinationIsAFile = false;
    createDestinationFolder = false;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    dateTimeFormat = null;
    addDateBeforeExtension = false;
    ifFileExists = CONST_DO_NOTHING;
  }

  public ActionMoveFiles() {
    this("");
  }

  public void allocate(int nrFields) {
    sourceFileFolder = new String[nrFields];
    destinationFileFolder = new String[nrFields];
    wildcard = new String[nrFields];
  }

  @Override
  public Object clone() {
    ActionMoveFiles je = (ActionMoveFiles) super.clone();
    if (sourceFileFolder != null) {
      int nrFields = sourceFileFolder.length;
      je.allocate(nrFields);
      System.arraycopy(sourceFileFolder, 0, je.sourceFileFolder, 0, nrFields);
      System.arraycopy(wildcard, 0, je.wildcard, 0, nrFields);
      System.arraycopy(destinationFileFolder, 0, je.destinationFileFolder, 0, nrFields);
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(600);

    retval.append(super.getXml());
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("move_empty_folders", moveEmptyFolders));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("arg_from_previous", argFromPrevious));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("include_subfolders", includeSubfolders));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("add_result_filesname", addResultFilenames));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("destination_is_a_file", destinationIsAFile));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("create_destination_folder", createDestinationFolder));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_date", addDate));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_time", addTime));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("nr_errors_less_than", nrErrorsLessThan));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("success_condition", successCondition));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("AddDateBeforeExtension", addDateBeforeExtension));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("DoNotKeepFolderStructure", doNotKeepFolderStructure));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("iffileexists", ifFileExists));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("destinationFolder", destinationFolder));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("ifmovedfileexists", ifMovedFileExists));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("moved_date_time_format", movedDateTimeFormat));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("create_move_to_folder", createMoveToFolder));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_moved_date", addMovedDate));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("add_moved_time", addMovedTime));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("SpecifyMoveFormat", specifyMoveFormat));
    retval
        .append(CONST_SPACES)
        .append(XmlHandler.addTagValue("AddMovedDateBeforeExtension", addMovedDateBeforeExtension));
    retval.append(CONST_SPACES).append(XmlHandler.addTagValue("simulate", simulate));

    retval.append("      <fields>").append(Const.CR);
    if (sourceFileFolder != null) {
      for (int i = 0; i < sourceFileFolder.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("source_filefolder", sourceFileFolder[i]));
        retval
            .append(CONST_SPACES_LONG)
            .append(XmlHandler.addTagValue("destination_filefolder", destinationFileFolder[i]));
        retval.append(CONST_SPACES_LONG).append(XmlHandler.addTagValue("wildcard", wildcard[i]));
        retval.append("        </field>").append(Const.CR);
      }
    }
    retval.append("      </fields>").append(Const.CR);

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      moveEmptyFolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "move_empty_folders"));
      argFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "arg_from_previous"));
      includeSubfolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "include_subfolders"));
      addResultFilenames =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_result_filesname"));
      destinationIsAFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "destination_is_a_file"));
      createDestinationFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "create_destination_folder"));
      addDate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_date"));
      addTime = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_time"));
      specifyFormat = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "SpecifyFormat"));
      addDateBeforeExtension =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "AddDateBeforeExtension"));
      doNotKeepFolderStructure =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "DoNotKeepFolderStructure"));
      dateTimeFormat = XmlHandler.getTagValue(entrynode, "date_time_format");
      nrErrorsLessThan = XmlHandler.getTagValue(entrynode, "nr_errors_less_than");
      successCondition = XmlHandler.getTagValue(entrynode, "success_condition");
      ifFileExists = XmlHandler.getTagValue(entrynode, "iffileexists");
      destinationFolder = XmlHandler.getTagValue(entrynode, "destinationFolder");
      ifMovedFileExists = XmlHandler.getTagValue(entrynode, "ifmovedfileexists");
      movedDateTimeFormat = XmlHandler.getTagValue(entrynode, "moved_date_time_format");
      addMovedDateBeforeExtension =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "AddMovedDateBeforeExtension"));
      createMoveToFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "create_move_to_folder"));
      addMovedDate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_moved_date"));
      addMovedTime = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_moved_time"));
      specifyMoveFormat =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "SpecifyMoveFormat"));
      simulate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "simulate"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        sourceFileFolder[i] = XmlHandler.getTagValue(fnode, "source_filefolder");
        destinationFileFolder[i] = XmlHandler.getTagValue(fnode, "destination_filefolder");
        wildcard[i] = XmlHandler.getTagValue(fnode, "wildcard");
      }
    } catch (HopXmlException xe) {

      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionMoveFiles.Error.Exception.UnableLoadXML"), xe);
    }
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
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
    String[] vSourceFileFolder = sourceFileFolder;
    String[] vDestinationFileFolder = destinationFileFolder;
    String[] vwildcard = wildcard;

    if (ifFileExists.equals("move_file")) {
      if (Utils.isEmpty(moveToFolder)) {
        logError(BaseMessages.getString(PKG, "ActionMoveFiles.Log.Error.MoveToFolderMissing"));
        return result;
      }
      FileObject folder = null;
      try {
        folder = HopVfs.getFileObject(moveToFolder, getVariables());
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
      } finally {
        if (folder != null) {
          try {
            folder.close();
          } catch (IOException ex) {
            /* Ignore */
          }
        }
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
                    vSourceFileFolder[iteration],
                    vDestinationFileFolder[iteration],
                    vwildcard[iteration]));
          }
        }
      }
    } else if (vSourceFileFolder != null && vDestinationFileFolder != null) {
      for (int i = 0; i < vSourceFileFolder.length && !parentWorkflow.isStopped(); i++) {
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

        if (!Utils.isEmpty(vSourceFileFolder[i]) && !Utils.isEmpty(vDestinationFileFolder[i])) {
          // ok we can process this file/folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionMoveFiles.Log.ProcessingRow",
                    vSourceFileFolder[i],
                    vDestinationFileFolder[i],
                    vwildcard[i]));
          }

          if (!processFileFolder(
              vSourceFileFolder[i],
              vDestinationFileFolder[i],
              vwildcard[i],
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
                    vSourceFileFolder[i],
                    vDestinationFileFolder[i],
                    vwildcard[i]));
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
    boolean retval = false;

    if ((nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrSuccess >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }

    return retval;
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
              destinationFileFolder.toString(),
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
          destinationFilename.createFile();
          sourceFileFolder.moveTo(destinationFilename);
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
              sourceFileFolder.moveTo(destinationFilename);
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
              sourceFileFolder.moveTo(destinationFile);
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
          case "delete_file" -> {
            if (!simulate) {
              sourceFileFolder.delete();
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
                sourceFileFolder.moveTo(destinationFile);
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
                    sourceFileFolder.moveTo(destinationFile);
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
                    sourceFileFolder.moveTo(destinationFile);
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
                case "fail" ->
                    // Update Errors
                    updateErrors();
              }
            }
          }
          case "fail" ->
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
      FileObject movetoFolderFolder) {
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
              currentFile
                  .toString()
                  .substring(sourceFileFolder.toString().length(), currentFile.toString().length());
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
            // Folders..only if include subfolders
            if (currentFile.getType() == FileType.FOLDER) {
              if (includeSubfolders && moveEmptyFolders && Utils.isEmpty(wildcard)) {
                entryStatus =
                    moveFile(
                        shortDestinationFilename,
                        currentFile,
                        filename,
                        movetoFolderFolder,
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
                        movetoFolderFolder,
                        parentWorkflow,
                        result);
              }
            }
          }
        } else {
          // In the Base Folder...
          // Folders..only if include subfolders
          if (currentFile.getType() == FileType.FOLDER) {
            if (includeSubfolders && moveEmptyFolders && Utils.isEmpty(wildcard)) {
              entryStatus =
                  moveFile(
                      shortDestinationFilename,
                      currentFile,
                      filename,
                      movetoFolderFolder,
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
                      movetoFolderFolder,
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
      String fileaddentry, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    try {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              HopVfs.getFileObject(fileaddentry, getVariables()),
              parentWorkflow.getWorkflowName(),
              toString());
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "ActionMoveFiles.Log.FileAddedToResultFilenames", fileaddentry));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionMoveFiles.Error.AddingToFilenameResult"),
          fileaddentry + "" + e.getMessage());
    }
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
              PKG, "ActionMoveFiles.Log.CanNotCreateParentFolder", folder.getName().toString()),
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
   * @param selectedfile
   * @param wildcard
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean getFileWildcard(String selectedfile, String wildcard) {
    Pattern pattern = null;
    boolean getIt = true;

    if (!Utils.isEmpty(wildcard)) {
      pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
      if (pattern != null) {
        Matcher matcher = pattern.matcher(selectedfile);
        getIt = matcher.matches();
      }
    }

    return getIt;
  }

  private String getDestinationFilename(String shortsourcefilename) {
    String shortfilename = shortsourcefilename;
    int lenstring = shortsourcefilename.length();
    int lastindexOfDot = shortfilename.lastIndexOf('.');
    if (lastindexOfDot == -1) {
      lastindexOfDot = lenstring;
    }

    if (isAddDateBeforeExtension()) {
      shortfilename = shortfilename.substring(0, lastindexOfDot);
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
      shortfilename += shortsourcefilename.substring(lastindexOfDot, lenstring);
    }

    return shortfilename;
  }

  private String getMoveDestinationFilename(String shortsourcefilename, String dateFormat) {
    String shortfilename = shortsourcefilename;
    int lenstring = shortsourcefilename.length();
    int lastindexOfDot = shortfilename.lastIndexOf('.');
    if (lastindexOfDot == -1) {
      lastindexOfDot = lenstring;
    }

    if (isAddMovedDateBeforeExtension()) {
      shortfilename = shortfilename.substring(0, lastindexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (dateFormat != null) {
      daf.applyPattern(dateFormat);
      String dt = daf.format(now);
      shortfilename += dt;
    } else {

      if (isSpecifyMoveFormat() && !Utils.isEmpty(getMovedDateTimeFormat())) {
        daf.applyPattern(getMovedDateTimeFormat());
        String dt = daf.format(now);
        shortfilename += dt;
      } else {
        if (isAddMovedDate()) {
          daf.applyPattern("yyyyMMdd");
          String d = daf.format(now);
          shortfilename += "_" + d;
        }
        if (isAddMovedTime()) {
          daf.applyPattern("HHmmssSSS");
          String t = daf.format(now);
          shortfilename += "_" + t;
        }
      }
    }
    if (isAddMovedDateBeforeExtension()) {
      shortfilename += shortsourcefilename.substring(lastindexOfDot, lenstring);
    }

    return shortfilename;
  }

  public void setAddDate(boolean adddate) {
    this.addDate = adddate;
  }

  public boolean isAddDate() {
    return addDate;
  }

  public boolean isAddMovedDate() {
    return addMovedDate;
  }

  public void setAddMovedDate(boolean addMovedDate) {
    this.addMovedDate = addMovedDate;
  }

  public boolean isAddMovedTime() {
    return addMovedTime;
  }

  public void setAddMovedTime(boolean addMovedTime) {
    this.addMovedTime = addMovedTime;
  }

  public void setIfFileExists(String ifFileExists) {
    this.ifFileExists = ifFileExists;
  }

  public String getIfFileExists() {
    return ifFileExists;
  }

  public void setIfMovedFileExists(String ifMovedFileExists) {
    this.ifMovedFileExists = ifMovedFileExists;
  }

  public String getIfMovedFileExists() {
    return ifMovedFileExists;
  }

  public void setAddTime(boolean addtime) {
    this.addTime = addtime;
  }

  public boolean isAddTime() {
    return addTime;
  }

  public void setAddDateBeforeExtension(boolean addDateBeforeExtension) {
    this.addDateBeforeExtension = addDateBeforeExtension;
  }

  public void setAddMovedDateBeforeExtension(boolean addMovedDateBeforeExtension) {
    this.addMovedDateBeforeExtension = addMovedDateBeforeExtension;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat(boolean specifyFormat) {
    this.specifyFormat = specifyFormat;
  }

  public void setSpecifyMoveFormat(boolean specifyMoveFormat) {
    this.specifyMoveFormat = specifyMoveFormat;
  }

  public boolean isSpecifyMoveFormat() {
    return specifyMoveFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public String getMovedDateTimeFormat() {
    return movedDateTimeFormat;
  }

  public void setMovedDateTimeFormat(String movedDateTimeFormat) {
    this.movedDateTimeFormat = movedDateTimeFormat;
  }

  public boolean isAddDateBeforeExtension() {
    return addDateBeforeExtension;
  }

  public boolean isAddMovedDateBeforeExtension() {
    return addMovedDateBeforeExtension;
  }

  public boolean isDoNotKeepFolderStructure() {
    return doNotKeepFolderStructure;
  }

  public void setDestinationFolder(String destinationFolder) {
    this.destinationFolder = destinationFolder;
  }

  public String getDestinationFolder() {
    return destinationFolder;
  }

  public void setDoNotKeepFolderStructure(boolean doNotKeepFolderStructure) {
    this.doNotKeepFolderStructure = doNotKeepFolderStructure;
  }

  public void setMoveEmptyFolders(boolean moveEmptyFolders) {
    this.moveEmptyFolders = moveEmptyFolders;
  }

  public void setIncludeSubfolders(boolean includeSubfolders) {
    this.includeSubfolders = includeSubfolders;
  }

  public void setAddresultfilesname(boolean addResultFilenames) {
    this.addResultFilenames = addResultFilenames;
  }

  public void setArgFromPrevious(boolean argfrompreviousin) {
    this.argFromPrevious = argfrompreviousin;
  }

  public void setDestinationIsAFile(boolean destinationIsAFile) {
    this.destinationIsAFile = destinationIsAFile;
  }

  public void setCreateDestinationFolder(boolean createDestinationFolder) {
    this.createDestinationFolder = createDestinationFolder;
  }

  public void setCreateMoveToFolder(boolean createMoveToFolder) {
    this.createMoveToFolder = createMoveToFolder;
  }

  public void setNrErrorsLessThan(String nrErrorsLessThan) {
    this.nrErrorsLessThan = nrErrorsLessThan;
  }

  public String getNrErrorsLessThan() {
    return nrErrorsLessThan;
  }

  public void setSimulate(boolean simulate) {
    this.simulate = simulate;
  }

  public void setSuccessCondition(String successCondition) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
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

    if (res == false) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < sourceFileFolder.length; i++) {
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
}

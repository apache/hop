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

package org.apache.hop.workflow.actions.unzip;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
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
import org.apache.hop.core.util.StringUtil;
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
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'unzip' action. Its main use would be to unzip files in a directory */
@Action(
    id = "UNZIP",
    name = "i18n::ActionUnZip.Name",
    description = "i18n::ActionUnZip.Description",
    image = "UnZip.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionUnZip.keyword",
    documentationUrl = "/workflow/actions/unzip.html")
@SuppressWarnings("java:S1104")
@Getter
@Setter
public class ActionUnZip extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionUnZip.class;
  public static final String CONST_ACTION_UN_ZIP_ERROR_SUCCESS_CONDITIONBROKEN =
      "ActionUnZip.Error.SuccessConditionbroken";
  public static final String CONST_SPACES = "      ";

  @HopMetadataProperty(key = "zipfilename")
  private String zipFilename;

  @HopMetadataProperty(key = "afterunzip")
  public int afterUnzip;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "wildcardexclude")
  private String wildcardExclude;

  @HopMetadataProperty(key = "targetdirectory")
  private String sourceDirectory; // targetdirectory on screen

  @HopMetadataProperty(key = "movetodirectory")
  private String moveToDirectory;

  @HopMetadataProperty(key = "addfiletoresult")
  private boolean addFileToResult;

  @HopMetadataProperty(key = "isfromprevious")
  private boolean fromPrevious;

  @HopMetadataProperty(key = "adddate")
  private boolean addDate;

  @HopMetadataProperty(key = "addtime")
  private boolean addTime;

  @HopMetadataProperty(key = "SpecifyFormat")
  private boolean specifyFormat;

  @HopMetadataProperty(key = "date_time_format")
  private String dateTimeFormat;

  @HopMetadataProperty(key = "rootzip")
  private boolean rootZip;

  @HopMetadataProperty(key = "createfolder")
  private boolean createFolder;

  @HopMetadataProperty(key = "nr_limit")
  private String nrLimit;

  @HopMetadataProperty(key = "wildcardSource")
  private String wildcardSource;

  @HopMetadataProperty(key = "iffileexists", storeWithCode = true)
  private FileExistsEnum ifFileExist;

  @HopMetadataProperty(key = "create_move_to_directory")
  private boolean createMoveToDirectory;

  @HopMetadataProperty(key = "addOriginalTimestamp")
  private boolean addOriginalTimestamp;

  @HopMetadataProperty(key = "setOriginalModificationDate")
  private boolean setOriginalModificationDate;

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  public static final String[] typeIfFileExistsDesc = {
    BaseMessages.getString(PKG, "ActionUnZip.Skip.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.Overwrite.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.Give_Unique_Name.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.Fail.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.OverwriteIfSizeDifferent.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.OverwriteIfSizeEquals.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.OverwriteIfZipBigger.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.OverwriteIfZipBiggerOrEqual.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.OverwriteIfZipSmaller.Label"),
    BaseMessages.getString(PKG, "ActionUnZip.OverwriteIfZipSmallerOrEqual.Label"),
  };

  private int nrErrors = 0;
  private int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  private static SimpleDateFormat daf;
  private boolean dateFormatSet = false;

  public ActionUnZip(String n) {
    super(n, "");
    zipFilename = null;
    afterUnzip = 0;
    wildcard = null;
    wildcardExclude = null;
    sourceDirectory = null;
    moveToDirectory = null;
    addFileToResult = false;
    fromPrevious = false;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    rootZip = false;
    createFolder = false;
    nrLimit = "10";
    wildcardSource = null;
    ifFileExist = FileExistsEnum.SKIP;
    successCondition = SUCCESS_IF_NO_ERRORS;
    createMoveToDirectory = false;

    addOriginalTimestamp = false;
    setOriginalModificationDate = false;
  }

  public ActionUnZip() {
    this("");
  }

  @Override
  public Object clone() {
    ActionUnZip je = (ActionUnZip) super.clone();
    return je;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    String realFilenameSource = resolve(zipFilename);
    String realWildcardSource = resolve(wildcardSource);
    String realWildcard = resolve(wildcard);
    String realWildcardExclude = resolve(wildcardExclude);
    String realTargetdirectory = resolve(sourceDirectory);
    String realMovetodirectory = resolve(moveToDirectory);

    limitFiles = Const.toInt(resolve(getNrLimit()), 10);
    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;

    if (fromPrevious) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionUnZip.Log.ArgFromPrevious.Found",
                (rows != null ? rows.size() : 0) + ""));
      }

      if (rows.isEmpty()) {
        return result;
      }
    } else {
      if (Utils.isEmpty(zipFilename)) {
        // Zip file/folder is missing
        logError(BaseMessages.getString(PKG, "ActionUnZip.No_ZipFile_Defined.Label"));
        return result;
      }
    }

    FileObject fileObject = null;
    FileObject targetdir = null;
    FileObject movetodir = null;

    try {

      // Let's make some checks here, before running action ...

      if (Utils.isEmpty(realTargetdirectory)) {
        logError(BaseMessages.getString(PKG, "ActionUnZip.Error.TargetFolderMissing"));
        return result;
      }

      boolean exitaction = false;

      // Target folder
      targetdir = HopVfs.getFileObject(realTargetdirectory, getVariables());

      if (!targetdir.exists()) {
        if (createFolder) {
          targetdir.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionUnZip.Log.TargetFolderCreated", realTargetdirectory));
          }

        } else {
          logError(BaseMessages.getString(PKG, "ActionUnZip.TargetFolderNotFound.Label"));
          exitaction = true;
        }
      } else {
        if (targetdir.getType() != FileType.FOLDER) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionUnZip.TargetFolderNotFolder.Label", realTargetdirectory));
          exitaction = true;
        } else {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionUnZip.TargetFolderExists.Label", realTargetdirectory));
          }
        }
      }

      // If user want to move zip files after process
      // movetodirectory must be provided
      if (afterUnzip == 2) {
        if (Utils.isEmpty(moveToDirectory)) {
          logError(BaseMessages.getString(PKG, "ActionUnZip.MoveToDirectoryEmpty.Label"));
          exitaction = true;
        } else {
          movetodir = HopVfs.getFileObject(realMovetodirectory, getVariables());
          if (!(movetodir.exists()) || movetodir.getType() != FileType.FOLDER) {
            if (createMoveToDirectory) {
              movetodir.createFolder();
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionUnZip.Log.MoveToFolderCreated", realMovetodirectory));
              }
            } else {
              logError(BaseMessages.getString(PKG, "ActionUnZip.MoveToDirectoryNotExists.Label"));
              exitaction = true;
            }
          }
        }
      }

      // We found errors...now exit
      if (exitaction) {
        return result;
      }

      if (fromPrevious) {
        if (rows != null) { // Copy the input row to the (command line) arguments
          for (int iteration = 0;
              iteration < rows.size() && !parentWorkflow.isStopped();
              iteration++) {
            if (successConditionBroken) {
              if (!successConditionBrokenExit) {
                logError(
                    BaseMessages.getString(
                        PKG, CONST_ACTION_UN_ZIP_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
                successConditionBrokenExit = true;
              }
              result.setNrErrors(nrErrors);
              return result;
            }

            resultRow = rows.get(iteration);

            // Get sourcefile/folder and wildcard
            realFilenameSource = resultRow.getString(0, null);
            realWildcardSource = resultRow.getString(1, null);

            fileObject = HopVfs.getFileObject(realFilenameSource, getVariables());
            if (fileObject.exists()) {
              processOneFile(
                  result,
                  parentWorkflow,
                  fileObject,
                  realTargetdirectory,
                  realWildcard,
                  realWildcardExclude,
                  movetodir,
                  realMovetodirectory,
                  realWildcardSource);
            } else {
              updateErrors();
              logError(
                  BaseMessages.getString(
                      PKG, "ActionUnZip.Error.CanNotFindFile", realFilenameSource));
            }
          }
        }
      } else {
        fileObject = HopVfs.getFileObject(realFilenameSource, getVariables());
        if (!fileObject.exists()) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionUnZip.ZipFile.NotExists.Label", realFilenameSource));
          return result;
        }

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionUnZip.Zip_FileExists.Label", realFilenameSource));
        }
        if (Utils.isEmpty(sourceDirectory)) {
          logError(BaseMessages.getString(PKG, "ActionUnZip.SourceFolderNotFound.Label"));
          return result;
        }

        processOneFile(
            result,
            parentWorkflow,
            fileObject,
            realTargetdirectory,
            realWildcard,
            realWildcardExclude,
            movetodir,
            realMovetodirectory,
            realWildcardSource);
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionUnZip.ErrorUnzip.Label", realFilenameSource, e.getMessage()));
      updateErrors();
    } finally {
      if (fileObject != null) {
        try {
          fileObject.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (targetdir != null) {
        try {
          targetdir.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (movetodir != null) {
        try {
          movetodir.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }

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
      logDetailed(BaseMessages.getString(PKG, "ActionUnZip.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(PKG, "ActionUnZip.Log.Info.FilesInSuccess", "" + nrSuccess));
      logDetailed("=======================================");
    }
  }

  private boolean processOneFile(
      Result result,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      FileObject fileObject,
      String realTargetdirectory,
      String realWildcard,
      String realWildcardExclude,
      FileObject movetodir,
      String realMovetodirectory,
      String realWildcardSource) {
    boolean retval = false;

    try {
      if (fileObject.getType().equals(FileType.FILE)) {
        // We have to unzip one zip file
        if (!unzipFile(
            fileObject,
            realTargetdirectory,
            realWildcard,
            realWildcardExclude,
            result,
            parentWorkflow,
            movetodir,
            realMovetodirectory)) {
          updateErrors();
        } else {
          updateSuccess();
        }
      } else {
        // Folder..let's see wildcard
        FileObject[] children = fileObject.getChildren();

        for (int i = 0; i < children.length && !parentWorkflow.isStopped(); i++) {
          if (successConditionBroken) {
            if (!successConditionBrokenExit) {
              logError(
                  BaseMessages.getString(
                      PKG, CONST_ACTION_UN_ZIP_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
              successConditionBrokenExit = true;
            }
            return false;
          }
          // Get only file!
          if (!children[i].getType().equals(FileType.FOLDER)) {
            boolean unzip = true;

            String filename = children[i].getName().getPath();

            Pattern patternSource = null;

            if (!Utils.isEmpty(realWildcardSource)) {
              patternSource = Pattern.compile(realWildcardSource);
            }

            // First see if the file matches the regular expression!
            if (patternSource != null) {
              Matcher matcher = patternSource.matcher(filename);
              unzip = matcher.matches();
            }
            if (unzip) {
              if (!unzipFile(
                  children[i],
                  realTargetdirectory,
                  realWildcard,
                  realWildcardExclude,
                  result,
                  parentWorkflow,
                  movetodir,
                  realMovetodirectory)) {
                updateErrors();
              } else {
                updateSuccess();
              }
            }
          }
        }
      }
    } catch (Exception e) {
      updateErrors();
      logError(BaseMessages.getString(PKG, "ActionUnZip.Error.Label", e.getMessage()));
    } finally {
      if (fileObject != null) {
        try {
          fileObject.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return retval;
  }

  private boolean unzipFile(
      FileObject sourceFileObject,
      String realTargetdirectory,
      String realWildcard,
      String realWildcardExclude,
      Result result,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      FileObject movetodir,
      String realMovetodirectory) {
    boolean retval = false;
    String unzipToFolder = realTargetdirectory;
    try {

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionUnZip.Log.ProcessingFile", sourceFileObject.toString()));
      }

      // Do you create a root folder?
      //
      if (rootZip) {
        String shortSourceFilename = sourceFileObject.getName().getBaseName();
        int lenstring = shortSourceFilename.length();
        int lastindexOfDot = shortSourceFilename.lastIndexOf('.');
        if (lastindexOfDot == -1) {
          lastindexOfDot = lenstring;
        }

        String folderName =
            realTargetdirectory + "/" + shortSourceFilename.substring(0, lastindexOfDot);
        FileObject rootfolder = HopVfs.getFileObject(folderName, getVariables());
        if (!rootfolder.exists()) {
          try {
            rootfolder.createFolder();
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionUnZip.Log.RootFolderCreated", folderName));
            }
          } catch (Exception e) {
            throw new Exception(
                BaseMessages.getString(PKG, "ActionUnZip.Error.CanNotCreateRootFolder", folderName),
                e);
          }
        }
        unzipToFolder = folderName;
      }

      // Try to read the entries from the VFS object...
      //
      String zipFilename = "zip:" + sourceFileObject.getName().getFriendlyURI();
      FileObject zipFile = HopVfs.getFileObject(zipFilename, getVariables());
      FileObject[] items =
          zipFile.findFiles(
              new AllFileSelector() {
                @Override
                public boolean traverseDescendents(FileSelectInfo info) {
                  return true;
                }

                @Override
                public boolean includeFile(FileSelectInfo info) {
                  // Never return the parent directory of a file list.
                  if (info.getDepth() == 0) {
                    return false;
                  }

                  FileObject fileObject = info.getFile();
                  return fileObject != null;
                }
              });

      Pattern pattern = null;
      if (!Utils.isEmpty(realWildcard)) {
        pattern = Pattern.compile(realWildcard);
      }
      Pattern patternexclude = null;
      if (!Utils.isEmpty(realWildcardExclude)) {
        patternexclude = Pattern.compile(realWildcardExclude);
      }

      for (FileObject item : items) {

        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG, CONST_ACTION_UN_ZIP_ERROR_SUCCESS_CONDITIONBROKEN, "" + nrErrors));
            successConditionBrokenExit = true;
          }
          return false;
        }

        synchronized (HopVfs.getFileSystemManager(getVariables())) {
          FileObject newFileObject = null;
          try {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionUnZip.Log.ProcessingZipEntry",
                      item.getName().getURI(),
                      sourceFileObject.toString()));
            }

            // get real destination filename
            //
            String newFileName = unzipToFolder + Const.FILE_SEPARATOR + getTargetFilename(item);
            newFileObject = HopVfs.getFileObject(newFileName, getVariables());

            if (item.getType().equals(FileType.FOLDER)) {
              // Directory
              //
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionUnZip.CreatingDirectory.Label", newFileName));
              }

              // Create Directory if necessary ...
              //
              if (!newFileObject.exists()) {
                newFileObject.createFolder();
              }
            } else {
              // File
              //
              boolean getIt = true;
              boolean getItexclude = false;

              // First see if the file matches the regular expression!
              //
              if (pattern != null) {
                Matcher matcher = pattern.matcher(item.getName().getURI());
                getIt = matcher.matches();
              }

              if (patternexclude != null) {
                Matcher matcherexclude = patternexclude.matcher(item.getName().getURI());
                getItexclude = matcherexclude.matches();
              }

              boolean take = takeThisFile(item, newFileName);

              if (getIt && !getItexclude && take) {
                if (isDetailed()) {
                  logDetailed(
                      BaseMessages.getString(
                          PKG,
                          "ActionUnZip.ExtractingEntry.Label",
                          item.getName().getURI(),
                          newFileName));
                }

                if (ifFileExist == FileExistsEnum.UNIQ) {
                  // Create file with unique name

                  int lenstring = newFileName.length();
                  int lastindexOfDot = newFileName.lastIndexOf('.');
                  if (lastindexOfDot == -1) {
                    lastindexOfDot = lenstring;
                  }

                  newFileName =
                      newFileName.substring(0, lastindexOfDot)
                          + StringUtil.getFormattedDateTimeNow(true)
                          + newFileName.substring(lastindexOfDot, lenstring);

                  if (isDebug()) {
                    logDebug(
                        BaseMessages.getString(
                            PKG, "ActionUnZip.Log.CreatingUniqFile", newFileName));
                  }
                }

                // See if the folder to the target file exists...
                //
                if (!newFileObject.getParent().exists()) {
                  newFileObject.getParent().createFolder(); // creates the whole path.
                }
                InputStream is = null;
                OutputStream os = null;

                try {
                  is = HopVfs.getInputStream(item);
                  os = HopVfs.getOutputStream(newFileObject, false);

                  if (is != null) {
                    byte[] buff = new byte[2048];
                    int len;

                    while ((len = is.read(buff)) > 0) {
                      os.write(buff, 0, len);
                    }

                    // Add filename to result filenames
                    addFilenameToResultFilenames(result, parentWorkflow, newFileName);
                  }
                } finally {
                  if (is != null) {
                    is.close();
                  }
                  if (os != null) {
                    os.close();
                  }
                }
              } // end if take
            }
          } catch (Exception e) {
            updateErrors();
            logError(
                BaseMessages.getString(
                    PKG,
                    "ActionUnZip.Error.CanNotProcessZipEntry",
                    item.getName().getURI(),
                    sourceFileObject.toString()),
                e);
          } finally {
            if (newFileObject != null) {
              try {
                newFileObject.close();
                if (setOriginalModificationDate) {
                  // Change last modification date
                  newFileObject
                      .getContent()
                      .setLastModifiedTime(item.getContent().getLastModifiedTime());
                }
              } catch (Exception e) {
                /* Ignore */
              } // ignore this
            }
            // Close file object
            // close() does not release resources!
            HopVfs.getFileSystemManager(getVariables()).closeFileSystem(item.getFileSystem());
            if (items != null) {
              items = null;
            }
          }
        } // Synchronized block on HopVfs.getInstance().getFileSystemManager()
      } // End for

      // Here gc() is explicitly called if e.g. createfile is used in the same
      // workflow for the same file. The problem is that after creating the file the
      // file object is not properly garbaged collected and thus the file cannot
      // be deleted anymore. This is a known problem in the JVM.

      // Unzip done...
      if (afterUnzip > 0) {
        doUnzipPostProcessing(sourceFileObject, movetodir, realMovetodirectory);
      }
      retval = true;
    } catch (Exception e) {
      updateErrors();
      logError(
          BaseMessages.getString(
              PKG, "ActionUnZip.ErrorUnzip.Label", sourceFileObject.toString(), e.getMessage()),
          e);
    }

    return retval;
  }

  /** Moving or deleting source file. */
  private void doUnzipPostProcessing(
      FileObject sourceFileObject, FileObject movetodir, String realMovetodirectory)
      throws FileSystemException {
    if (afterUnzip == 1) {
      // delete zip file
      boolean deleted = sourceFileObject.delete();
      if (!deleted) {
        updateErrors();
        logError(
            BaseMessages.getString(
                PKG, "ActionUnZip.Cant_Delete_File.Label", sourceFileObject.toString()));
      }
      // File deleted
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "ActionUnZip.File_Deleted.Label", sourceFileObject.toString()));
      }
    } else if (afterUnzip == 2) {
      FileObject destFile = null;
      // Move File
      try {
        String destinationFilename =
            movetodir + Const.FILE_SEPARATOR + sourceFileObject.getName().getBaseName();
        destFile = HopVfs.getFileObject(destinationFilename, getVariables());

        sourceFileObject.moveTo(destFile);

        // File moved
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileMovedTo",
                  sourceFileObject.toString(),
                  realMovetodirectory));
        }
      } catch (Exception e) {
        updateErrors();
        logError(
            BaseMessages.getString(
                PKG,
                "ActionUnZip.Cant_Move_File.Label",
                sourceFileObject.toString(),
                realMovetodirectory,
                e.getMessage()));
      } finally {
        if (destFile != null) {
          try {
            destFile.close();
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
    }
  }

  private void addFilenameToResultFilenames(
      Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, String newfile)
      throws Exception {
    if (addFileToResult) {
      // Add file to result files name
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              HopVfs.getFileObject(newfile, getVariables()),
              parentWorkflow.getWorkflowName(),
              toString());
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
    }
  }

  private void updateErrors() {
    nrErrors++;
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ((nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }
    return retval;
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

  private boolean takeThisFile(FileObject sourceFile, String destinationFile)
      throws FileSystemException {
    boolean retval = false;
    File destination = new File(destinationFile);
    if (!destination.exists()) {
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionUnZip.Log.CanNotFindFile", destinationFile));
      }
      return true;
    }
    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ActionUnZip.Log.FileExists", destinationFile));
    }
    if (ifFileExist == FileExistsEnum.SKIP) {
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionUnZip.Log.FileSkip", destinationFile));
      }
      return false;
    }
    if (ifFileExist == FileExistsEnum.FAIL) {
      updateErrors();
      logError(
          BaseMessages.getString(PKG, "ActionUnZip.Log.FileError", destinationFile, "" + nrErrors));
      return false;
    }

    if (ifFileExist == FileExistsEnum.OVERWRITE) {
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionUnZip.Log.FileOverwrite", destinationFile));
      }
      return true;
    }

    Long entrySize = sourceFile.getContent().getSize();
    Long destinationSize = destination.length();

    if (ifFileExist == FileExistsEnum.OVERWRITE_DIFF_SIZE) {
      if (entrySize != destinationSize) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileDiffSize.Diff",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return true;
      } else {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileDiffSize.Same",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return false;
      }
    }
    if (ifFileExist == FileExistsEnum.OVERWRITE_EQUAL_SIZE) {
      if (entrySize == destinationSize) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileEqualSize.Same",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return true;
      } else {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileEqualSize.Diff",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return false;
      }
    }
    if (ifFileExist == FileExistsEnum.OVERWRITE_ZIP_BIG) {
      if (entrySize > destinationSize) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileBigSize.Big",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return true;
      } else {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileBigSize.Small",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return false;
      }
    }
    if (ifFileExist == FileExistsEnum.OVERWRITE_ZIP_BIG_EQUAL) {
      if (entrySize >= destinationSize) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileBigEqualSize.Big",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return true;
      } else {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileBigEqualSize.Small",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return false;
      }
    }
    if (ifFileExist == FileExistsEnum.OVERWRITE_ZIP_BIG_SMALL) {
      if (entrySize < destinationSize) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileSmallSize.Small",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return true;
      } else {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileSmallSize.Big",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return false;
      }
    }
    if (ifFileExist == FileExistsEnum.OVERWRITE_ZIP_BIG_SMALL_EQUAL) {
      if (entrySize <= destinationSize) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileSmallEqualSize.Small",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return true;
      } else {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "ActionUnZip.Log.FileSmallEqualSize.Big",
                  sourceFile.getName().getURI(),
                  "" + entrySize,
                  destinationFile,
                  "" + destinationSize));
        }
        return false;
      }
    }
    if (ifFileExist == FileExistsEnum.UNIQ) {
      // Create file with unique name
      return true;
    }

    return retval;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  protected String getTargetFilename(FileObject file) throws FileSystemException {

    String retval = "";
    String filename = file.getName().getPath();
    // Replace possible environment variables...
    if (filename != null) {
      retval = filename;
    }
    if (file.getType() != FileType.FILE) {
      return retval;
    }

    if (!specifyFormat && !addDate && !addTime) {
      return retval;
    }

    int lenstring = retval.length();
    int lastindexOfDot = retval.lastIndexOf('.');
    if (lastindexOfDot == -1) {
      lastindexOfDot = lenstring;
    }

    retval = retval.substring(0, lastindexOfDot);

    if (daf == null) {
      daf = new SimpleDateFormat();
    }

    Date timestamp = new Date();
    if (addOriginalTimestamp) {
      timestamp = new Date(file.getContent().getLastModifiedTime());
    }

    if (specifyFormat && !Utils.isEmpty(dateTimeFormat)) {
      if (!dateFormatSet) {
        daf.applyPattern(dateTimeFormat);
      }
      String dt = daf.format(timestamp);
      retval += dt;
    } else {

      if (addDate) {
        if (!dateFormatSet) {
          daf.applyPattern("yyyyMMdd");
        }
        String d = daf.format(timestamp);
        retval += "_" + d;
      }
      if (addTime) {
        if (!dateFormatSet) {
          daf.applyPattern("HHmmssSSS");
        }
        String t = daf.format(timestamp);
        retval += "_" + t;
      }
    }

    if (daf != null) {
      dateFormatSet = true;
    }

    retval += filename.substring(lastindexOfDot, lenstring);

    return retval;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ValidatorContext ctx1 = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx1, getVariables());
    AndValidator.putValidators(
        ctx1,
        ActionValidatorUtils.notBlankValidator(),
        ActionValidatorUtils.fileDoesNotExistValidator());

    ActionValidatorUtils.andValidator().validate(this, "zipFilename", remarks, ctx1);

    if (2 == afterUnzip) {
      // setting says to move
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "moveToDirectory",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    }

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "sourceDirectory",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}

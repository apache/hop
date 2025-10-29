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

package org.apache.hop.workflow.actions.zipfile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
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
import org.apache.hop.workflow.action.validator.FileDoesNotExistValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * This defines a 'zip file' action. Its main use would be to zip files in a directory and process
 * zipped files (deleted or move).
 */
@Action(
    id = "ZIP_FILE",
    name = "i18n::ActionZipFile.Name",
    description = "i18n::ActionZipFile.Description",
    image = "Zip.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionZipFile.keyword",
    documentationUrl = "/workflow/actions/zipfile.html")
public class ActionZipFile extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionZipFile.class;

  @HopMetadataProperty(key = "zipfilename")
  private String zipFilename;

  @HopMetadataProperty(key = "compressionrate")
  private int compressionRate;

  @HopMetadataProperty(key = "ifzipfileexists")
  private int ifZipFileExists;

  @HopMetadataProperty(key = "afterzip")
  private int afterZip;

  @HopMetadataProperty(key = "wildcard")
  private String wildCard;

  @HopMetadataProperty(key = "wildcardexclude")
  private String excludeWildCard;

  @HopMetadataProperty(key = "sourcedirectory")
  private String sourceDirectory;

  @HopMetadataProperty(key = "movetodirectory")
  private String moveToDirectory;

  @HopMetadataProperty(key = "addfiletoresult")
  private boolean addFileToResult;

  @HopMetadataProperty(key = "isfromprevious")
  private boolean fromPrevious;

  @HopMetadataProperty(key = "createparentfolder")
  private boolean createParentFolder;

  @HopMetadataProperty(key = "adddate")
  private boolean addDate;

  @HopMetadataProperty(key = "addtime")
  private boolean addTime;

  @HopMetadataProperty(key = "SpecifyFormat")
  private boolean specifyFormat;

  @HopMetadataProperty(key = "date_time_format")
  private String dateTimeFormat;

  @HopMetadataProperty(key = "createMoveToDirectory")
  private boolean createMoveToDirectory;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includingSubFolders;

  @HopMetadataProperty(key = "stored_source_path_depth")
  private String storedSourcePathDepth;

  /** Default constructor. */
  public ActionZipFile(String n) {
    super(n, "");
    dateTimeFormat = null;
    zipFilename = null;
    ifZipFileExists = 2;
    afterZip = 0;
    compressionRate = 1;
    wildCard = null;
    excludeWildCard = null;
    sourceDirectory = null;
    moveToDirectory = null;
    addFileToResult = false;
    fromPrevious = false;
    createParentFolder = false;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    createMoveToDirectory = false;
    includingSubFolders = true;
    storedSourcePathDepth = "1";
  }

  public ActionZipFile() {
    this("");
  }

  @Override
  public Object clone() {
    ActionZipFile je = (ActionZipFile) super.clone();
    return je;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  private boolean createParentFolder(String filename) {
    // Check for parent folder
    FileObject parentfolder = null;

    boolean result = false;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject(filename, getVariables()).getParent();

      if (!parentfolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionZipFile.CanNotFindFolder", "" + parentfolder.getName()));
        }
        parentfolder.createFolder();
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionZipFile.FolderCreated", "" + parentfolder.getName()));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionZipFile.FolderExists", "" + parentfolder.getName()));
        }
      }
      result = true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionZipFile.CanNotCreateFolder", "" + parentfolder.getName()),
          e);
    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
        } catch (Exception ex) {
          // Ignore
        }
      }
    }
    return result;
  }

  public boolean processRowFile(
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result,
      String realZipFilename,
      String realWildCard,
      String realExcludeWildcard,
      String realSourceDirectoryOrFile,
      String realMoveToDirectory,
      boolean createParentFolder)
      throws HopException {
    boolean fileExists = false;
    File tempFile = null;
    File fileZip;
    boolean successResult = true;
    boolean renameOk = false;
    boolean orginExist;

    // Check if target file/folder exists!
    String localSourceFilename;

    try (FileObject originFile = HopVfs.getFileObject(realSourceDirectoryOrFile, getVariables())) {
      localSourceFilename = HopVfs.getFilename(originFile);
      orginExist = originFile.exists();
    } catch (Exception e) {
      throw new HopException(
          "Error finding source file or directory: " + realSourceDirectoryOrFile, e);
    }

    String localrealZipfilename = realZipFilename;
    if (realZipFilename != null && orginExist) {

      try (FileObject fileObject = HopVfs.getFileObject(localrealZipfilename, getVariables())) {
        localrealZipfilename = HopVfs.getFilename(fileObject);
        // Check if Zip File exists
        if (fileObject.exists()) {
          fileExists = true;
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(PKG, "ActionZipFile.Zip_FileExists1.Label")
                    + localrealZipfilename
                    + BaseMessages.getString(PKG, "ActionZipFile.Zip_FileExists2.Label"));
          }
        }
        // Let's see if we need to create parent folder of destination zip filename
        if (createParentFolder) {
          createParentFolder(localrealZipfilename);
        }

        // Let's start the process now
        if (ifZipFileExists == 3 && fileExists) {
          // the zip file exists and user wants this to fail.
          successResult = false;
        } else if (ifZipFileExists == 2 && fileExists) {
          // the zip file exists and user want to do nothing
          if (addFileToResult) {
            // Add file to result files name
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    fileObject,
                    parentWorkflow.getWorkflowName(),
                    toString());
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
          }
        } else if (afterZip == 2 && realMoveToDirectory == null) {
          // After Zip, Move files..User must give a destination Folder
          successResult = false;
          logError(
              BaseMessages.getString(
                  PKG, "ActionZipFile.AfterZip_No_DestinationFolder_Defined.Label"));
        } else {
          // After Zip, Move files..User must give a destination Folder

          // Let's see if we deal with file or folder
          FileObject[] fileList;

          FileObject sourceFileOrFolder = HopVfs.getFileObject(localSourceFilename, getVariables());
          boolean isSourceDirectory = sourceFileOrFolder.getType().equals(FileType.FOLDER);
          final Pattern pattern;
          final Pattern excludePattern;

          if (isSourceDirectory) {
            // Let's prepare the pattern matcher for performance reasons.
            // We only do this if the target is a folder !
            //
            if (!Utils.isEmpty(realWildCard)) {
              pattern = Pattern.compile(realWildCard);
            } else {
              pattern = null;
            }
            if (!Utils.isEmpty(realExcludeWildcard)) {
              excludePattern = Pattern.compile(realExcludeWildcard);
            } else {
              excludePattern = null;
            }

            // Target is a directory
            // Get all the files in the directory...
            //
            if (includingSubFolders) {
              fileList =
                  sourceFileOrFolder.findFiles(
                      new ZipJobEntryPatternFileSelector(pattern, excludePattern));
            } else {
              fileList = sourceFileOrFolder.getChildren();
            }
          } else {
            pattern = null;
            excludePattern = null;

            // Target is a file
            fileList = new FileObject[] {sourceFileOrFolder};
          }

          if (fileList.length == 0) {
            successResult = false;
            logError(
                BaseMessages.getString(
                    PKG, "ActionZipFile.Log.FolderIsEmpty", localSourceFilename));
          } else if (!checkContainsFile(localSourceFilename, fileList, isSourceDirectory)) {
            successResult = false;
            logError(
                BaseMessages.getString(
                    PKG, "ActionZipFile.Log.NoFilesInFolder", localSourceFilename));
          } else {
            if (ifZipFileExists == 0 && fileExists) {
              // the zip file exists and user want to create new one with unique name
              // Format Date

              // do we have already a .zip at the end?
              if (localrealZipfilename.toLowerCase().endsWith(".zip")) {
                // strip this off
                localrealZipfilename =
                    localrealZipfilename.substring(0, localrealZipfilename.length() - 4);
              }

              localrealZipfilename += "_" + StringUtil.getFormattedDateTimeNow(true) + ".zip";
              if (isDebug()) {
                logDebug(
                    BaseMessages.getString(PKG, "ActionZipFile.Zip_FileNameChange1.Label")
                        + localrealZipfilename
                        + BaseMessages.getString(PKG, "ActionZipFile.Zip_FileNameChange1.Label"));
              }
            } else if (ifZipFileExists == 1 && fileExists) {
              // the zip file exists and user want to append
              // get a temp file
              fileZip = getFile(localrealZipfilename);
              tempFile = File.createTempFile(fileZip.getName(), null);

              // delete it, otherwise we cannot rename existing zip to it.
              if (!tempFile.delete()) {
                throw new HopException("Unable to delete temporary file " + tempFile);
              }

              renameOk = fileZip.renameTo(tempFile);

              if (!renameOk) {
                logError(
                    BaseMessages.getString(PKG, "ActionZipFile.Cant_Rename_Temp1.Label")
                        + fileZip.getAbsolutePath()
                        + BaseMessages.getString(PKG, "ActionZipFile.Cant_Rename_Temp2.Label")
                        + tempFile.getAbsolutePath()
                        + BaseMessages.getString(PKG, "ActionZipFile.Cant_Rename_Temp3.Label"));
              }
              if (isDebug()) {
                logDebug(
                    BaseMessages.getString(PKG, "ActionZipFile.Zip_FileAppend1.Label")
                        + localrealZipfilename
                        + BaseMessages.getString(PKG, "ActionZipFile.Zip_FileAppend2.Label"));
              }
            }

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionZipFile.Files_Found1.Label")
                      + fileList.length
                      + BaseMessages.getString(PKG, "ActionZipFile.Files_Found2.Label")
                      + localSourceFilename
                      + BaseMessages.getString(PKG, "ActionZipFile.Files_Found3.Label"));
            }

            // Keep track of the files added to the zip archive.
            //
            List<FileObject> zippedFiles = new ArrayList<>();

            // Prepare Zip File
            try (OutputStream dest =
                HopVfs.getOutputStream(localrealZipfilename, false, getVariables())) {
              try (BufferedOutputStream buff = new BufferedOutputStream(dest)) {
                try (ZipOutputStream out = new ZipOutputStream(buff)) {

                  HashSet<String> fileSet = new HashSet<>();

                  if (renameOk) {
                    // User want to append files to existing Zip file
                    // The idea is to rename the existing zip file to a temporary file
                    // and then adds all entries in the existing zip along with the new files,
                    // excluding the zip entries that have the same name as one of the new files.
                    //
                    moveRenameZipArchive(tempFile, fileSet, out);
                  }

                  // Set the method
                  out.setMethod(ZipOutputStream.DEFLATED);
                  // Set the compression level
                  if (compressionRate == 0) {
                    out.setLevel(Deflater.NO_COMPRESSION);
                  } else if (compressionRate == 1) {
                    out.setLevel(Deflater.DEFAULT_COMPRESSION);
                  }
                  if (compressionRate == 2) {
                    out.setLevel(Deflater.BEST_COMPRESSION);
                  }
                  if (compressionRate == 3) {
                    out.setLevel(Deflater.BEST_SPEED);
                  }
                  // Specify Zipped files (After that we will move,delete them...)
                  int fileNum = 0;

                  // Get the files in the list...
                  for (int i = 0; i < fileList.length && !parentWorkflow.isStopped(); i++) {
                    boolean getIt = true;
                    boolean getItexclude = false;

                    // First see if the file matches the regular expression.
                    // Do this only if the target is a folder.
                    //
                    if (isSourceDirectory) {
                      // If we include sub-folders, we match on the whole name, not just the
                      // basename
                      //
                      String filename;
                      if (includingSubFolders) {
                        filename = fileList[i].getName().getPath();
                      } else {
                        filename = fileList[i].getName().getBaseName();
                      }
                      if (pattern != null) {
                        // Matches the base name of the file (backward compatible!)
                        //
                        Matcher matcher = pattern.matcher(filename);
                        getIt = matcher.matches();
                      }

                      if (excludePattern != null) {
                        Matcher excludeMatcher = excludePattern.matcher(filename);
                        getItexclude = excludeMatcher.matches();
                      }
                    }

                    // Get processing File
                    //
                    String targetFilename = HopVfs.getFilename(fileList[i]);
                    if (sourceFileOrFolder.getType().equals(FileType.FILE)) {
                      targetFilename = localSourceFilename;
                    }

                    try (FileObject file = HopVfs.getFileObject(targetFilename, getVariables())) {
                      boolean isTargetDirectory =
                          file.exists() && file.getType().equals(FileType.FOLDER);

                      if (getIt
                          && !getItexclude
                          && !isTargetDirectory
                          && !fileSet.contains(targetFilename)) {
                        // We can add the file to the Zip Archive
                        if (isDebug()) {
                          logDebug(
                              BaseMessages.getString(PKG, "ActionZipFile.Add_FilesToZip1.Label")
                                  + fileList[i]
                                  + BaseMessages.getString(
                                      PKG, "ActionZipFile.Add_FilesToZip2.Label")
                                  + localSourceFilename
                                  + BaseMessages.getString(
                                      PKG, "ActionZipFile.Add_FilesToZip3.Label"));
                        }

                        // Associate a file input stream for the current file.
                        //
                        addFileToZip(file, fileList, i, sourceFileOrFolder, isSourceDirectory, out);

                        // Get Zipped File
                        zippedFiles.add(fileList[i]);
                        fileNum = fileNum + 1;
                      }
                    }
                  }
                }
              }
            }

            if (isBasic()) {
              logBasic(
                  BaseMessages.getString(
                      PKG, "ActionZipFile.Log.TotalZippedFiles", "" + zippedFiles.size()));
            }
            // Delete Temp File
            if (tempFile != null && !tempFile.delete()) {
              throw new HopException("Unable to delete temporary file " + tempFile);
            }

            // -----Get the list of Zipped Files and Move or Delete Them
            if (afterZip == 1 || afterZip == 2) {
              // iterate through the array of Zipped files
              for (FileObject fileObjectd : zippedFiles) {
                // Delete, Move File
                if (!isSourceDirectory) {
                  fileObjectd = HopVfs.getFileObject(localSourceFilename, getVariables());
                }

                // Here we can move, delete files
                if (afterZip == 1) {
                  successResult = deleteFile(successResult, fileObjectd, localSourceFilename);
                } else if (afterZip == 2) {
                  successResult = moveFile(successResult, fileObjectd, realMoveToDirectory);
                }

                // We no longer need this file, close it.
                //
                fileObjectd.close();
              }
            }

            if (addFileToResult) {
              // Add file to result files name
              ResultFile resultFile =
                  new ResultFile(
                      ResultFile.FILE_TYPE_GENERAL,
                      fileObject,
                      parentWorkflow.getWorkflowName(),
                      toString());
              result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
            }

            successResult = true;
          }
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "ActionZipFile.Cant_CreateZipFile1.Label")
                + localrealZipfilename
                + BaseMessages.getString(PKG, "ActionZipFile.Cant_CreateZipFile2.Label"),
            e);
        successResult = false;
      }
    } else {
      successResult = false;
      if (localrealZipfilename == null) {
        logError(BaseMessages.getString(PKG, "ActionZipFile.No_ZipFile_Defined.Label"));
      }
      if (!orginExist) {
        logError(
            BaseMessages.getString(
                PKG, "ActionZipFile.No_FolderCible_Defined.Label", localSourceFilename));
      }
    }
    // return a verifier
    return successResult;
  }

  private static void moveRenameZipArchive(
      File tempFile, HashSet<String> fileSet, ZipOutputStream out) throws IOException {
    ZipEntry entry;
    try (ZipInputStream zin = new ZipInputStream(new FileInputStream(tempFile))) {
      entry = zin.getNextEntry();

      while (entry != null) {
        String name = entry.getName();

        if (!fileSet.contains(name)) {

          // Add ZIP entry to output stream.
          out.putNextEntry(new ZipEntry(name));
          // Transfer bytes from the ZIP file to the output file
          int len;
          byte[] buffer = new byte[18024];
          while ((len = zin.read(buffer)) > 0) {
            out.write(buffer, 0, len);
          }

          fileSet.add(name);
        }
        entry = zin.getNextEntry();
      }
    }
  }

  private void addFileToZip(
      FileObject file,
      FileObject[] fileList,
      int i,
      FileObject sourceFileOrFolder,
      boolean isSourceDirectory,
      ZipOutputStream out)
      throws IOException, HopException {
    try (InputStream in = HopVfs.getInputStream(file)) {
      // Add ZIP entry to output stream.
      //
      String relativeName;
      String fullName = fileList[i].getName().getPath();
      String basePath = sourceFileOrFolder.getName().getPath();
      if (isSourceDirectory) {
        if (fullName.startsWith(basePath)) {
          relativeName = fullName.substring(basePath.length() + 1);
        } else {
          relativeName = fullName;
        }
      } else if (fromPrevious) {
        int depth = determineDepth(resolve(storedSourcePathDepth));
        relativeName = determineZipfilenameForDepth(fullName, depth);
      } else {
        relativeName = fileList[i].getName().getBaseName();
      }
      out.putNextEntry(new ZipEntry(relativeName));

      int len;
      byte[] buffer = new byte[18024];
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      out.flush();
      out.closeEntry();

      // Close the current file input stream
    }
  }

  private boolean moveFile(
      boolean successResult, FileObject fileObjectd, String realMoveToDirectory) {
    // Move File
    boolean success = successResult;
    try (FileObject fileObjectm =
        HopVfs.getFileObject(
            realMoveToDirectory + Const.FILE_SEPARATOR + fileObjectd.getName().getBaseName(),
            getVariables())) {

      fileObjectd.moveTo(fileObjectm);
    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionZipFile.Cant_Move_File1.Label")
              + fileObjectd
              + BaseMessages.getString(PKG, "ActionZipFile.Cant_Move_File2.Label")
              + e.getMessage());
      success = false;
    }
    // File moved
    if (isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "ActionZipFile.File_Moved1.Label")
              + fileObjectd
              + BaseMessages.getString(PKG, "ActionZipFile.File_Moved2.Label"));
    }
    return success;
  }

  private boolean deleteFile(
      boolean successResult, FileObject fileObjectd, String localSourceFilename)
      throws FileSystemException {
    // Delete File
    boolean success = successResult;
    boolean deleted = fileObjectd.delete();
    if (!deleted) {
      success = false;
      logError(
          BaseMessages.getString(PKG, "ActionZipFile.Cant_Delete_File1.Label")
              + localSourceFilename
              + Const.FILE_SEPARATOR
              + fileObjectd
              + BaseMessages.getString(PKG, "ActionZipFile.Cant_Delete_File2.Label"));
    }
    // File deleted
    if (isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "ActionZipFile.File_Deleted1.Label")
              + localSourceFilename
              + Const.FILE_SEPARATOR
              + fileObjectd
              + BaseMessages.getString(PKG, "ActionZipFile.File_Deleted2.Label"));
    }
    return success;
  }

  private int determineDepth(String depthString) throws HopException {
    DecimalFormat df = new DecimalFormat("0");
    ParsePosition pp = new ParsePosition(0);
    df.setParseIntegerOnly(true);
    try {
      Number n = df.parse(depthString, pp);
      if (n == null) {
        return 1; // default
      }
      if (pp.getErrorIndex() == 0) {
        throw new HopException(
            "Unable to convert stored depth '"
                + depthString
                + "' to depth at position "
                + pp.getErrorIndex());
      }
      return n.intValue();
    } catch (Exception e) {
      throw new HopException("Unable to convert stored depth '" + depthString + "' to depth", e);
    }
  }

  /**
   * Get the requested part of the filename
   *
   * @param filename the filename (full) (/path/to/a/file.txt)
   * @param depth the depth to get. 0 means: the complete filename, 1: the name only (file.txt), 2:
   *     one folder (a/file.txt) 3: two folders (to/a/file.txt) and so on.
   * @return the requested part of the file name up to a certain depth
   * @throws HopFileException
   */
  private String determineZipfilenameForDepth(String filename, int depth) throws HopException {
    try {
      if (Utils.isEmpty(filename)) {
        return null;
      }
      if (depth == 0) {
        return filename;
      }
      FileObject fileObject = HopVfs.getFileObject(filename, getVariables());
      FileObject folder = fileObject.getParent();
      String baseName = fileObject.getName().getBaseName();
      if (depth == 1) {
        return baseName;
      }
      StringBuilder path = new StringBuilder(baseName);
      int d = 1;
      while (d < depth && folder != null) {
        path.insert(0, '/');
        path.insert(0, folder.getName().getBaseName());
        folder = folder.getParent();
        d++;
      }
      return path.toString();
    } catch (Exception e) {
      throw new HopException("Unable to get zip filename '" + filename + "' to depth " + depth, e);
    }
  }

  private File getFile(final String filename) {
    try {
      String uri = HopVfs.getFileObject(resolve(filename), getVariables()).getName().getPath();
      return new File(uri);
    } catch (HopFileException ex) {
      logError("Error in Fetching URI for File: " + filename, ex);
    }
    return new File(filename);
  }

  private boolean checkContainsFile(
      String realSourceDirectoryOrFile, FileObject[] filelist, boolean isDirectory)
      throws FileSystemException {
    boolean retval = false;
    for (int i = 0; i < filelist.length; i++) {
      FileObject file = filelist[i];
      if ((file.exists() && file.getType().equals(FileType.FILE))) {
        retval = true;
      }
    }
    return retval;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();

    // reset values
    String realZipfilename;
    String realWildcard = null;
    String realWildcardExclude = null;
    String realTargetdirectory;
    String realMovetodirectory = resolve(moveToDirectory);

    // Sanity check
    boolean sanityControlOK = true;

    if (afterZip == 2) {
      if (Utils.isEmpty(realMovetodirectory)) {
        sanityControlOK = false;
        logError(
            BaseMessages.getString(
                PKG, "ActionZipFile.AfterZip_No_DestinationFolder_Defined.Label"));
      } else {
        FileObject moveToDirectory = null;
        try {
          moveToDirectory = HopVfs.getFileObject(realMovetodirectory, getVariables());
          if (moveToDirectory.exists()) {
            if (moveToDirectory.getType() == FileType.FOLDER) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionZipFile.Log.MoveToFolderExist", realMovetodirectory));
              }
            } else {
              sanityControlOK = false;
              logError(
                  BaseMessages.getString(
                      PKG, "ActionZipFile.Log.MoveToFolderNotFolder", realMovetodirectory));
            }
          } else {
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG, "ActionZipFile.Log.MoveToFolderNotNotExist", realMovetodirectory));
            }
            if (createMoveToDirectory) {
              moveToDirectory.createFolder();
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionZipFile.Log.MoveToFolderCreaterd", realMovetodirectory));
              }
            } else {
              sanityControlOK = false;
              logError(
                  BaseMessages.getString(
                      PKG, "ActionZipFile.Log.MoveToFolderNotNotExist", realMovetodirectory));
            }
          }
        } catch (Exception e) {
          sanityControlOK = false;
          logError(
              BaseMessages.getString(
                  PKG, "ActionZipFile.ErrorGettingMoveToFolder.Label", realMovetodirectory),
              e);
        } finally {
          if (moveToDirectory != null) {
            realMovetodirectory = HopVfs.getFilename(moveToDirectory);
            try {
              moveToDirectory.close();
            } catch (Exception e) {
              logError("Error moving to directory", e);
              sanityControlOK = false;
            }
          }
        }
      }
    }

    if (!sanityControlOK) {
      return errorResult(result);
    }

    // arguments from previous

    if (fromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionZipFile.ArgFromPrevious.Found", (rows != null ? rows.size() : 0) + ""));
    }
    if (fromPrevious && rows != null) {
      try {
        for (int iteration = 0;
            iteration < rows.size() && !parentWorkflow.isStopped();
            iteration++) {
          // get arguments from previous action
          RowMetaAndData resultRow = rows.get(iteration);
          // get target directory
          realTargetdirectory = resultRow.getString(0, null);
          if (!Utils.isEmpty(realTargetdirectory)) {
            // get wildcard to include
            if (!Utils.isEmpty(resultRow.getString(1, null))) {
              realWildcard = resultRow.getString(1, null);
            }
            // get wildcard to exclude
            if (!Utils.isEmpty(resultRow.getString(2, null))) {
              realWildcardExclude = resultRow.getString(2, null);
            }

            // get destination zip file
            realZipfilename = resultRow.getString(3, null);
            if (!Utils.isEmpty(realZipfilename)) {
              if (!processRowFile(
                  parentWorkflow,
                  result,
                  realZipfilename,
                  realWildcard,
                  realWildcardExclude,
                  realTargetdirectory,
                  realMovetodirectory,
                  createParentFolder)) {
                return errorResult(result);
              }
            } else {
              logError("destination zip filename is empty! Ignoring row...");
            }
          } else {
            logError("Target directory is empty! Ignoring row...");
          }
        }
      } catch (Exception e) {
        logError("Error during process!", e);
        result.setResult(false);
        result.setNrErrors(1);
      }
    } else if (!fromPrevious) {
      if (!Utils.isEmpty(sourceDirectory)) {
        // get values from action
        realZipfilename =
            getFullFilename(resolve(zipFilename), addDate, addTime, specifyFormat, dateTimeFormat);
        realWildcard = resolve(wildCard);
        realWildcardExclude = resolve(excludeWildCard);
        realTargetdirectory = resolve(sourceDirectory);

        boolean success;
        try {
          success =
              processRowFile(
                  parentWorkflow,
                  result,
                  realZipfilename,
                  realWildcard,
                  realWildcardExclude,
                  realTargetdirectory,
                  realMovetodirectory,
                  createParentFolder);
        } catch (HopException e) {
          logError("Error zipping data", e);
          success = false;
        }
        if (success) {
          result.setResult(true);
        } else {
          errorResult(result);
        }
      } else {
        logError("Source folder/file is empty! Ignoring row...");
      }
    }

    // End
    return result;
  }

  private Result errorResult(Result result) {
    result.setNrErrors(1);
    result.setResult(false);
    return result;
  }

  public String getFullFilename(
      String filename,
      boolean addDate,
      boolean addTime,
      boolean specifyFormat,
      String datetimeFolder) {
    String retval;
    if (Utils.isEmpty(filename)) {
      return null;
    }

    // Replace possible environment variables...
    String realfilename = resolve(filename);
    int lenstring = realfilename.length();
    int lastindexOfDot = realfilename.lastIndexOf('.');
    if (lastindexOfDot == -1) {
      lastindexOfDot = lenstring;
    }

    retval = realfilename.substring(0, lastindexOfDot);

    final SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (specifyFormat && !Utils.isEmpty(datetimeFolder)) {
      daf.applyPattern(datetimeFolder);
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (addDate) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (addTime) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }
    retval += realfilename.substring(lastindexOfDot, lenstring);
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
    if (3 == ifZipFileExists) {
      // execute method fails if the file already exists; we should too
      FileDoesNotExistValidator.putFailIfExists(ctx1, true);
    }
    ActionValidatorUtils.andValidator().validate(this, "zipFilename", remarks, ctx1);

    if (2 == afterZip) {
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

  /** Helper class providing pattern restrictions for file names to be zipped */
  public static class ZipJobEntryPatternFileSelector implements FileSelector {

    private Pattern pattern;
    private Pattern patternExclude;

    public ZipJobEntryPatternFileSelector(Pattern pattern, Pattern patternExclude) {
      this.pattern = pattern;
      this.patternExclude = patternExclude;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo fileInfo) throws Exception {
      return true;
    }

    @Override
    public boolean includeFile(FileSelectInfo fileInfo) throws Exception {
      boolean include;

      // Only include files in the sub-folders...
      // When we include sub-folders we match the whole filename, not just the base-name
      //
      if (fileInfo.getFile().getType().equals(FileType.FILE)) {
        include = true;
        if (pattern != null) {
          String name = fileInfo.getFile().getName().getBaseName();
          include = pattern.matcher(name).matches();
        }
        if (include && patternExclude != null) {
          String name = fileInfo.getFile().getName().getBaseName();
          include = !patternExclude.matcher(name).matches();
        }
      } else {
        include = false;
      }
      return include;
    }
  }

  /**
   * Gets zipFilename
   *
   * @return value of zipFilename
   */
  public String getZipFilename() {
    return zipFilename;
  }

  /**
   * Sets zipFilename
   *
   * @param zipFilename value of zipFilename
   */
  public void setZipFilename(String zipFilename) {
    this.zipFilename = zipFilename;
  }

  /**
   * Gets compressionRate
   *
   * @return value of compressionRate
   */
  public int getCompressionRate() {
    return compressionRate;
  }

  /**
   * Sets compressionRate
   *
   * @param compressionRate value of compressionRate
   */
  public void setCompressionRate(int compressionRate) {
    this.compressionRate = compressionRate;
  }

  /**
   * Gets ifZipFileExists
   *
   * @return value of ifZipFileExists
   */
  public int getIfZipFileExists() {
    return ifZipFileExists;
  }

  /**
   * Sets ifZipFileExists
   *
   * @param ifZipFileExists value of ifZipFileExists
   */
  public void setIfZipFileExists(int ifZipFileExists) {
    this.ifZipFileExists = ifZipFileExists;
  }

  /**
   * Gets afterZip
   *
   * @return value of afterZip
   */
  public int getAfterZip() {
    return afterZip;
  }

  /**
   * Sets afterZip
   *
   * @param afterZip value of afterZip
   */
  public void setAfterZip(int afterZip) {
    this.afterZip = afterZip;
  }

  /**
   * Gets wildCard
   *
   * @return value of wildCard
   */
  public String getWildCard() {
    return wildCard;
  }

  /**
   * Sets wildCard
   *
   * @param wildCard value of wildCard
   */
  public void setWildCard(String wildCard) {
    this.wildCard = wildCard;
  }

  /**
   * Gets excludeWildCard
   *
   * @return value of excludeWildCard
   */
  public String getExcludeWildCard() {
    return excludeWildCard;
  }

  /**
   * Sets excludeWildCard
   *
   * @param excludeWildCard value of excludeWildCard
   */
  public void setExcludeWildCard(String excludeWildCard) {
    this.excludeWildCard = excludeWildCard;
  }

  /**
   * Gets sourceDirectory
   *
   * @return value of sourceDirectory
   */
  public String getSourceDirectory() {
    return sourceDirectory;
  }

  /**
   * Sets sourceDirectory
   *
   * @param sourceDirectory value of sourceDirectory
   */
  public void setSourceDirectory(String sourceDirectory) {
    this.sourceDirectory = sourceDirectory;
  }

  /**
   * Gets movetoDirectory
   *
   * @return value of moveToDirectory
   */
  public String getMoveToDirectory() {
    return moveToDirectory;
  }

  /**
   * Sets moveToDirectory
   *
   * @param moveToDirectory value of moveToDirectory
   */
  public void setMoveToDirectory(String moveToDirectory) {
    this.moveToDirectory = moveToDirectory;
  }

  /**
   * Gets addFileToResult
   *
   * @return value of addFileToResult
   */
  public boolean isAddFileToResult() {
    return addFileToResult;
  }

  /**
   * Sets addFileToResult
   *
   * @param addFileToResult value of addFileToResult
   */
  public void setAddFileToResult(boolean addFileToResult) {
    this.addFileToResult = addFileToResult;
  }

  /**
   * Gets isFromPrevious
   *
   * @return value of isFromPrevious
   */
  public boolean isFromPrevious() {
    return fromPrevious;
  }

  /**
   * Sets isFromPrevious
   *
   * @param fromPrevious value of isFromPrevious
   */
  public void setFromPrevious(boolean fromPrevious) {
    this.fromPrevious = fromPrevious;
  }

  /**
   * Gets createParentFolder
   *
   * @return value of createParentFolder
   */
  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  /**
   * Sets createParentFolder
   *
   * @param createParentFolder value of createParentFolder
   */
  public void setCreateParentFolder(boolean createParentFolder) {
    this.createParentFolder = createParentFolder;
  }

  /**
   * Gets addDate
   *
   * @return value of addDate
   */
  public boolean isAddDate() {
    return addDate;
  }

  /**
   * Sets addDate
   *
   * @param addDate value of addDate
   */
  public void setAddDate(boolean addDate) {
    this.addDate = addDate;
  }

  /**
   * Gets addTime
   *
   * @return value of addTime
   */
  public boolean isAddTime() {
    return addTime;
  }

  /**
   * Sets addTime
   *
   * @param addTime value of addTime
   */
  public void setAddTime(boolean addTime) {
    this.addTime = addTime;
  }

  /**
   * Gets specifyFormat
   *
   * @return value of specifyFormat
   */
  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  /**
   * Sets specifyFormat
   *
   * @param specifyFormat value of specifyFormat
   */
  public void setSpecifyFormat(boolean specifyFormat) {
    this.specifyFormat = specifyFormat;
  }

  /**
   * Gets dateTimeFormat
   *
   * @return value of dateTimeFormat
   */
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  /**
   * Sets dateTimeFormat
   *
   * @param dateTimeFormat value of dateTimeFormat
   */
  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  /**
   * Gets createMoveToDirectory
   *
   * @return value of createMoveToDirectory
   */
  public boolean isCreateMoveToDirectory() {
    return createMoveToDirectory;
  }

  /**
   * Sets createMoveToDirectory
   *
   * @param createMoveToDirectory value of createMoveToDirectory
   */
  public void setCreateMoveToDirectory(boolean createMoveToDirectory) {
    this.createMoveToDirectory = createMoveToDirectory;
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

  /**
   * Gets storedSourcePathDepth
   *
   * @return value of storedSourcePathDepth
   */
  public String getStoredSourcePathDepth() {
    return storedSourcePathDepth;
  }

  /**
   * Sets storedSourcePathDepth
   *
   * @param storedSourcePathDepth value of storedSourcePathDepth
   */
  public void setStoredSourcePathDepth(String storedSourcePathDepth) {
    this.storedSourcePathDepth = storedSourcePathDepth;
  }
}

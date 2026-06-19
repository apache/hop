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

package org.apache.hop.workflow.actions.copyfiles;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.NameScope;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.xml.ILegacyXml;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

/** This defines a 'copy files' action. */
@Getter
@Setter
@Action(
    id = "COPY_FILES",
    name = "i18n::ActionCopyFiles.Name",
    description = "i18n::ActionCopyFiles.Description",
    image = "CopyFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionCopyFiles.keyword",
    documentationUrl = "/workflow/actions/copyfiles.html")
public class ActionCopyFiles extends ActionBase implements ILegacyXml {
  private static final Class<?> PKG = ActionCopyFiles.class;

  /** Legacy wizard prefix on source paths (stripped on load, no longer written). */
  public static final String DEST_URL = "EMPTY_DEST_URL-";

  public static final String SOURCE_URL = "EMPTY_SOURCE_URL-";

  private static final String CONST_SPACE_SHORT = "      ";
  private static final String CONST_FILE_COPIED = "ActionCopyFiles.Log.FileCopied";
  private static final String CONST_COPY_PROCESS = "ActionCopyFiles.Error.Exception.CopyProcess";
  private static final String CONST_FILE_EXISTS = "ActionCopyFiles.Log.FileExists";
  private static final String CONST_FILE_EXISTS_SKIP_COPY =
      "ActionCopyFiles.Log.FileExistsSkippingCopy";
  private static final String CONST_FOLDER_EXISTS = "ActionCopyFiles.Log.FolderExists";
  private static final String CONST_FOLDER_EXISTS_SKIP_COPY =
      "ActionCopyFiles.Log.FolderExistsSkippingCopy";
  private static final String CONST_FILE_OVERWRITE = "ActionCopyFiles.Log.FileOverwrite";

  /** Destination parent folder exists or was created successfully. */
  private static final int DESTINATION_FOLDER_READY = 1;

  /** Destination parent folder is missing and "Create destination folder" is off. */
  private static final int DESTINATION_FOLDER_NOT_AVAILABLE = 0;

  /** Creating the destination folder failed (already logged). */
  private static final int DESTINATION_FOLDER_FAILED = -1;

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private HashSet<String> listFilesRemove = new HashSet<>();

  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private HashSet<String> listAddResult = new HashSet<>();

  @HopMetadataProperty(key = "copy_empty_folders")
  private boolean copyEmptyFolders = true;

  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @HopMetadataProperty(key = "overwrite_files")
  private boolean overwriteFiles;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty(key = "remove_source_files")
  private boolean removeSourceFiles;

  @HopMetadataProperty(key = "add_result_filesname")
  private boolean addResultFilenames;

  @HopMetadataProperty(key = "destination_is_a_file")
  private boolean destinationIsAFile;

  @HopMetadataProperty(key = "create_destination_folder")
  private boolean createDestinationFolder;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<CopyFilesItem> fileRows = new ArrayList<>();

  public ActionCopyFiles(String n) {
    super(n, "");
  }

  public ActionCopyFiles() {
    this("");
  }

  public ActionCopyFiles(ActionCopyFiles other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.copyEmptyFolders = other.copyEmptyFolders;
    this.argFromPrevious = other.argFromPrevious;
    this.overwriteFiles = other.overwriteFiles;
    this.includeSubFolders = other.includeSubFolders;
    this.removeSourceFiles = other.removeSourceFiles;
    this.addResultFilenames = other.addResultFilenames;
    this.destinationIsAFile = other.destinationIsAFile;
    this.createDestinationFolder = other.createDestinationFolder;
    this.fileRows = new ArrayList<>();
    if (other.fileRows != null) {
      for (CopyFilesItem row : other.fileRows) {
        this.fileRows.add(
            new CopyFilesItem(
                row.getSourceFileFolder(), row.getDestinationFileFolder(), row.getWildcard()));
      }
    }
  }

  @Override
  public Object clone() {
    ActionCopyFiles copy = (ActionCopyFiles) super.clone();
    copy.listFilesRemove = new HashSet<>();
    copy.listAddResult = new HashSet<>();
    copy.fileRows = new ArrayList<>();
    if (fileRows != null) {
      for (CopyFilesItem row : fileRows) {
        copy.fileRows.add(
            new CopyFilesItem(
                row.getSourceFileFolder(), row.getDestinationFileFolder(), row.getWildcard()));
      }
    }
    return copy;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public void convertLegacyXml(Node node) throws HopException {
    normalizeLegacyVfsPaths();
  }

  /**
   * Strips legacy per-row URL prefixes ({@link #SOURCE_URL}{i}-, {@link #DEST_URL}{i}-) left over
   * from older Hop GUI storage. New saves write plain VFS paths only.
   */
  void normalizeLegacyVfsPaths() {
    if (fileRows == null) {
      fileRows = new ArrayList<>();
      return;
    }
    List<CopyFilesItem> normalized = new ArrayList<>(fileRows.size());
    for (int i = 0; i < fileRows.size(); i++) {
      CopyFilesItem row = fileRows.get(i);
      normalized.add(
          new CopyFilesItem(
              stripLegacyRowPrefix(row.getSourceFileFolder(), SOURCE_URL, i),
              stripLegacyRowPrefix(row.getDestinationFileFolder(), DEST_URL, i),
              row.getWildcard()));
    }
    fileRows = normalized;
  }

  private static String stripLegacyRowPrefix(String path, String prefix, int index) {
    if (path == null) {
      return null;
    }
    return path.replace(prefix + index + "-", "");
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;

    List<RowMetaAndData> rows = result.getRows();

    int nbrFail = 0;

    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ActionCopyFiles.Log.Starting"));
    }

    result.setResult(false);
    result.setNrErrors(1);

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionCopyFiles.Log.ArgFromPrevious.Found",
              (rows != null ? rows.size() : 0) + ""));
    }

    if (argFromPrevious && rows != null) {
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        RowMetaAndData resultRow = rows.get(iteration);

        String vSourceFileFolderPrevious = resultRow.getString(0, null);
        String vDestinationFileFolderPrevious = resultRow.getString(1, null);
        String vWildcardPrevious = resultRow.getString(2, null);

        if (!Utils.isEmpty(vSourceFileFolderPrevious)
            && !Utils.isEmpty(vDestinationFileFolderPrevious)) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyFiles.Log.ProcessingRow",
                    HopVfs.getFriendlyURI(resolve(vSourceFileFolderPrevious), getVariables()),
                    HopVfs.getFriendlyURI(resolve(vDestinationFileFolderPrevious), getVariables()),
                    resolve(vWildcardPrevious)));
          }

          if (!processFileFolder(
              vSourceFileFolderPrevious,
              vDestinationFileFolderPrevious,
              vWildcardPrevious,
              parentWorkflow,
              result)) {
            nbrFail++;
          }
        } else {
          if (isDetailed()) {
            String srcHint = "";
            String destHint = "";
            String wildHint = "";
            if (fileRows != null && iteration < fileRows.size()) {
              CopyFilesItem item = fileRows.get(iteration);
              srcHint =
                  item.getSourceFileFolder() != null
                      ? HopVfs.getFriendlyURI(resolve(item.getSourceFileFolder()), getVariables())
                      : "";
              destHint =
                  item.getDestinationFileFolder() != null
                      ? HopVfs.getFriendlyURI(
                          resolve(item.getDestinationFileFolder()), getVariables())
                      : "";
              wildHint = item.getWildcard() != null ? item.getWildcard() : "";
            }
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCopyFiles.Log.IgnoringRow", srcHint, destHint, wildHint));
          }
        }
      }
    } else if (fileRows != null) {
      for (int i = 0; i < fileRows.size() && !parentWorkflow.isStopped(); i++) {
        CopyFilesItem row = fileRows.get(i);
        String src = row.getSourceFileFolder();
        String dst = row.getDestinationFileFolder();
        String wild = row.getWildcard();
        if (!Utils.isEmpty(src) && !Utils.isEmpty(dst)) {
          if (isBasic()) {
            logBasic(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyFiles.Log.ProcessingRow",
                    HopVfs.getFriendlyURI(resolve(src), getVariables()),
                    HopVfs.getFriendlyURI(resolve(dst), getVariables()),
                    resolve(wild)));
          }

          if (!processFileFolder(src, dst, wild, parentWorkflow, result)) {
            nbrFail++;
          }
        } else {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyFiles.Log.IgnoringRow",
                    HopVfs.getFriendlyURI(resolve(Const.NVL(src, "")), getVariables()),
                    HopVfs.getFriendlyURI(resolve(Const.NVL(dst, "")), getVariables()),
                    Const.NVL(wild, "")));
          }
        }
      }
    }

    if (nbrFail == 0) {
      result.setResult(true);
      result.setNrErrors(0);
    } else {
      result.setNrErrors(nbrFail);
    }

    if (isBasic()) {
      if (nbrFail == 0) {
        logBasic(BaseMessages.getString(PKG, "ActionCopyFiles.Log.FinishedSuccess"));
      } else {
        logBasic(
            BaseMessages.getString(
                PKG, "ActionCopyFiles.Log.FinishedWithErrors", Integer.toString(nbrFail)));
      }
    }

    return result;
  }

  boolean processFileFolder(
      String sourceFileFolderName,
      String destinationFileFolderName,
      String wildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {
    boolean entrystatus = false;
    FileObject sourceFileFolder = null;
    FileObject destinationFileFolder = null;

    // Clear list files to remove after copy process
    // This list is also added to result files name
    listFilesRemove.clear();
    listAddResult.clear();

    // Get real source, destination file and wildcard
    String realSourceFileFolderName = resolve(sourceFileFolderName);
    String realDestinationFileFolderName = resolve(destinationFileFolderName);
    String realWildcard = resolve(wildcard);

    try {
      sourceFileFolder = HopVfs.getFileObject(realSourceFileFolderName, getVariables());
      destinationFileFolder = HopVfs.getFileObject(realDestinationFileFolderName, getVariables());

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "ActionCopyFiles.Log.Debug.ResolvedPaths",
                HopVfs.getFriendlyURI(realSourceFileFolderName, getVariables()),
                HopVfs.getFriendlyURI(realDestinationFileFolderName, getVariables()),
                Const.NVL(realWildcard, "")));
      }

      if (sourceFileFolder.exists()) {

        // Check if destination folder/parent folder exists !
        // If user wanted and if destination folder does not exist
        // Apache Hop will create it
        int destinationStatus = ensureDestinationFolder(destinationFileFolder);
        if (destinationStatus == DESTINATION_FOLDER_READY) {

          // Basic Tests
          if (sourceFileFolder.getType().equals(FileType.FOLDER) && destinationIsAFile) {
            // Source is a folder, destination is a file
            // WARNING !!! CAN NOT COPY FOLDER TO FILE !!!

            logError(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyFiles.Log.CanNotCopyFolderToFile",
                    HopVfs.getFriendlyURI(realSourceFileFolderName, getVariables()),
                    HopVfs.getFriendlyURI(realDestinationFileFolderName, getVariables())));

          } else {

            if (destinationFileFolder.getType().equals(FileType.FOLDER)
                && sourceFileFolder.getType().equals(FileType.FILE)) {
              // Source is a file, destination is a folder
              // Copy the file to the destination folder
              //
              FileObject destChild =
                  destinationFileFolder.resolveFile(
                      sourceFileFolder.getName().getBaseName(), NameScope.CHILD);
              long copied = copyFileWithByteTracking(sourceFileFolder, destChild, result);
              emitCopyLineage(sourceFileFolder, destChild, copied);

              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        CONST_FILE_COPIED,
                        HopVfs.getFriendlyURI(sourceFileFolder),
                        HopVfs.getFriendlyURI(destinationFileFolder)));
              }

            } else if (sourceFileFolder.getType().equals(FileType.FILE) && destinationIsAFile) {
              // Source is a file, destination is a file (stream copy + byte metrics; no copyFrom)
              copyOneFileToFile(sourceFileFolder, destinationFileFolder, result);
            } else {
              // Both source and destination are folders
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG,
                        "ActionCopyFiles.Log.ScanningSourceFolder",
                        HopVfs.getFriendlyURI(sourceFileFolder)));
              }

              TextFileSelector textFileSelector =
                  new TextFileSelector(
                      sourceFileFolder,
                      destinationFileFolder,
                      realWildcard,
                      parentWorkflow,
                      result);
              try {
                destinationFileFolder.copyFrom(sourceFileFolder, textFileSelector);
              } finally {
                textFileSelector.shutdown();
              }
            }

            // Remove Files if needed
            if (removeSourceFiles && !listFilesRemove.isEmpty()) {
              String sourceFilefoldername = sourceFileFolder.toString();
              int trimPathLength = sourceFilefoldername.length() + 1;
              FileObject removeFile;

              for (Iterator<String> iter = listFilesRemove.iterator();
                  iter.hasNext() && !parentWorkflow.isStopped(); ) {
                String fileremoventry = iter.next();
                removeFile = null; // re=null each iteration
                // Try to get the file relative to the existing connection
                if (fileremoventry.startsWith(sourceFilefoldername)
                    && trimPathLength < fileremoventry.length()) {

                  removeFile = sourceFileFolder.getChild(fileremoventry.substring(trimPathLength));
                }

                // Unable to retrieve file through existing connection; Get the file through a new
                // VFS connection
                if (removeFile == null) {
                  removeFile = HopVfs.getFileObject(fileremoventry, getVariables());
                }

                // Remove ONLY Files
                if (removeFile.getType() == FileType.FILE) {
                  boolean deletefile = removeFile.delete();
                  if (!deletefile) {
                    logError(
                        CONST_SPACE_SHORT
                            + BaseMessages.getString(
                                PKG,
                                "ActionCopyFiles.Error.Exception.CanRemoveFileFolder",
                                HopVfs.getFriendlyURI(fileremoventry, getVariables())));
                  } else {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  "ActionCopyFiles.Log.FileFolderRemoved",
                                  HopVfs.getFriendlyURI(fileremoventry, getVariables())));
                    }
                  }
                }
              }
            }

            // Add files to result files name
            if (addResultFilenames && !listAddResult.isEmpty()) {
              String destinationFilefoldername = destinationFileFolder.toString();
              int trimPathLength = destinationFilefoldername.length() + 1;
              FileObject addFile;

              for (String fileaddentry : listAddResult) {
                addFile = null; // re=null each iteration

                // Try to get the file relative to the existing connection
                if (fileaddentry.startsWith(destinationFilefoldername)
                    && trimPathLength < fileaddentry.length()) {
                  addFile = destinationFileFolder.getChild(fileaddentry.substring(trimPathLength));
                }

                // Unable to retrieve file through existing connection; Get the file through a new
                // VFS connection
                if (addFile == null) {
                  addFile = HopVfs.getFileObject(fileaddentry, getVariables());
                }

                // Add ONLY Files
                if (addFile.getType() == FileType.FILE) {
                  ResultFile resultFile =
                      new ResultFile(
                          ResultFile.FILE_TYPE_GENERAL,
                          addFile,
                          parentWorkflow.getWorkflowName(),
                          toString());
                  result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
                  if (isDetailed()) {
                    logDetailed(
                        CONST_SPACE_SHORT
                            + BaseMessages.getString(
                                PKG,
                                "ActionCopyFiles.Log.FileAddedToResultFilesName",
                                HopVfs.getFriendlyURI(fileaddentry, getVariables())));
                  }
                }
              }
            }

            entrystatus = true;
          }
        } else if (destinationStatus == DESTINATION_FOLDER_NOT_AVAILABLE) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionCopyFiles.Error.DestinationFolderNotFound",
                  HopVfs.getFriendlyURI(realDestinationFileFolderName, getVariables())));
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionCopyFiles.Error.SourceFileNotExists",
                HopVfs.getFriendlyURI(realSourceFileFolderName, getVariables())));
      }
    } catch (FileSystemException fse) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionCopyFiles.Error.Exception.CopyProcessFileSystemException",
              fse.getMessage()),
          fse);
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              CONST_COPY_PROCESS,
              HopVfs.getFriendlyURI(realSourceFileFolderName, getVariables()),
              HopVfs.getFriendlyURI(realDestinationFileFolderName, getVariables()),
              e.getMessage()),
          e);
    } finally {
      if (sourceFileFolder != null) {
        try {
          sourceFileFolder.close();
          sourceFileFolder = null;
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (destinationFileFolder != null) {
        try {
          destinationFileFolder.close();
          destinationFileFolder = null;
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }

    return entrystatus;
  }

  private static final int COPY_BUFFER_SIZE = 8192;

  /**
   * Copies file content through {@link CountingInputStream} / {@link CountingOutputStream} so
   * {@link Result#getBytesReadThisAction()} and {@link Result#getBytesWrittenThisAction()} match
   * bytes moved over the streams (when {@link Const#HOP_METRIC_DATA_VOLUME} is enabled).
   */
  private long copyFileWithByteTracking(FileObject source, FileObject destination, Result result)
      throws IOException {
    FileObject parent = destination.getParent();
    if (parent != null && !parent.exists()) {
      parent.createFolder();
    }
    long read;
    long written;
    try (InputStream rawIn = HopVfs.getInputStream(source);
        CountingInputStream in = new CountingInputStream(rawIn);
        OutputStream rawOut = HopVfs.getOutputStream(destination, false);
        CountingOutputStream out = new CountingOutputStream(rawOut)) {
      byte[] buffer = new byte[COPY_BUFFER_SIZE];
      int len;
      while ((len = in.read(buffer)) != -1) {
        if (len > 0) {
          out.write(buffer, 0, len);
        }
      }
      out.flush();
      read = in.getCount();
      written = out.getCount();
    }
    result.setBytesReadThisAction(result.getBytesReadThisAction() + read);
    result.setBytesWrittenThisAction(result.getBytesWrittenThisAction() + written);
    return read;
  }

  private void emitCopyLineage(FileObject source, FileObject destination, long bytesTransferred) {
    if (getParentWorkflow() == null || source == null || destination == null) {
      return;
    }
    LineageFileIoEmitter.emitWorkflowActionFileIo(
        getParentWorkflow(),
        this,
        FileIoOperation.COPY,
        source,
        destination,
        bytesTransferred > 0 ? bytesTransferred : null,
        true,
        null);
  }

  /**
   * File-to-file copy: same rules as the former one-to-one {@link FileSelector} (exists / overwrite
   * / logging), then stream copy for byte metrics. Does not use {@code copyFrom}.
   *
   * @return true if the file was copied, false if skipped or on error
   */
  private boolean copyOneFileToFile(FileObject sourceFile, FileObject destFile, Result result) {
    boolean shouldCopy = false;
    try {
      if (destFile.exists()) {
        if (isDetailed()) {
          logDetailed(
              CONST_SPACE_SHORT
                  + BaseMessages.getString(
                      PKG,
                      overwriteFiles ? CONST_FILE_EXISTS : CONST_FILE_EXISTS_SKIP_COPY,
                      HopVfs.getFriendlyURI(destFile)));
        }
        if (overwriteFiles) {
          if (isDetailed()) {
            logDetailed(
                CONST_SPACE_SHORT
                    + BaseMessages.getString(
                        PKG,
                        CONST_FILE_OVERWRITE,
                        HopVfs.getFriendlyURI(sourceFile),
                        HopVfs.getFriendlyURI(destFile)));
          }
          shouldCopy = true;
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              CONST_SPACE_SHORT
                  + BaseMessages.getString(
                      PKG,
                      CONST_FILE_COPIED,
                      HopVfs.getFriendlyURI(sourceFile),
                      HopVfs.getFriendlyURI(destFile)));
        }
        shouldCopy = true;
      }

      if (!shouldCopy) {
        return false;
      }

      long copied = copyFileWithByteTracking(sourceFile, destFile, result);
      emitCopyLineage(sourceFile, destFile, copied);

      if (removeSourceFiles) {
        listFilesRemove.add(sourceFile.toString());
      }
      if (addResultFilenames) {
        listAddResult.add(destFile.toString());
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              CONST_COPY_PROCESS,
              HopVfs.getFriendlyURI(sourceFile),
              HopVfs.getFriendlyURI(destFile),
              e.getMessage()));
      return false;
    }
  }

  /**
   * Ensures the destination folder (or the parent when destination is a file) exists or can be
   * created.
   *
   * @return {@link #DESTINATION_FOLDER_READY} if ready, {@link #DESTINATION_FOLDER_NOT_AVAILABLE}
   *     if missing and create is disabled, or {@link #DESTINATION_FOLDER_FAILED} on error (already
   *     logged)
   */
  private void createMissingDestinationParent(FileObject folder) throws FileSystemException {
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionCopyFiles.Log.FolderParentWillCreate", HopVfs.getFriendlyURI(folder)));
    }
    folder.createFolder();
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionCopyFiles.Log.FolderParentCreated", HopVfs.getFriendlyURI(folder)));
    }
  }

  private int ensureDestinationFolder(FileObject filefolder) {
    FileObject folder = null;
    try {
      folder = destinationIsAFile ? filefolder.getParent() : filefolder;

      if (!folder.exists()) {
        if (createDestinationFolder) {
          createMissingDestinationParent(folder);
        } else {
          return DESTINATION_FOLDER_NOT_AVAILABLE;
        }
      }
      return DESTINATION_FOLDER_READY;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionCopyFiles.Error.FolderParentCreateFailed",
              folder != null ? HopVfs.getFriendlyURI(folder) : HopVfs.getFriendlyURI(filefolder)),
          e);
      return DESTINATION_FOLDER_FAILED;
    } finally {
      if (folder != null) {
        try {
          folder.close();
          folder = null;
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;
    String destinationFolder = null;
    IWorkflowEngine<WorkflowMeta> parentjob;
    Pattern pattern;
    private int traverseCount;

    // Store connection to destination source for improved performance to remote hosts
    FileObject destinationFolderObject = null;

    private final Result copyResult;

    /**
     * @param selectedfile
     * @return True if the selectedfile matches the wildcard
     */
    private boolean getFileWildcard(String selectedfile) {
      boolean getIt = true;
      // First see if the file matches the regular expression!
      if (pattern != null) {
        Matcher matcher = pattern.matcher(selectedfile);
        getIt = matcher.matches();
      }
      return getIt;
    }

    public TextFileSelector(
        FileObject sourcefolderin,
        FileObject destinationfolderin,
        String filewildcard,
        IWorkflowEngine<WorkflowMeta> parentWorkflow,
        Result result) {

      this.copyResult = result;
      if (sourcefolderin != null) {
        sourceFolder = sourcefolderin.toString();
      }
      if (destinationfolderin != null) {
        destinationFolderObject = destinationfolderin;
        destinationFolder = destinationFolderObject.toString();
      }
      if (!Utils.isEmpty(filewildcard)) {
        fileWildcard = filewildcard;
        pattern = Pattern.compile(fileWildcard);
      }
      parentjob = parentWorkflow;
    }

    @Override
    public boolean includeFile(FileSelectInfo info) {
      boolean returncode = false;
      boolean streamCopied = false;
      FileObject filename = null;
      String addFileNameString = null;
      try {

        if (!info.getFile().toString().equals(sourceFolder) && !parentjob.isStopped()) {
          // Pass over the Base folder itself

          String shortFilename = info.getFile().getName().getBaseName();
          // Built destination filename
          if (destinationFolderObject == null) {
            // Resolve the destination folder
            destinationFolderObject = HopVfs.getFileObject(destinationFolder, getVariables());
          }

          String fullName = info.getFile().toString();
          String baseFolder = info.getBaseFolder().toString();
          String path = fullName.substring(fullName.indexOf(baseFolder) + baseFolder.length() + 1);
          filename = destinationFolderObject.resolveFile(path, NameScope.DESCENDENT);

          if (!info.getFile().getParent().equals(info.getBaseFolder())) {

            // Not in the Base Folder..Only if include sub folders
            if (includeSubFolders) {
              // Folders..only if include subfolders
              if (info.getFile().getType() == FileType.FOLDER) {
                if (includeSubFolders && copyEmptyFolders && Utils.isEmpty(fileWildcard)) {
                  if ((filename == null) || (!filename.exists())) {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  "ActionCopyFiles.Log.FolderCopied",
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                    }
                    returncode = true;
                  } else {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  overwriteFiles
                                      ? CONST_FOLDER_EXISTS
                                      : CONST_FOLDER_EXISTS_SKIP_COPY,
                                  HopVfs.getFriendlyURI(filename)));
                    }
                    if (overwriteFiles) {
                      if (isDetailed()) {
                        logDetailed(
                            CONST_SPACE_SHORT
                                + BaseMessages.getString(
                                    PKG,
                                    "ActionCopyFiles.Log.FolderOverwrite",
                                    HopVfs.getFriendlyURI(info.getFile()),
                                    HopVfs.getFriendlyURI(filename)));
                      }
                      returncode = true;
                    }
                  }
                }

              } else {
                if (getFileWildcard(shortFilename)) {
                  // Check if the file exists
                  if ((filename == null) || (!filename.exists())) {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  CONST_FILE_COPIED,
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                    }
                    returncode = true;
                  } else {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  overwriteFiles ? CONST_FILE_EXISTS : CONST_FILE_EXISTS_SKIP_COPY,
                                  HopVfs.getFriendlyURI(filename)));
                    }
                    if (overwriteFiles) {
                      if (isDetailed()) {
                        logDetailed(
                            CONST_SPACE_SHORT
                                + BaseMessages.getString(
                                    PKG,
                                    CONST_FILE_OVERWRITE,
                                    HopVfs.getFriendlyURI(info.getFile()),
                                    HopVfs.getFriendlyURI(filename)));
                      }

                      returncode = true;
                    }
                  }
                }
              }
            }
          } else {
            // In the Base Folder...
            // Folders..only if include subfolders
            if (info.getFile().getType() == FileType.FOLDER) {
              if (includeSubFolders && copyEmptyFolders && Utils.isEmpty(fileWildcard)) {
                if ((filename == null) || (!filename.exists())) {
                  if (isDetailed()) {
                    logDetailed(
                        CONST_SPACE_SHORT
                            + BaseMessages.getString(
                                PKG,
                                "ActionCopyFiles.Log.FolderCopied",
                                HopVfs.getFriendlyURI(info.getFile()),
                                filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                  }

                  returncode = true;
                } else {
                  if (isDetailed()) {
                    logDetailed(
                        CONST_SPACE_SHORT
                            + BaseMessages.getString(
                                PKG,
                                overwriteFiles
                                    ? CONST_FOLDER_EXISTS
                                    : CONST_FOLDER_EXISTS_SKIP_COPY,
                                HopVfs.getFriendlyURI(filename)));
                  }
                  if (overwriteFiles) {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  "ActionCopyFiles.Log.FolderOverwrite",
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  HopVfs.getFriendlyURI(filename)));
                    }

                    returncode = true;
                  }
                }
              }
            } else {
              // file...Check if exists
              filename =
                  HopVfs.getFileObject(
                      destinationFolder + Const.FILE_SEPARATOR + shortFilename, getVariables());

              if (getFileWildcard(shortFilename)) {
                if ((filename == null) || (!filename.exists())) {
                  if (isDetailed()) {
                    logDetailed(
                        CONST_SPACE_SHORT
                            + BaseMessages.getString(
                                PKG,
                                CONST_FILE_COPIED,
                                HopVfs.getFriendlyURI(info.getFile()),
                                filename != null ? HopVfs.getFriendlyURI(filename) : ""));
                  }
                  returncode = true;

                } else {
                  if (isDetailed()) {
                    logDetailed(
                        CONST_SPACE_SHORT
                            + BaseMessages.getString(
                                PKG,
                                overwriteFiles ? CONST_FILE_EXISTS : CONST_FILE_EXISTS_SKIP_COPY,
                                HopVfs.getFriendlyURI(filename)));
                  }

                  if (overwriteFiles) {
                    if (isDetailed()) {
                      logDetailed(
                          CONST_SPACE_SHORT
                              + BaseMessages.getString(
                                  PKG,
                                  CONST_FILE_OVERWRITE,
                                  HopVfs.getFriendlyURI(info.getFile()),
                                  HopVfs.getFriendlyURI(filename)));
                    }

                    returncode = true;
                  }
                }
              }
            }
          }
        }

        if (returncode && filename != null && info.getFile().getType() == FileType.FILE) {
          long copied = copyFileWithByteTracking(info.getFile(), filename, copyResult);
          ActionCopyFiles.this.emitCopyLineage(info.getFile(), filename, copied);
          streamCopied = true;
        }
      } catch (Exception e) {

        logError(
            BaseMessages.getString(
                PKG,
                CONST_COPY_PROCESS,
                HopVfs.getFriendlyURI(info.getFile()),
                filename != null ? HopVfs.getFriendlyURI(filename) : null,
                e.getMessage()));

        returncode = false;
      } finally {
        if (filename != null) {
          try {
            if (returncode && addResultFilenames) {
              addFileNameString = filename.toString();
            }
            filename.close();
            filename = null;
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
      if (returncode && removeSourceFiles) {
        // add this folder/file to remove files
        // This list will be fetched and all entries files
        // will be removed
        listFilesRemove.add(info.getFile().toString());
      }

      if (returncode && addResultFilenames) {
        // add this folder/file to result files name
        listAddResult.add(
            addFileNameString); // was a NPE before with the file_name=null above in the finally
      }

      // File rows: we already copied bytes above when streamCopied; true would make copyFrom copy
      // again. Folder rows: unchanged — still return returncode so VFS creates empty dirs.
      return streamCopied ? false : returncode;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return (traverseCount++ == 0 || includeSubFolders);
    }

    public void shutdown() {
      if (destinationFolderObject != null) {
        try {
          destinationFolderObject.close();

        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
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
                "fileRows",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    if (fileRows != null) {
      for (int i = 0; i < fileRows.size(); i++) {
        ActionValidatorUtils.andValidator()
            .validate(this, "fileRows[" + i + "].sourceFileFolder", remarks, ctx);
      }
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }
}

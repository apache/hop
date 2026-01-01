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

package org.apache.hop.workflow.actions.checkfilelocked;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** This defines a 'check files locked' action. */
@Action(
    id = "CHECK_FILES_LOCKED",
    name = "i18n::ActionCheckFilesLocked.Name",
    description = "i18n::ActionCheckFilesLocked.Description",
    image = "CheckFilesLocked.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionCheckFilesLocked.keyword",
    documentationUrl = "/workflow/actions/checkfilelocked.html")
public class ActionCheckFilesLocked extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCheckFilesLocked.class;

  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubfolders;

  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<CheckedFile> checkedFiles;

  public ActionCheckFilesLocked(String n) {
    super(n, "");
    argFromPrevious = false;
    checkedFiles = new ArrayList<>();

    includeSubfolders = false;
  }

  public ActionCheckFilesLocked() {
    this("");
  }

  @Override
  public ActionCheckFilesLocked clone() {
    return new ActionCheckFilesLocked(this);
  }

  public ActionCheckFilesLocked(ActionCheckFilesLocked a) {
    this();
    this.argFromPrevious = a.argFromPrevious;
    this.includeSubfolders = a.includeSubfolders;
    for (CheckedFile checkedFile : a.checkedFiles) {
      this.checkedFiles.add(new CheckedFile(checkedFile));
    }
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
  public Result execute(Result previousResult, int nr) {
    List<RowMetaAndData> rows = previousResult.getRows();
    boolean oneFileLocked = false;
    previousResult.setResult(true);

    try {
      if (argFromPrevious && isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionCheckFilesLocked.FoundPreviousRows",
                String.valueOf((rows != null ? rows.size() : 0))));
      }

      if (argFromPrevious && rows != null) {
        oneFileLocked = isOneArgumentsFileLocked(rows);
      } else if (!checkedFiles.isEmpty()) {
        oneFileLocked = isOneSpecifiedFileLocked();
      } else {
        if (isBasic()) {
          logBasic(
              "This action didn't execute any locking checks "
                  + "as there were no lines to check and no arguments provided.");
        }
      }

      if (oneFileLocked) {
        previousResult.setResult(false);
        previousResult.setNrErrors(1);
      }
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionCheckFilesLocked.ErrorRunningAction", e));
    }

    return previousResult;
  }

  private boolean isOneSpecifiedFileLocked() {
    boolean oneFileLocked = false;
    for (int i = 0; i < checkedFiles.size() && !parentWorkflow.isStopped() && !oneFileLocked; i++) {
      CheckedFile checkedFile = checkedFiles.get(i);
      // ok we can process this file/folder
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionCheckFilesLocked.ProcessingArg",
                checkedFile.getName(),
                checkedFile.getWildcard()));
      }

      oneFileLocked = processCheckedFilesLine(checkedFile.getName(), checkedFile.getWildcard());
    }
    return oneFileLocked;
  }

  private boolean isOneArgumentsFileLocked(List<RowMetaAndData> rows) throws HopValueException {
    boolean oneFileLocked = false;
    // Copy the input row to the (command line) arguments
    for (int iteration = 0;
        iteration < rows.size() && !parentWorkflow.isStopped() && !oneFileLocked;
        iteration++) {
      RowMetaAndData resultRow = rows.get(iteration);

      // Get values from previous result
      String fileFolderPrevious = resultRow.getString(0, "");
      String fileMasksPrevious = resultRow.getString(1, "");

      // ok we can process this file/folder
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionCheckFilesLocked.ProcessingRow",
                fileFolderPrevious,
                fileMasksPrevious));
      }

      oneFileLocked = processCheckedFilesLine(fileFolderPrevious, fileMasksPrevious);
    }
    return oneFileLocked;
  }

  private boolean processCheckedFilesLine(String name, String wildcard) {
    boolean locked = false;
    String realFileFolderName = resolve(name);
    String realWildcard = resolve(wildcard);

    try (FileObject fileFolder = HopVfs.getFileObject(realFileFolderName, getVariables())) {
      FileObject[] files = new FileObject[] {fileFolder};
      if (fileFolder.exists()) {
        // the file or folder exists
        if (fileFolder.getType() == FileType.FOLDER) {
          // It's a folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.ProcessingFolder", realFileFolderName));
          }
          // Retrieve all files
          files = fileFolder.findFiles(new TextFileSelector(fileFolder.toString(), realWildcard));

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.TotalFilesToCheck", String.valueOf(files.length)));
          }
        } else {
          // It's a file
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.ProcessingFile", realFileFolderName));
          }
        }
        // Check files locked
        locked = checkFilesLocked(files);
      } else {
        // We can not find thsi file
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionCheckFilesLocked.FileNotExist", realFileFolderName));
        }
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionCheckFilesLocked.CouldNotProcess", realFileFolderName, e.getMessage()));
    }
    return locked;
  }

  private boolean checkFilesLocked(FileObject[] files) throws HopException {
    boolean oneFileLocked = false;
    for (int i = 0; i < files.length && !oneFileLocked; i++) {
      FileObject file = files[i];
      String filename = HopVfs.getFilename(file);
      LockFile locked = new LockFile(filename, getVariables());
      if (locked.isLocked()) {
        oneFileLocked = true;
        logError(BaseMessages.getString(PKG, "ActionCheckFilesLocked.Log.FileLocked", filename));
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionCheckFilesLocked.Log.FileNotLocked", filename));
        }
      }
    }
    return oneFileLocked;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;

    public TextFileSelector(String sourcefolderin, String filewildcard) {

      if (!Utils.isEmpty(sourcefolderin)) {
        sourceFolder = sourcefolderin;
      }

      if (!Utils.isEmpty(filewildcard)) {
        fileWildcard = filewildcard;
      }
    }

    @Override
    public boolean includeFile(FileSelectInfo info) {
      boolean includeFile = false;
      try {
        // Skip the Base folder itself.
        //
        if (info.getFile().toString().equals(sourceFolder)) {
          return false;
        }
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionCheckFilesLocked.CheckingFile", info.getFile().toString()));
        }

        String shortFilename = info.getFile().getName().getBaseName();

        if (!info.getFile().getParent().equals(info.getBaseFolder())) {
          // Not in the Base Folder. Only if include sub folders
          //
          includeFile =
              (includeSubfolders
                  && (info.getFile().getType() == FileType.FILE)
                  && getFileWildcard(shortFilename, fileWildcard));
        } else {
          // In the Base Folder...
          //
          includeFile =
              (info.getFile().getType() == FileType.FILE)
                  && getFileWildcard(shortFilename, fileWildcard);
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "ActionCheckFilesLocked.Error.Exception.ProcessError"),
            BaseMessages.getString(
                PKG,
                "JobCheckFilesLocked.Error.Exception.Process",
                info.getFile().toString(),
                e.getMessage()));
      }

      return includeFile;
    }

    /**
     * @param selectedFile The selected file or folder name
     * @param wildcard The wildcard
     * @return True if the selected file matches the wildcard
     */
    private boolean getFileWildcard(String selectedFile, String wildcard) {
      boolean getIt = true;

      if (!Utils.isEmpty(wildcard)) {
        Pattern pattern = Pattern.compile(wildcard);
        // First see if the file matches the regular expression!
        Matcher matcher = pattern.matcher(selectedFile);
        getIt = matcher.matches();
      }

      return getIt;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return info.getDepth() == 0 || includeSubfolders;
    }
  }

  public void setIncludeSubfolders(boolean includeSubfolders) {
    this.includeSubfolders = includeSubfolders;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  /**
   * Gets argFromPrevious
   *
   * @return value of argFromPrevious
   */
  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  /**
   * Sets argFromPrevious
   *
   * @param argFromPrevious value of argFromPrevious
   */
  public void setArgFromPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  /**
   * Gets includeSubfolders
   *
   * @return value of includeSubfolders
   */
  public boolean isIncludeSubfolders() {
    return includeSubfolders;
  }

  /**
   * Gets checkedFiles
   *
   * @return value of checkedFiles
   */
  public List<CheckedFile> getCheckedFiles() {
    return checkedFiles;
  }

  /**
   * Sets checkedFiles
   *
   * @param checkedFiles value of checkedFiles
   */
  public void setCheckedFiles(List<CheckedFile> checkedFiles) {
    this.checkedFiles = checkedFiles;
  }

  public static class CheckedFile {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "filemask")
    private String wildcard;

    public CheckedFile() {}

    public CheckedFile(CheckedFile f) {
      this();
      this.name = f.name;
      this.wildcard = f.wildcard;
    }

    public CheckedFile(String name, String wildcard) {
      this.name = name;
      this.wildcard = wildcard;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets wildcard
     *
     * @return value of wildcard
     */
    public String getWildcard() {
      return wildcard;
    }

    /**
     * Sets wildcard
     *
     * @param wildcard value of wildcard
     */
    public void setWildcard(String wildcard) {
      this.wildcard = wildcard;
    }
  }
}

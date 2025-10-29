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

package org.apache.hop.workflow.actions.folderisempty;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/**
 * This defines a 'create folder' action. Its main use would be to create empty folder that can be
 * used to control the flow in ETL cycles.
 */
@Action(
    id = "FOLDER_IS_EMPTY",
    name = "i18n::ActionFolderIsEmpty.Name",
    description = "i18n::ActionFolderIsEmpty.Description",
    image = "FolderIsEmpty.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionFolderIsEmpty.keyword",
    documentationUrl = "/workflow/actions/folderisempty.html")
public class ActionFolderIsEmpty extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFolderIsEmpty.class;

  @HopMetadataProperty(key = "foldername")
  private String folderName;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty(key = "specify_wildcard")
  private boolean specifyWildcard;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  private int filescount;
  private int folderscount;
  private Pattern pattern;

  public ActionFolderIsEmpty(String n) {
    super(n, "");
    folderName = null;
    wildcard = null;
    includeSubFolders = false;
    specifyWildcard = false;
  }

  public ActionFolderIsEmpty() {
    this("");
  }

  public ActionFolderIsEmpty(ActionFolderIsEmpty meta) {
    super(meta.getName(), meta.getDescription(), meta.getPluginId());
    this.folderName = meta.folderName;
    this.includeSubFolders = meta.includeSubFolders;
    this.specifyWildcard = meta.specifyWildcard;
    this.wildcard = meta.wildcard;
  }

  @Override
  public Object clone() {
    return new ActionFolderIsEmpty(this);
  }

  public void setSpecifyWildcard(boolean specifyWildcard) {
    this.specifyWildcard = specifyWildcard;
  }

  public boolean isSpecifyWildcard() {
    return specifyWildcard;
  }

  public void setFolderName(String folderName) {
    this.folderName = folderName;
  }

  public String getFolderName() {
    return folderName;
  }

  public String getRealFolderName() {
    return resolve(getFolderName());
  }

  public String getWildcard() {
    return wildcard;
  }

  public String getRealWildcard() {
    return resolve(getWildcard());
  }

  public void setWildcard(String wildcard) {
    this.wildcard = wildcard;
  }

  public boolean isIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders(boolean includeSubfolders) {
    this.includeSubFolders = includeSubfolders;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(0);

    filescount = 0;
    folderscount = 0;
    pattern = null;

    if (!Utils.isEmpty(getWildcard())) {
      pattern = Pattern.compile(getRealWildcard());
    }

    if (folderName != null) {
      String realFoldername = getRealFolderName();
      FileObject folderObject = null;
      try {
        folderObject = HopVfs.getFileObject(realFoldername, getVariables());
        if (folderObject.exists()) {
          // Check if it's a folder
          if (folderObject.getType() == FileType.FOLDER) {
            // File provided is a folder, so we can process ...
            try {
              folderObject.findFiles(new TextFileSelector(folderObject.toString()));
            } catch (Exception ex) {
              if (!(ex.getCause() instanceof ExpectedException)) {
                throw ex;
              }
            }
            if (isBasic()) {
              logBasic("Total files", "We found : " + filescount + " file(s)");
            }
            if (filescount == 0) {
              result.setResult(true);
              result.setNrLinesInput(folderscount);
            }
          } else {
            // Not a folder, fail
            logError("[" + realFoldername + "] is not a folder, failing.");
            result.setNrErrors(1);
          }
        } else {
          // No Folder found
          if (isBasic()) {
            logBasic("we can not find [" + realFoldername + "] !");
          }
          result.setNrErrors(1);
        }
      } catch (Exception e) {
        logError("Error checking folder [" + realFoldername + "]", e);
        result.setResult(false);
        result.setNrErrors(1);
      } finally {
        if (folderObject != null) {
          try {
            folderObject.close();
            folderObject = null;
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
    } else {
      logError("No Foldername is defined.");
      result.setNrErrors(1);
    }

    return result;
  }

  private class ExpectedException extends Exception {
    private static final long serialVersionUID = -692662556327569162L;
  }

  private class TextFileSelector implements FileSelector {
    String rootFolder = null;

    public TextFileSelector(String rootfolder) {
      if (rootfolder != null) {
        rootFolder = rootfolder;
      }
    }

    @Override
    public boolean includeFile(FileSelectInfo info) throws ExpectedException {
      boolean returncode = false;
      FileObject filename = null;
      boolean rethrow = false;
      try {
        if (!info.getFile().toString().equals(rootFolder)) {
          // Pass over the Base folder itself
          if ((info.getFile().getType() == FileType.FILE)) {
            if (info.getFile().getParent().equals(info.getBaseFolder())) {
              // We are in the Base folder
              if ((isSpecifyWildcard() && GetFileWildcard(info.getFile().getName().getBaseName()))
                  || !isSpecifyWildcard()) {
                if (isDetailed()) {
                  logDetailed("We found file : " + info.getFile().toString());
                }
                filescount++;
              }
            } else {
              // We are not in the base Folder...ONLY if Use sub folders
              // We are in the Base folder
              if (isIncludeSubFolders()
                  && ((isSpecifyWildcard()
                          && GetFileWildcard(info.getFile().getName().getBaseName()))
                      || !isSpecifyWildcard())) {
                if (isDetailed()) {
                  logDetailed("We found file : " + info.getFile().toString());
                }
                filescount++;
              }
            }
          } else {
            folderscount++;
          }
        }
        if (filescount > 0) {
          rethrow = true;
          throw new ExpectedException();
        }
        return true;

      } catch (Exception e) {
        if (!rethrow) {
          logError(
              BaseMessages.getString(PKG, "ActionFolderIsEmpty.Error"),
              BaseMessages.getString(
                  PKG,
                  "ActionFolderIsEmpty.Error.Exception",
                  info.getFile().toString(),
                  e.getMessage()));
          returncode = false;
        } else {
          throw (ExpectedException) e;
        }
      } finally {
        if (filename != null) {
          try {
            filename.close();
            filename = null;
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
      return returncode;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return true;
    }
  }

  /**********************************************************
   *
   * @param selectedfile
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean GetFileWildcard(String selectedfile) {
    boolean getIt = true;

    // First see if the file matches the regular expression!
    if (pattern != null) {
      Matcher matcher = pattern.matcher(selectedfile);
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
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "filename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}

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

package org.apache.hop.workflow.actions.createfolder;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;

/**
 * This defines a 'create folder' action. Its main use would be to create empty folder that can be
 * used to control the flow in ETL cycles.
 */
@Action(
    id = "CREATE_FOLDER",
    name = "i18n::ActionCreateFolder.Name",
    description = "i18n::ActionCreateFolder.Description",
    image = "CreateFolder.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionCreateFolder.keyword",
    documentationUrl = "/workflow/actions/createfolder.html")
public class ActionCreateFolder extends ActionBase implements Cloneable, IAction {
  private static final String CONST_FOLDER = "Folder [";

  @HopMetadataProperty(key = "foldername")
  private String folderName;

  @HopMetadataProperty(key = "fail_of_folder_exists")
  private boolean failIfFolderExists;

  public ActionCreateFolder(String n) {
    super(n, "");
    folderName = null;
    failIfFolderExists = true;
  }

  public ActionCreateFolder() {
    this("");
  }

  public ActionCreateFolder(ActionCreateFolder f) {
    super(f.getName(), f.getDescription(), f.getPluginId());
    this.folderName = f.folderName;
    this.failIfFolderExists = f.failIfFolderExists;
  }

  @Override
  public ActionCreateFolder clone() {
    return new ActionCreateFolder(this);
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    if (StringUtils.isEmpty(folderName)) {
      logError("No folder name is defined.");
      return result;
    }

    String realFolderName = getRealFolderName();
    try (FileObject folderObject = HopVfs.getFileObject(realFolderName, getVariables())) {

      if (folderObject.exists()) {
        boolean isFolder = folderObject.getType() == FileType.FOLDER;

        // Check if it's a folder
        //
        if (isFailIfFolderExists()) {
          // Folder exists and fail flag is enabled.
          //
          result.setResult(false);
          if (isFolder) {
            logError(CONST_FOLDER + realFolderName + "] exists, failing.");
          } else {
            logError("File [" + realFolderName + "] exists, failing.");
          }
        } else {
          // Folder already exists: there is no reason to try and create it.
          //
          result.setResult(true);
          if (isDetailed()) {
            logDetailed(CONST_FOLDER + realFolderName + "] already exists, not recreating.");
          }
        }
        return result;
      }

      // No Folder yet, create an empty Folder.
      folderObject.createFolder();
      if (isDetailed()) {
        logDetailed(CONST_FOLDER + realFolderName + "] created!");
      }
      result.setResult(true);

    } catch (Exception e) {
      logError("Could not create Folder [" + realFolderName + "]", e);
      result.setResult(false);
      result.setNrErrors(1);
    }

    return result;
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

  public String getRealFolderName() {
    return resolve(getFolderName());
  }

  /**
   * Gets folderName
   *
   * @return value of folderName
   */
  public String getFolderName() {
    return folderName;
  }

  /**
   * Sets folderName
   *
   * @param folderName value of folderName
   */
  public void setFolderName(String folderName) {
    this.folderName = folderName;
  }

  /**
   * Gets failIfFolderExists
   *
   * @return value of failIfFolderExists
   */
  public boolean isFailIfFolderExists() {
    return failIfFolderExists;
  }

  /**
   * Sets failIfFolderExists
   *
   * @param failIfFolderExists value of failIfFolderExists
   */
  public void setFailIfFolderExists(boolean failIfFolderExists) {
    this.failIfFolderExists = failIfFolderExists;
  }
}

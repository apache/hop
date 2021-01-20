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

package org.apache.hop.workflow.actions.createfolder;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'create folder' action. Its main use would be to create empty folder that can be
 * used to control the flow in ETL cycles.
 *
 * @author Sven/Samatar
 * @since 18-10-2007
 */
@Action(
    id = "CREATE_FOLDER",
    name = "i18n::ActionCreateFolder.Name",
    description = "i18n::ActionCreateFolder.Description",
    image = "CreateFolder.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/createfolder.html")
public class ActionCreateFolder extends ActionBase implements Cloneable, IAction {
  private String folderName;
  private boolean failOfFolderExists;

  public ActionCreateFolder(String n) {
    super(n, "");
    folderName = null;
    failOfFolderExists = true;
  }

  public ActionCreateFolder() {
    this("");
  }

  public Object clone() {
    ActionCreateFolder je = (ActionCreateFolder) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(50);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("foldername", folderName));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("fail_of_folder_exists", failOfFolderExists));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      folderName = XmlHandler.getTagValue(entrynode, "foldername");
      failOfFolderExists =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "fail_of_folder_exists"));
    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'create folder' from XML node", xe);
    }
  }

  public void setFoldername(String folderName) {
    this.folderName = folderName;
  }

  public String getFoldername() {
    return folderName;
  }

  public String getRealFoldername() {
    return resolve(getFoldername());
  }

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    if (folderName != null) {
      String realFoldername = getRealFoldername();
      FileObject folderObject = null;
      try {
        folderObject = HopVfs.getFileObject(realFoldername);

        if (folderObject.exists()) {
          boolean isFolder = false;

          // Check if it's a folder
          if (folderObject.getType() == FileType.FOLDER) {
            isFolder = true;
          }

          if (isFailOfFolderExists()) {
            // Folder exists and fail flag is on.
            result.setResult(false);
            if (isFolder) {
              logError("Folder [" + realFoldername + "] exists, failing.");
            } else {
              logError("File [" + realFoldername + "] exists, failing.");
            }
          } else {
            // Folder already exists, no reason to try to create it
            result.setResult(true);
            if (log.isDetailed()) {
              logDetailed("Folder [" + realFoldername + "] already exists, not recreating.");
            }
          }

        } else {
          // No Folder yet, create an empty Folder.
          folderObject.createFolder();
          if (log.isDetailed()) {
            logDetailed("Folder [" + realFoldername + "] created!");
          }
          result.setResult(true);
        }
      } catch (Exception e) {
        logError("Could not create Folder [" + realFoldername + "]", e);
        result.setResult(false);
        result.setNrErrors(1);
      } finally {
        if (folderObject != null) {
          try {
            folderObject.close();
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
    } else {
      logError("No Foldername is defined.");
    }

    return result;
  }

  public boolean isEvaluation() {
    return true;
  }

  public boolean isFailOfFolderExists() {
    return failOfFolderExists;
  }

  public void setFailOfFolderExists(boolean failIfFolderExists) {
    this.failOfFolderExists = failIfFolderExists;
  }

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
}

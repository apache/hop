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

package org.apache.hop.workflow.actions.deletefile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.FileExistsValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'delete file' action. Its main use would be to delete trigger files, but it will
 * delete any file.
 *
 * @author Sven Boden
 * @since 10-02-2007
 */
@Action(
    id = "DELETE_FILE",
    name = "i18n::ActionDeleteFile.Name",
    description = "i18n::ActionDeleteFile.Description",
    image = "DeleteFile.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/deletefile.html")
public class ActionDeleteFile extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionDeleteFile.class; // For Translator

  private String filename;
  private boolean failIfFileNotExists;

  public ActionDeleteFile(String n) {
    super(n, "");
    filename = null;
    failIfFileNotExists = false;
  }

  public ActionDeleteFile() {
    this("");
  }

  public Object clone() {
    ActionDeleteFile je = (ActionDeleteFile) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(50);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("filename", filename));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("fail_if_file_not_exists", failIfFileNotExists));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      filename = XmlHandler.getTagValue(entrynode, "filename");
      failIfFileNotExists =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "fail_if_file_not_exists"));
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "ActionDeleteFile.Error_0001_Unable_To_Load_Job_From_Xml_Node"),
          xe);
    }
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public String getFilename() {
    return filename;
  }

  public String getRealFilename() {
    return resolve(getFilename());
  }

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    if (filename != null) {
      String realFilename = getRealFilename();

      FileObject fileObject = null;
      try {
        fileObject = HopVfs.getFileObject(realFilename);

        if (!fileObject.exists()) {
          if (isFailIfFileNotExists()) {
            // File doesn't exist and fail flag is on.
            result.setResult(false);
            logError(
                BaseMessages.getString(
                    PKG, "ActionDeleteFile.ERROR_0004_File_Does_Not_Exist", realFilename));
          } else {
            // File already deleted, no reason to try to delete it
            result.setResult(true);
            if (log.isBasic()) {
              logBasic(
                  BaseMessages.getString(
                      PKG, "ActionDeleteFile.File_Already_Deleted", realFilename));
            }
          }
        } else {
          boolean deleted = fileObject.delete();
          if (!deleted) {
            logError(
                BaseMessages.getString(
                    PKG, "ActionDeleteFile.ERROR_0005_Could_Not_Delete_File", realFilename));
            result.setResult(false);
            result.setNrErrors(1);
          }
          if (log.isBasic()) {
            logBasic(BaseMessages.getString(PKG, "ActionDeleteFile.File_Deleted", realFilename));
          }
          result.setResult(true);
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionDeleteFile.ERROR_0006_Exception_Deleting_File",
                realFilename,
                e.getMessage()),
            e);
        result.setResult(false);
        result.setNrErrors(1);
      } finally {
        if (fileObject != null) {
          try {
            fileObject.close();
          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }
    } else {
      logError(BaseMessages.getString(PKG, "ActionDeleteFile.ERROR_0007_No_Filename_Is_Defined"));
    }

    return result;
  }

  public boolean isFailIfFileNotExists() {
    return failIfFileNotExists;
  }

  public void setFailIfFileNotExists(boolean failIfFileExists) {
    this.failIfFileNotExists = failIfFileExists;
  }

  public boolean isEvaluation() {
    return true;
  }

  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(filename)) {
      String realFileName = resolve(filename);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realFileName, ResourceType.FILE));
      references.add(reference);
    }
    return references;
  }

  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());
    if (isFailIfFileNotExists()) {
      FileExistsValidator.putFailIfDoesNotExist(ctx, true);
    }
    ActionValidatorUtils.andValidator().validate(this, "filename", remarks, ctx);
  }
}

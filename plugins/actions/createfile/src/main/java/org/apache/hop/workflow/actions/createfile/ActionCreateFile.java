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

package org.apache.hop.workflow.actions.createfile;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
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

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'create file' action. Its main use would be to create empty trigger files that can
 * be used to control the flow in ETL cycles.
 *
 * @author Sven Boden
 * @since 28-01-2007
 */
@Action(
    id = "CREATE_FILE",
    name = "i18n::ActionCreateFile.Name",
    description = "i18n::ActionCreateFile.Description",
    image = "CreateFile.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/createfile.html")
public class ActionCreateFile extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCreateFile.class; // For Translator
  private String filename;

  private boolean failIfFileExists;
  private boolean addfilenameresult;

  public ActionCreateFile(String n) {
    super(n, "");
    filename = null;
    failIfFileExists = true;
    addfilenameresult = false;
  }

  public ActionCreateFile() {
    this("");
  }

  public Object clone() {
    ActionCreateFile je = (ActionCreateFile) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(50);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("filename", filename));
    retval.append("      ").append(XmlHandler.addTagValue("fail_if_file_exists", failIfFileExists));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("add_filename_result", addfilenameresult));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      filename = XmlHandler.getTagValue(entrynode, "filename");
      failIfFileExists =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "fail_if_file_exists"));
      addfilenameresult =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_filename_result"));

    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'create file' from XML node", xe);
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

  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    result.setResult(false);

    if (filename != null) {
      String realFilename = getRealFilename();
      FileObject fileObject = null;
      try {
        fileObject = HopVfs.getFileObject(realFilename);

        if (fileObject.exists()) {
          if (isFailIfFileExists()) {
            // File exists and fail flag is on.
            result.setResult(false);
            logError("File [" + realFilename + "] exists, failing.");
          } else {
            // File already exists, no reason to try to create it
            result.setResult(true);
            logBasic("File [" + realFilename + "] already exists, not recreating.");
          }
          // add filename to result filenames if needed
          if (isAddFilenameToResult()) {
            addFilenameToResult(realFilename, result, parentWorkflow);
          }
        } else {
          // No file yet, create an empty file.
          fileObject.createFile();
          logBasic("File [" + realFilename + "] created!");
          // add filename to result filenames if needed
          if (isAddFilenameToResult()) {
            addFilenameToResult(realFilename, result, parentWorkflow);
          }
          result.setResult(true);
        }
      } catch (IOException e) {
        logError("Could not create file [" + realFilename + "], exception: " + e.getMessage());
        result.setResult(false);
        result.setNrErrors(1);
      } finally {
        if (fileObject != null) {
          try {
            fileObject.close();
            fileObject = null;
          } catch (IOException ex) {
            // Ignore
          }
        }
      }
    } else {
      logError("No filename is defined.");
    }

    return result;
  }

  private void addFilenameToResult(
      String targetFilename, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow)
      throws HopException {
    FileObject targetFile = null;
    try {
      targetFile = HopVfs.getFileObject(targetFilename);

      // Add to the result files...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              targetFile,
              parentWorkflow.getWorkflowName(),
              toString());
      resultFile.setComment("");
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

      if (log.isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionCreateFile.FileAddedToResult", targetFilename));
      }
    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      try {
        if (targetFile != null) {
          targetFile.close();
        }
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }

  public boolean isEvaluation() {
    return true;
  }

  public boolean isFailIfFileExists() {
    return failIfFileExists;
  }

  public void setFailIfFileExists(boolean failIfFileExists) {
    this.failIfFileExists = failIfFileExists;
  }

  public boolean isAddFilenameToResult() {
    return addfilenameresult;
  }

  public void setAddFilenameToResult(boolean addfilenameresult) {
    this.addfilenameresult = addfilenameresult;
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

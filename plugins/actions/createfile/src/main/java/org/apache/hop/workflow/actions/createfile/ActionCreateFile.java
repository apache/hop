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

package org.apache.hop.workflow.actions.createfile;

import java.io.IOException;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * This defines a 'create file' action. Its main use would be to create empty trigger files that can
 * be used to control the flow in ETL cycles.
 */
@Setter
@Getter
@Action(
    id = "CREATE_FILE",
    name = "i18n::ActionCreateFile.Name",
    description = "i18n::ActionCreateFile.Description",
    image = "CreateFile.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionCreateFile.Keyword",
    documentationUrl = "/workflow/actions/createfile.html")
public class ActionCreateFile extends ActionBase implements Cloneable {
  private static final Class<?> PKG = ActionCreateFile.class;

  private static final String CONST_FILE = "File [";
  private static final String CONST_FILENAME = "filename";

  @HopMetadataProperty(key = "filename")
  private String filename;

  @HopMetadataProperty(key = "fail_if_file_exists")
  private boolean failIfFileExists;

  @HopMetadataProperty(key = "add_filename_result")
  private boolean addFilenameToResult;

  public ActionCreateFile(String n) {
    super(n, "");
    filename = null;
    failIfFileExists = true;
    addFilenameToResult = false;
  }

  public ActionCreateFile() {
    this("");
  }

  @Override
  public String getRealFilename() {
    return resolve(getFilename());
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setResult(false);

    if (filename != null) {
      String realFilename = getRealFilename();
      try (FileObject fileObject = HopVfs.getFileObject(realFilename, getVariables())) {

        if (fileObject.exists()) {
          if (isFailIfFileExists()) {
            // File exists and fail flag is on.
            result.setResult(false);
            logError(CONST_FILE + realFilename + "] exists, failing.");
          } else {
            // File already exists, no reason to try to create it
            result.setResult(true);
            logBasic(CONST_FILE + realFilename + "] already exists, not recreating.");
          }
          // add filename to result filenames if needed
          if (isAddFilenameToResult()) {
            addFilenameToResult(realFilename, result, parentWorkflow);
          }
        } else {
          // No file yet, create an empty file.
          fileObject.createFile();
          logBasic(CONST_FILE + realFilename + "] created!");
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
      }
    } else {
      logError("No filename is defined.");
    }

    return result;
  }

  private void addFilenameToResult(
      String targetFilename, Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow)
      throws HopException {

    try (FileObject targetFile = HopVfs.getFileObject(targetFilename, getVariables())) {

      // Add to the result files...
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              targetFile,
              parentWorkflow.getWorkflowName(),
              toString());
      resultFile.setComment("");
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionCreateFile.FileAddedToResult", targetFilename));
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
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
    ActionValidatorUtils.andValidator().validate(this, CONST_FILENAME, remarks, ctx);
  }
}

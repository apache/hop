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

package org.apache.hop.workflow.actions.deleteresultfilenames;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
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

/**
 * This defines a 'deleteresultfilenames' action. Its main use would be to create empty folder that
 * can be used to control the flow in ETL cycles.
 */
@Action(
    id = "DELETE_RESULT_FILENAMES",
    name = "i18n::ActionDeleteResultFilenames.Name",
    description = "i18n::ActionDeleteResultFilenames.Description",
    image = "DeleteResultFilenames.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionDeleteResultFilenames.keyword",
    documentationUrl = "/workflow/actions/deleteresultfilenames.html")
@Getter
@Setter
public class ActionDeleteResultFilenames extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionDeleteResultFilenames.class;

  @HopMetadataProperty(key = "specify_wildcard")
  private boolean specifyWildcard;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "wildcardexclude")
  private String wildcardExclude;

  public ActionDeleteResultFilenames(String n) {
    super(n, "");
    wildcardExclude = null;
    wildcard = null;
    specifyWildcard = false;
  }

  public ActionDeleteResultFilenames() {
    this("");
  }

  public ActionDeleteResultFilenames(ActionDeleteResultFilenames a) {
    super(a);
    this.specifyWildcard = a.specifyWildcard;
    this.wildcard = a.wildcard;
    this.wildcardExclude = a.wildcardExclude;
  }

  @Override
  public Object clone() {
    return new ActionDeleteResultFilenames(this);
  }

  @Override
  public Result execute(Result result, int nr) {
    result.setResult(false);
    try {
      int size = result.getResultFiles().size();
      if (isBasic()) {
        logBasic(
            BaseMessages.getString(PKG, "ActionDeleteResultFilenames.log.FilesFound", "" + size));
      }
      if (!specifyWildcard) {
        // Remove all files
        result.getResultFiles().clear();
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDeleteResultFilenames.log.DeletedFiles", "" + size));
        }
      } else {
        List<ResultFile> resultFiles = result.getResultFilesList();
        if (!Utils.isEmpty(resultFiles)) {
          for (Iterator<ResultFile> it = resultFiles.iterator();
              it.hasNext() && !parentWorkflow.isStopped(); ) {
            ResultFile resultFile = it.next();
            FileObject file = resultFile.getFile();
            if (file != null
                && file.exists()
                && checkFileWildcard(file.getName().getBaseName(), resolve(wildcard), true)
                && !checkFileWildcard(
                    file.getName().getBaseName(), resolve(wildcardExclude), false)) {
              // Remove file from result files list
              result.getResultFiles().remove(resultFile.getFile().toString());

              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionDeleteResultFilenames.log.DeletedFile", file.toString()));
              }
            }
          }
        }
      }
      result.setResult(true);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionDeleteResultFilenames.Error", e.toString()));
    }
    return result;
  }

  /**
   * @param selectedFile The selected file
   * @param wildcard The wildcard
   * @return True if the selected file matches the wildcard
   */
  private boolean checkFileWildcard(String selectedFile, String wildcard, boolean include) {
    Pattern pattern;
    boolean getIt = include;

    if (!Utils.isEmpty(wildcard)) {
      pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
      Matcher matcher = pattern.matcher(selectedFile);
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
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx,
        ActionValidatorUtils.notNullValidator(),
        ActionValidatorUtils.fileDoesNotExistValidator());
    ActionValidatorUtils.andValidator().validate(this, "filename", remarks, ctx);
  }
}

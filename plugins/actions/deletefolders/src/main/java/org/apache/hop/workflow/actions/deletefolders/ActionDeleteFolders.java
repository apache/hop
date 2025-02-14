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

package org.apache.hop.workflow.actions.deletefolders;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines a 'delete folders' action. */
@Action(
    id = "DELETE_FOLDERS",
    name = "i18n::ActionDeleteFolders.Name",
    description = "i18n::ActionDeleteFolders.Description",
    image = "DeleteFolders.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionDeleteFolders.keyword",
    documentationUrl = "/workflow/actions/deletefolders.html")
@SuppressWarnings("java:S1104")
public class ActionDeleteFolders extends ActionBase {
  private static final Class<?> PKG = ActionDeleteFolders.class;

  @Getter
  public enum SuccessCondition implements IEnumHasCodeAndDescription {
    NO_ERRORS(
        "success_if_no_errors",
        BaseMessages.getString(PKG, "ActionDeleteFolders.SuccessWhenAllWorksFine.Label")),
    ERRORS_LESS(
        "success_if_errors_less",
        BaseMessages.getString(PKG, "ActionDeleteFolders.SuccessWhenErrorsLessThan.Label")),
    AT_LEAST_X_FOLDERS_DELETED(
        "success_when_at_least",
        BaseMessages.getString(PKG, "ActionDeleteFolders.SuccessWhenAtLeast.Label"));

    private final String code;
    private final String description;

    SuccessCondition(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SuccessCondition.class);
    }

    public static SuccessCondition lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          SuccessCondition.class, description, NO_ERRORS);
    }
  }

  @Getter
  @Setter
  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @Getter
  @Setter
  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<FileItem> fileItems;

  @Getter
  @Setter
  @HopMetadataProperty(key = "limit_folders")
  private String limitFolders;

  @Getter
  @Setter
  @HopMetadataProperty(key = "success_condition", storeWithCode = true)
  private SuccessCondition successCondition;

  int nrErrors = 0;
  int nrSuccess = 0;
  boolean successConditionBroken = false;
  int nrLimitFolders = 0;

  public ActionDeleteFolders() {
    this("");
  }

  public ActionDeleteFolders(String name) {
    super(name, "");
    argFromPrevious = false;
    fileItems = List.of();
    successCondition = SuccessCondition.NO_ERRORS;
    limitFolders = "10";
  }

  public ActionDeleteFolders(ActionDeleteFolders other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.argFromPrevious = other.argFromPrevious;
    this.limitFolders = other.limitFolders;
    this.successCondition = other.successCondition;
    this.fileItems = new ArrayList<>(other.fileItems);
  }

  @Override
  public Object clone() {
    return new ActionDeleteFolders(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();

    result.setNrErrors(1);
    result.setResult(false);

    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    nrLimitFolders = Const.toInt(resolve(getLimitFolders()), 10);

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionDeleteFolders.FoundPreviousRows",
              String.valueOf((rows != null ? rows.size() : 0))));
    }

    if (argFromPrevious && rows != null) {
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        if (successConditionBroken) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionDeleteFolders.Error.SuccessConditionBroken", "" + nrErrors));
          result.setNrErrors(nrErrors);
          result.setNrLinesDeleted(nrSuccess);
          return result;
        }
        RowMetaAndData resultRow = rows.get(iteration);
        String argsPrevious = resultRow.getString(0, null);
        if (!Utils.isEmpty(argsPrevious)) {
          if (deleteFolder(argsPrevious)) {
            updateSuccess();
          } else {
            updateErrors();
          }
        } else {
          // empty filename !
          logError(BaseMessages.getString(PKG, "ActionDeleteFolders.Error.EmptyLine"));
        }
      }
    } else if (fileItems != null) {
      for (FileItem item : fileItems) {
        if (parentWorkflow.isStopped()) break;
        if (successConditionBroken) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionDeleteFolders.Error.SuccessConditionBroken", "" + nrErrors));
          result.setNrErrors(nrErrors);
          result.setNrLinesDeleted(nrSuccess);
          return result;
        }
        String realFileName = resolve(item.getFileName());
        if (!Utils.isEmpty(realFileName)) {
          if (deleteFolder(realFileName)) {
            updateSuccess();
          } else {
            updateErrors();
          }
        } else {
          // empty filename !
          logError(BaseMessages.getString(PKG, "ActionDeleteFolders.Error.EmptyLine"));
        }
      }
    }

    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(PKG, "ActionDeleteFolders.Log.Info.NrError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionDeleteFolders.Log.Info.NrDeletedFolders", "" + nrSuccess));
      logDetailed("=======================================");
    }

    result.setNrErrors(nrErrors);
    result.setNrLinesDeleted(nrSuccess);
    if (getSuccessStatus()) {
      result.setResult(true);
    }

    return result;
  }

  private void updateErrors() {
    nrErrors++;
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    return (nrErrors > 0 && getSuccessCondition() == SuccessCondition.NO_ERRORS)
        || (nrErrors >= nrLimitFolders && getSuccessCondition() == SuccessCondition.ERRORS_LESS);
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private boolean getSuccessStatus() {
    return (nrErrors == 0 && getSuccessCondition() == SuccessCondition.NO_ERRORS)
        || (nrSuccess >= nrLimitFolders
            && getSuccessCondition() == SuccessCondition.AT_LEAST_X_FOLDERS_DELETED)
        || (nrErrors <= nrLimitFolders && getSuccessCondition() == SuccessCondition.ERRORS_LESS);
  }

  private boolean deleteFolder(String folderName) {
    boolean rcode = false;

    try (FileObject folder = HopVfs.getFileObject(folderName, getVariables())) {

      if (folder.exists()) {
        // the file or folder exists
        if (folder.getType() == FileType.FOLDER) {
          // It's a folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionDeleteFolders.ProcessingFolder", folderName));
          }
          // Delete Files
          int count = folder.delete(new TextFileSelector());

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionDeleteFolders.TotalDeleted", folderName, String.valueOf(count)));
          }
          rcode = true;
        } else {
          // Error...This file is not a folder!
          logError(BaseMessages.getString(PKG, "ActionDeleteFolders.Error.NotFolder"));
        }
      } else {
        // File already deleted, no reason to try to delete it
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(PKG, "ActionDeleteFolders.FolderAlreadyDeleted", folderName));
        }
        rcode = true;
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionDeleteFolders.CouldNotDelete", folderName, e.getMessage()),
          e);
    }

    return rcode;
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
    boolean res =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "fileItems",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    /* TODO: If we enable action check
    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());
    for (FileItem item : fileItems) {
      ActionValidatorUtils.andValidator().validate(this, "fileName", remarks, ctx);
    }*/
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);

    ResourceReference reference = null;
    for (FileItem item : fileItems) {
      String filename = resolve(item.getFileName());
      if (reference == null) {
        reference = new ResourceReference(this);
        references.add(reference);
      }
      reference.getEntries().add(new ResourceEntry(filename, ResourceType.FILE));
    }
    return references;
  }

  private static class TextFileSelector implements FileSelector {
    @Override
    public boolean includeFile(FileSelectInfo info) {
      return true;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return true;
    }
  }
}

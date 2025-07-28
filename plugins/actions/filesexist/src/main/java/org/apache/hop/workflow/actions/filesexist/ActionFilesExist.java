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

package org.apache.hop.workflow.actions.filesexist;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
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

/** This defines a Files exist action. */
@Setter
@Getter
@Action(
    id = "FILES_EXIST",
    name = "i18n::ActionFilesExist.Name",
    description = "i18n::ActionFilesExist.Description",
    image = "FilesExist.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionFilesExist.Keyword",
    documentationUrl = "/workflow/actions/filesexist.html")
public class ActionFilesExist extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFilesExist.class;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<FileItem> fileItems;

  public ActionFilesExist(String name) {
    super(name, "");
    fileItems = List.of();
  }

  public ActionFilesExist() {
    this("");
  }

  public ActionFilesExist(ActionFilesExist other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.fileItems = new ArrayList<>(other.fileItems);
  }

  @Override
  public Object clone() {
    return new ActionFilesExist(this);
  }

  @Override
  public Result execute(Result result, int nr) {
    result.setResult(false);
    result.setNrErrors(0);
    int missingFiles = 0;
    int nrErrors = 0;

    if (fileItems != null) {
      for (FileItem item : fileItems) {
        if (parentWorkflow.isStopped()) break;

        String path = resolve(item.getFileName());
        try (FileObject file = HopVfs.getFileObject(path, getVariables())) {

          if (file.exists()
              && file.isReadable()) { // TODO: is it needed to check file for readability?
            if (isDetailed()) {
              logDetailed(BaseMessages.getString(PKG, "ActionFilesExist.File_Exists", path));
            }
          } else {
            missingFiles++;
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionFilesExist.File_Does_Not_Exist", path));
            }
          }

        } catch (Exception e) {
          nrErrors++;
          missingFiles++;
          logError(
              BaseMessages.getString(PKG, "ActionFilesExist.ERROR_0004_IO_Exception", e.toString()),
              e);
        }
      }
    }

    result.setNrErrors(nrErrors);

    if (missingFiles == 0) {
      result.setResult(true);
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

    if (Utils.isEmpty(fileItems)) {
      String message = BaseMessages.getString(PKG, "ActionFilesExist.CheckResult.NothingToCheck");
      remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_WARNING, message, this));
    }
  }
}

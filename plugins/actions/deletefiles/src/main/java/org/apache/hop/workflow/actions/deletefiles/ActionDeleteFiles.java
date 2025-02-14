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

package org.apache.hop.workflow.actions.deletefiles;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'delete files' action. */
@Setter
@Getter
@Action(
    id = "DELETE_FILES",
    name = "i18n::ActionDeleteFiles.Name",
    description = "i18n::ActionDeleteFiles.Description",
    image = "DeleteFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionDeleteFiles.keyword",
    documentationUrl = "/workflow/actions/deletefiles.html")
public class ActionDeleteFiles extends ActionBase {

  private static final Class<?> PKG = ActionDeleteFiles.class;

  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubfolders;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<FileItem> fileItems;

  public ActionDeleteFiles(String workflowName) {
    super(workflowName, "");
    argFromPrevious = false;
    includeSubfolders = false;
    fileItems = List.of();
  }

  public ActionDeleteFiles() {
    this("");
  }

  public ActionDeleteFiles(ActionDeleteFiles other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.argFromPrevious = other.argFromPrevious;
    this.includeSubfolders = other.includeSubfolders;
    this.fileItems = new ArrayList<>(other.fileItems);
  }

  @Override
  public Object clone() {
    return new ActionDeleteFiles(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> resultRows = result.getRows();

    int numberOfErrFiles = 0;
    result.setResult(false);
    result.setNrErrors(1);

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionDeleteFiles.FoundPreviousRows",
              String.valueOf((resultRows != null ? resultRows.size() : 0))));
    }

    Multimap<String, String> pathToMaskMap = populateDataForWorkflowExecution(resultRows);

    for (Map.Entry<String, String> pathToMask : pathToMaskMap.entries()) {
      final String filePath = resolve(pathToMask.getKey());
      if (filePath.trim().isEmpty()) {
        // Relative paths are permitted, and providing an empty path means deleting all files inside
        // a root pdi-folder.
        // It is much more likely to be a mistake than a desirable action, so we don't delete
        // anything
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionDeleteFiles.NoPathProvided"));
        }
      } else {
        final String fileMask = resolve(pathToMask.getValue());

        if (parentWorkflow.isStopped()) {
          break;
        }

        if (!processFile(filePath, fileMask, parentWorkflow)) {
          numberOfErrFiles++;
        }
      }
    }

    if (numberOfErrFiles == 0) {
      result.setResult(true);
      result.setNrErrors(0);
    } else {
      result.setNrErrors(numberOfErrFiles);
      result.setResult(false);
    }

    return result;
  }

  /**
   * For workflow execution path to files and file masks should be provided. These values can be
   * obtained in two ways: 1. As an argument of a current action 2. As a table, that comes as a
   * result of execution previous workflow/pipeline.
   *
   * <p>As the logic of processing this data is the same for both of these cases, we first populate
   * this data (in this method) and then process it.
   *
   * <p>We are using guava multimap here, because if allows key duplication and there could be a
   * situation where two paths to one folder with different wildcards are provided.
   */
  private Multimap<String, String> populateDataForWorkflowExecution(
      List<RowMetaAndData> rowsFromPreviousMeta) throws HopValueException {
    Multimap<String, String> pathToMaskMap = ArrayListMultimap.create();
    if (argFromPrevious && rowsFromPreviousMeta != null) {
      for (RowMetaAndData resultRow : rowsFromPreviousMeta) {
        if (resultRow.size() < 2) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionDeleteFiles.Error.InvalidNumberOfRowsFromPrevMeta",
                  resultRow.size()));
          return pathToMaskMap;
        }
        String pathToFile = resultRow.getString(0, null);
        String fileMask = resultRow.getString(1, null);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionDeleteFiles.ProcessingRow", pathToFile, fileMask));
        }

        pathToMaskMap.put(pathToFile, fileMask);
      }
    } else if (fileItems != null && !fileItems.isEmpty()) {
      for (FileItem item : fileItems) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDeleteFiles.ProcessingArg", item.getFileName(), item.getFileMask()));
        }
        pathToMaskMap.put(item.getFileName(), item.getFileMask());
      }
    }

    return pathToMaskMap;
  }

  boolean processFile(String path, String wildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    boolean isDeleted = false;

    try (FileObject fileFolder = HopVfs.getFileObject(path, getVariables())) {

      if (fileFolder.exists()) {
        if (fileFolder.getType() == FileType.FOLDER) {

          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionDeleteFiles.ProcessingFolder", path));
          }

          int totalDeleted =
              fileFolder.delete(
                  new TextFileSelector(fileFolder.toString(), wildcard, parentWorkflow));

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionDeleteFiles.TotalDeleted", String.valueOf(totalDeleted)));
          }
          isDeleted = true;
        } else {

          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionDeleteFiles.ProcessingFile", path));
          }
          isDeleted = fileFolder.delete();
          if (!isDeleted) {
            logError(BaseMessages.getString(PKG, "ActionDeleteFiles.CouldNotDeleteFile", path));
          } else {
            if (isBasic()) {
              logBasic(BaseMessages.getString(PKG, "ActionDeleteFiles.FileDeleted", path));
            }
          }
        }
      } else {
        // File already deleted, no reason to try to delete it
        if (isBasic()) {
          logBasic(BaseMessages.getString(PKG, "ActionDeleteFiles.FileAlreadyDeleted", path));
        }
        isDeleted = true;
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionDeleteFiles.CouldNotProcess", path, e.getMessage()),
          e);
    }

    return isDeleted;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;
    IWorkflowEngine<WorkflowMeta> workflow;

    public TextFileSelector(
        String sourceFolder, String fileWildcard, IWorkflowEngine<WorkflowMeta> workflow) {

      if (!Utils.isEmpty(sourceFolder)) {
        this.sourceFolder = sourceFolder;
      }

      if (!Utils.isEmpty(fileWildcard)) {
        this.fileWildcard = fileWildcard;
      }
      this.workflow = workflow;
    }

    @Override
    public boolean includeFile(FileSelectInfo info) {
      boolean doReturnCode = false;
      try {

        if (!info.getFile().toString().equals(sourceFolder) && !workflow.isStopped()) {
          // Pass over the Base folder itself
          String shortFilename = info.getFile().getName().getBaseName();

          if (!info.getFile().getParent().equals(info.getBaseFolder())) {
            // Not in the Base Folder. Only if include sub folders
            if (includeSubfolders
                && (info.getFile().getType() == FileType.FILE)
                && getFileWildcard(shortFilename, fileWildcard)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionDeleteFiles.DeletingFile", info.getFile().toString()));
              }
              doReturnCode = true;
            }
          } else {
            // In the Base Folder...
            if ((info.getFile().getType() == FileType.FILE)
                && getFileWildcard(shortFilename, fileWildcard)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionDeleteFiles.DeletingFile", info.getFile().toString()));
              }
              doReturnCode = true;
            }
          }
        }
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG,
                "ActionDeleteFiles.Error.Exception.DeleteProcessError",
                info.getFile().toString(),
                e.getMessage()));

        doReturnCode = false;
      }

      return doReturnCode;
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return true;
    }
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param wildcard
   * @return True if the selected file matches the wildcard
   **********************************************************/
  private boolean getFileWildcard(String selectedfile, String wildcard) {
    boolean getIt = true;

    if (!Utils.isEmpty(wildcard)) {
      Pattern pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
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

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (FileItem item : fileItems) {
      ActionValidatorUtils.andValidator().validate(this, item.getFileName(), remarks, ctx);
    }
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
}

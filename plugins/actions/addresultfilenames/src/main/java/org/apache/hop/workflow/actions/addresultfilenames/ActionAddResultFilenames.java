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

package org.apache.hop.workflow.actions.addresultfilenames;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'add result filenames' action. */
@Action(
    id = "ADD_RESULT_FILENAMES",
    name = "i18n::ActionAddResultFilenames.Name",
    description = "i18n::ActionAddResultFilenames.Description",
    image = "AddResultFileNames.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionAddResultFilenames.keyword",
    documentationUrl = "/workflow/actions/addresultfilenames.html")
public class ActionAddResultFilenames extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionAddResultFilenames.class;

  @HopMetadataProperty(key = "arg_from_previous")
  private boolean argFromPrevious;

  @HopMetadataProperty(key = "delete_all_before")
  private boolean deleteAllBefore;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<Argument> arguments;

  public ActionAddResultFilenames(String n) {
    super(n, "");
    arguments = new ArrayList<>();
    argFromPrevious = false;
    deleteAllBefore = false;

    includeSubFolders = false;
  }

  public ActionAddResultFilenames() {
    this("");
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow;

    int nrErrFiles = 0;
    result.setResult(true);

    if (deleteAllBefore) {
      // clear result filenames
      int size = result.getResultFiles().size();
      if (isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ActionAddResultFilenames.log.FilesFound", "" + size));
      }

      result.getResultFiles().clear();
      if (this.isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionAddResultFilenames.log.DeletedFiles", "" + size));
      }
    }

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionAddResultFilenames.FoundPreviousRows",
              String.valueOf((rows != null ? rows.size() : 0))));
    }

    if (argFromPrevious && rows != null) { // Copy the input row to the (command line) arguments
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        resultRow = rows.get(iteration);

        // Get values from previous result
        String fileFolderPrevious = resultRow.getString(0, null);
        // The wildcard is optional: a row with only the file/folder takes all its files
        String fileMasksPrevious = resultRow.size() > 1 ? resultRow.getString(1, null) : null;

        // ok we can process this file/folder
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionAddResultFilenames.ProcessingRow",
                  fileFolderPrevious,
                  fileMasksPrevious));
        }

        if (!processFile(fileFolderPrevious, fileMasksPrevious, parentWorkflow, result)) {
          nrErrFiles++;
        }
      }
    } else if (arguments != null) {

      for (int i = 0; i < arguments.size() && !parentWorkflow.isStopped(); i++) {
        Argument argument = arguments.get(i);
        // ok we can process this file/folder
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionAddResultFilenames.ProcessingArg",
                  argument.getArgument(),
                  argument.getMask()));
        }
        if (!processFile(argument.getArgument(), argument.getMask(), parentWorkflow, result)) {
          nrErrFiles++;
        }
      }
    }

    if (nrErrFiles > 0) {
      result.setResult(false);
      result.setNrErrors(nrErrFiles);
    }

    return result;
  }

  private boolean processFile(
      String filename,
      String wildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result) {

    String realFileFolderName = resolve(filename);
    String realWildcard = resolve(wildcard);
    FileObject filefolder = null;

    try {
      filefolder = HopVfs.getFileObject(realFileFolderName, getVariables());
      if (!filefolder.exists()) {
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionAddResultFilenames.FileCanNotbeFound", realFileFolderName));
        }
        return false;
      }
      if (filefolder.getType() == FileType.FILE) {
        // Wildcard applies only to folders; a file entry is added as-is.
        addToResultFiles(filefolder.toString(), parentWorkflow, result);
      } else {
        // Compile once so an invalid mask fails the folder entry (issue #9264),
        // instead of being swallowed while selecting each file.
        addMatchingFilesFromFolder(
            filefolder, compileWildcard(realWildcard), parentWorkflow, result);
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionAddResultFilenames.CouldNotProcess", realFileFolderName, e.getMessage()),
          e);
      return false;
    } finally {
      closeQuietly(filefolder);
    }
  }

  private void addMatchingFilesFromFolder(
      FileObject folder,
      Pattern compiledWildcard,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result)
      throws Exception {
    FileObject[] list = folder.findFiles(new TextFileSelector(folder.toString(), compiledWildcard));
    for (int i = 0; i < list.length && !parentWorkflow.isStopped(); i++) {
      addToResultFiles(list[i].toString(), parentWorkflow, result);
    }
  }

  private void addToResultFiles(
      String filePath, IWorkflowEngine<WorkflowMeta> parentWorkflow, Result result)
      throws Exception {
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionAddResultFilenames.AddingFileToResult", filePath));
    }
    ResultFile resultFile =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL,
            HopVfs.getFileObject(filePath, getVariables()),
            parentWorkflow.getWorkflowName(),
            toString());
    result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
  }

  private static void closeQuietly(FileObject fileFolder) {
    if (fileFolder == null) {
      return;
    }
    try {
      fileFolder.close();
    } catch (IOException ex) {
      // Ignore
    }
  }

  /**
   * Compile the folder file-mask regex once. An invalid expression must fail that folder entry
   * instead of being swallowed while selecting files. Not used for file entries.
   */
  private static Pattern compileWildcard(String wildcard) {
    if (Utils.isEmpty(wildcard)) {
      return null;
    }
    return Pattern.compile(wildcard);
  }

  private class TextFileSelector implements FileSelector {
    private final Pattern fileWildcard;
    private final String sourceFolder;

    TextFileSelector(String sourceFolder, Pattern fileWildcard) {
      this.sourceFolder = sourceFolder;
      this.fileWildcard = fileWildcard;
    }

    @Override
    public boolean includeFile(FileSelectInfo info) {
      try {
        if (info.getFile().toString().equals(sourceFolder)) {
          return false;
        }
        if (info.getFile().getType() != FileType.FILE) {
          return false;
        }
        if (!isInScope(info)) {
          return false;
        }
        return matchesWildcard(info.getFile().getName().getBaseName());
      } catch (Exception e) {
        logError(
            "Error while finding files ... in ["
                + info.getFile().toString()
                + "]. Exception :"
                + e.getMessage());
        return false;
      }
    }

    private boolean isInScope(FileSelectInfo info) throws Exception {
      if (info.getFile().getParent().equals(info.getBaseFolder())) {
        return true;
      }
      return includeSubFolders;
    }

    private boolean matchesWildcard(String shortFilename) {
      if (fileWildcard == null) {
        return true;
      }
      Matcher matcher = fileWildcard.matcher(shortFilename);
      return matcher.matches();
    }

    @Override
    public boolean traverseDescendents(FileSelectInfo info) {
      return info.getDepth() == 0 || includeSubFolders;
    }
  }

  public void setIncludeSubFolders(boolean includeSubFolders) {
    this.includeSubFolders = includeSubFolders;
  }

  public void setArgumentsPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  public void setDeleteAllBefore(boolean deleteallbefore) {
    this.deleteAllBefore = deleteallbefore;
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
                "arguments",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < arguments.size(); i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  /**
   * Gets argFromPrevious
   *
   * @return value of argFromPrevious
   */
  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  /**
   * @param argFromPrevious The argFromPrevious to set
   */
  public void setArgFromPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  /**
   * Gets deleteAllBefore
   *
   * @return value of deleteAllBefore
   */
  public boolean isDeleteAllBefore() {
    return deleteAllBefore;
  }

  /**
   * Gets includeSubFolders
   *
   * @return value of includeSubFolders
   */
  public boolean isIncludeSubFolders() {
    return includeSubFolders;
  }

  /**
   * Gets arguments
   *
   * @return value of arguments
   */
  public List<Argument> getArguments() {
    return arguments;
  }

  /**
   * @param arguments The arguments to set
   */
  public void setArguments(List<Argument> arguments) {
    this.arguments = arguments;
  }
}

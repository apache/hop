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

package org.apache.hop.workflow.actions.deletefiles;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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
import org.apache.hop.core.exception.HopValueException;
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
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'delete files' action.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
@Action(
    id = "DELETE_FILES",
    name = "i18n::ActionDeleteFiles.Name",
    description = "i18n::ActionDeleteFiles.Description",
    image = "DeleteFiles.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/deletefiles.html")
public class ActionDeleteFiles extends ActionBase implements Cloneable, IAction {

  private static final Class<?> PKG = ActionDeleteFiles.class; // For Translator

  private boolean argFromPrevious;

  private boolean includeSubfolders;

  private String[] arguments;

  private String[] filemasks;

  public ActionDeleteFiles(String workflowName) {
    super(workflowName, "");
    argFromPrevious = false;
    arguments = null;

    includeSubfolders = false;
  }

  public ActionDeleteFiles() {
    this("");
  }

  public void allocate(int numberOfFields) {
    arguments = new String[numberOfFields];
    filemasks = new String[numberOfFields];
  }

  public Object clone() {
    ActionDeleteFiles action = (ActionDeleteFiles) super.clone();
    if (arguments != null) {
      int nrFields = arguments.length;
      action.allocate(nrFields);
      System.arraycopy(arguments, 0, action.arguments, 0, nrFields);
      System.arraycopy(filemasks, 0, action.filemasks, 0, nrFields);
    }
    return action;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("arg_from_previous", argFromPrevious));
    retval.append("      ").append(XmlHandler.addTagValue("include_subfolders", includeSubfolders));

    retval.append("      <fields>").append(Const.CR);
    if (arguments != null) {
      for (int i = 0; i < arguments.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval.append("          ").append(XmlHandler.addTagValue("name", arguments[i]));
        retval.append("          ").append(XmlHandler.addTagValue("filemask", filemasks[i]));
        retval.append("        </field>").append(Const.CR);
      }
    }
    retval.append("      </fields>").append(Const.CR);

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      argFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "arg_from_previous"));
      includeSubfolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "include_subfolders"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      int numberOfFields = XmlHandler.countNodes(fields, "field");
      allocate(numberOfFields);

      for (int i = 0; i < numberOfFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        arguments[i] = XmlHandler.getTagValue(fnode, "name");
        filemasks[i] = XmlHandler.getTagValue(fnode, "filemask");
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionDeleteFiles.UnableToLoadFromXml"), xe);
    }
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> resultRows = result.getRows();

    int numberOfErrFiles = 0;
    result.setResult(false);
    result.setNrErrors(1);

    if (argFromPrevious && log.isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionDeleteFiles.FoundPreviousRows",
              String.valueOf((resultRows != null ? resultRows.size() : 0))));
    }

    Multimap<String, String> pathToMaskMap = populateDataForJobExecution(resultRows);

    for (Map.Entry<String, String> pathToMask : pathToMaskMap.entries()) {
      final String filePath = resolve(pathToMask.getKey());
      if (filePath.trim().isEmpty()) {
        // Relative paths are permitted, and providing an empty path means deleting all files inside
        // a root pdi-folder.
        // It is much more likely to be a mistake than a desirable action, so we don't delete
        // anything (see PDI-15181)
        if (log.isDetailed()) {
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
   * <p>As the logic of processing this data is the same for both of this cases, we first populate
   * this data (in this method) and then process it.
   *
   * <p>We are using guava multimap here, because if allows key duplication and there could be a
   * situation where two paths to one folder with different wildcards are provided.
   */
  private Multimap<String, String> populateDataForJobExecution(
      List<RowMetaAndData> rowsFromPreviousMeta) throws HopValueException {
    Multimap<String, String> pathToMaskMap = ArrayListMultimap.create();
    if (argFromPrevious && rowsFromPreviousMeta != null) {
      for (RowMetaAndData resultRow : rowsFromPreviousMeta) {
        if (resultRow.size() < 2) {
          logError(
              BaseMessages.getString(
                  PKG, "JobDeleteFiles.Error.InvalidNumberOfRowsFromPrevMeta", resultRow.size()));
          return pathToMaskMap;
        }
        String pathToFile = resultRow.getString(0, null);
        String fileMask = resultRow.getString(1, null);

        if (log.isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionDeleteFiles.ProcessingRow", pathToFile, fileMask));
        }

        pathToMaskMap.put(pathToFile, fileMask);
      }
    } else if (arguments != null) {
      for (int i = 0; i < arguments.length; i++) {
        if (log.isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionDeleteFiles.ProcessingArg", arguments[i], filemasks[i]));
        }
        pathToMaskMap.put(arguments[i], filemasks[i]);
      }
    }

    return pathToMaskMap;
  }

  boolean processFile(String path, String wildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow) {
    boolean isDeleted = false;
    FileObject fileFolder = null;

    try {
      fileFolder = HopVfs.getFileObject(path);

      if (fileFolder.exists()) {
        if (fileFolder.getType() == FileType.FOLDER) {

          if (log.isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionDeleteFiles.ProcessingFolder", path));
          }

          int totalDeleted =
              fileFolder.delete(
                  new TextFileSelector(fileFolder.toString(), wildcard, parentWorkflow));

          if (log.isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionDeleteFiles.TotalDeleted", String.valueOf(totalDeleted)));
          }
          isDeleted = true;
        } else {

          if (log.isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionDeleteFiles.ProcessingFile", path));
          }
          isDeleted = fileFolder.delete();
          if (!isDeleted) {
            logError(BaseMessages.getString(PKG, "ActionDeleteFiles.CouldNotDeleteFile", path));
          } else {
            if (log.isBasic()) {
              logBasic(BaseMessages.getString(PKG, "ActionDeleteFiles.FileDeleted", path));
            }
          }
        }
      } else {
        // File already deleted, no reason to try to delete it
        if (log.isBasic()) {
          logBasic(BaseMessages.getString(PKG, "ActionDeleteFiles.FileAlreadyDeleted", path));
        }
        isDeleted = true;
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionDeleteFiles.CouldNotProcess", path, e.getMessage()),
          e);
    } finally {
      if (fileFolder != null) {
        try {
          fileFolder.close();
        } catch (IOException ex) {
          // Ignore
        }
      }
    }

    return isDeleted;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;
    IWorkflowEngine<WorkflowMeta> parentjob;

    public TextFileSelector(
        String sourcefolderin, String filewildcard, IWorkflowEngine<WorkflowMeta> parentWorkflow) {

      if (!Utils.isEmpty(sourcefolderin)) {
        sourceFolder = sourcefolderin;
      }

      if (!Utils.isEmpty(filewildcard)) {
        fileWildcard = filewildcard;
      }
      parentjob = parentWorkflow;
    }

    public boolean includeFile(FileSelectInfo info) {
      boolean doReturnCode = false;
      try {

        if (!info.getFile().toString().equals(sourceFolder) && !parentjob.isStopped()) {
          // Pass over the Base folder itself
          String shortFilename = info.getFile().getName().getBaseName();

          if (!info.getFile().getParent().equals(info.getBaseFolder())) {
            // Not in the Base Folder..Only if include sub folders
            if (includeSubfolders
                && (info.getFile().getType() == FileType.FILE)
                && getFileWildcard(shortFilename, fileWildcard)) {
              if (log.isDetailed()) {
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
              if (log.isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionDeleteFiles.DeletingFile", info.getFile().toString()));
              }
              doReturnCode = true;
            }
          }
        }
      } catch (Exception e) {
        log.logError(
            BaseMessages.getString(PKG, "JobDeleteFiles.Error.Exception.DeleteProcessError"),
            BaseMessages.getString(
                PKG,
                "JobDeleteFiles.Error.Exception.DeleteProcess",
                info.getFile().toString(),
                e.getMessage()));

        doReturnCode = false;
      }

      return doReturnCode;
    }

    public boolean traverseDescendents(FileSelectInfo info) {
      return true;
    }
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param wildcard
   * @return True if the selectedfile matches the wildcard
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

  public void setIncludeSubfolders(boolean includeSubfolders) {
    this.includeSubfolders = includeSubfolders;
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
    boolean isValid =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "arguments",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!isValid) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < arguments.length; i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (arguments != null) {
      ResourceReference reference = null;
      for (int i = 0; i < arguments.length; i++) {
        String filename = resolve(arguments[i]);
        if (reference == null) {
          reference = new ResourceReference(this);
          references.add(reference);
        }
        reference.getEntries().add(new ResourceEntry(filename, ResourceType.FILE));
      }
    }
    return references;
  }

  public void setArguments(String[] arguments) {
    this.arguments = arguments;
  }

  public void setFilemasks(String[] filemasks) {
    this.filemasks = filemasks;
  }

  public void setArgFromPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  public String[] getArguments() {
    return arguments;
  }

  public String[] getFilemasks() {
    return filemasks;
  }

  public boolean isIncludeSubfolders() {
    return includeSubfolders;
  }
}

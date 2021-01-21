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
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.List;

/**
 * This defines a 'delete folders' action.
 *
 * @author Samatar Hassan
 * @since 13-05-2008
 */
@Action(
    id = "DELETE_FOLDERS",
    name = "i18n::ActionDeleteFolders.Name",
    description = "i18n::ActionDeleteFolders.Description",
    image = "DeleteFolders.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/deletefolders.html")
public class ActionDeleteFolders extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionDeleteFolders.class; // For Translator

  public boolean argFromPrevious;

  public String[] arguments;

  private String successCondition;
  public static final String SUCCESS_IF_AT_LEAST_X_FOLDERS_DELETED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private String limitFolders;

  int nrErrors = 0;
  int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int nrLimitFolders = 0;

  public ActionDeleteFolders(String name) {
    super(name, "");
    argFromPrevious = false;
    arguments = null;

    successCondition = SUCCESS_IF_NO_ERRORS;
    limitFolders = "10";
  }

  public ActionDeleteFolders() {
    this("");
  }

  public void allocate(int nrFields) {
    arguments = new String[nrFields];
  }

  public Object clone() {
    ActionDeleteFolders je = (ActionDeleteFolders) super.clone();
    if (arguments != null) {
      int nrFields = arguments.length;
      je.allocate(nrFields);
      System.arraycopy(arguments, 0, je.arguments, 0, nrFields);
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("arg_from_previous", argFromPrevious));
    retval.append("      ").append(XmlHandler.addTagValue("success_condition", successCondition));
    retval.append("      ").append(XmlHandler.addTagValue("limit_folders", limitFolders));

    retval.append("      <fields>").append(Const.CR);
    if (arguments != null) {
      for (int i = 0; i < arguments.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval.append("          ").append(XmlHandler.addTagValue("name", arguments[i]));
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
      successCondition = XmlHandler.getTagValue(entrynode, "success_condition");
      limitFolders = XmlHandler.getTagValue(entrynode, "limit_folders");

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        arguments[i] = XmlHandler.getTagValue(fnode, "name");
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionDeleteFolders.UnableToLoadFromXml"), xe);
    }
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();

    result.setNrErrors(1);
    result.setResult(false);

    nrErrors = 0;
    nrSuccess = 0;
    successConditionBroken = false;
    successConditionBrokenExit = false;
    nrLimitFolders = Const.toInt(resolve(getLimitFolders()), 10);

    if (argFromPrevious && log.isDetailed()) {
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
                  PKG, "ActionDeleteFolders.Error.SuccessConditionbroken", "" + nrErrors));
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
    } else if (arguments != null) {
      for (int i = 0; i < arguments.length && !parentWorkflow.isStopped(); i++) {
        if (successConditionBroken) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionDeleteFolders.Error.SuccessConditionbroken", "" + nrErrors));
          result.setNrErrors(nrErrors);
          result.setNrLinesDeleted(nrSuccess);
          return result;
        }
        String realfilename = resolve(arguments[i]);
        if (!Utils.isEmpty(realfilename)) {
          if (deleteFolder(realfilename)) {
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

    if (log.isDetailed()) {
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
    boolean retval = false;
    if ((nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= nrLimitFolders && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }
    return retval;
  }

  private void updateSuccess() {
    nrSuccess++;
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ((nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrSuccess >= nrLimitFolders
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FOLDERS_DELETED))
        || (nrErrors <= nrLimitFolders && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }

    return retval;
  }

  private boolean deleteFolder(String folderName) {
    boolean rcode = false;
    FileObject filefolder = null;

    try {
      filefolder = HopVfs.getFileObject(folderName);

      if (filefolder.exists()) {
        // the file or folder exists
        if (filefolder.getType() == FileType.FOLDER) {
          // It's a folder
          if (log.isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionDeleteFolders.ProcessingFolder", folderName));
          }
          // Delete Files
          int count = filefolder.delete(new TextFileSelector());

          if (log.isDetailed()) {
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
        if (log.isBasic()) {
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
    } finally {
      if (filefolder != null) {
        try {
          filefolder.close();
        } catch (IOException ex) {
          // Ignore
        }
      }
    }

    return rcode;
  }

  private class TextFileSelector implements FileSelector {
    public boolean includeFile(FileSelectInfo info) {
      return true;
    }

    public boolean traverseDescendents(FileSelectInfo info) {
      return true;
    }
  }

  public void setPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
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

  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  public String[] getArguments() {
    return arguments;
  }

  public void setSuccessCondition(String successCondition) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  public void setLimitFolders(String limitFolders) {
    this.limitFolders = limitFolders;
  }

  public String getLimitFolders() {
    return limitFolders;
  }
}

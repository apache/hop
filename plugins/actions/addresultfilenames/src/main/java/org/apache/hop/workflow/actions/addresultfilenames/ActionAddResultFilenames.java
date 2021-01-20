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

package org.apache.hop.workflow.actions.addresultfilenames;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'add result filenames' action.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
@Action(
    id = "ADD_RESULT_FILENAMES",
    name = "i18n::ActionAddResultFilenames.Name",
    description = "i18n::ActionAddResultFilenames.Description",
    image = "AddResultFileNames.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/actions/addresultfilenames.html")
public class ActionAddResultFilenames extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionAddResultFilenames.class; // For Translator

  public boolean argFromPrevious;

  public boolean deleteallbefore;

  public boolean includeSubfolders;

  public String[] arguments;

  public String[] filemasks;

  public ActionAddResultFilenames(String n) {
    super(n, "");
    argFromPrevious = false;
    deleteallbefore = false;
    arguments = null;

    includeSubfolders = false;
  }

  public ActionAddResultFilenames() {
    this("");
  }

  public Object clone() {
    ActionAddResultFilenames je = (ActionAddResultFilenames) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("arg_from_previous", argFromPrevious));
    retval.append("      ").append(XmlHandler.addTagValue("include_subfolders", includeSubfolders));
    retval.append("      ").append(XmlHandler.addTagValue("delete_all_before", deleteallbefore));

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

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      argFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "arg_from_previous"));
      includeSubfolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "include_subfolders"));
      deleteallbefore =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "delete_all_before"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      arguments = new String[nrFields];
      filemasks = new String[nrFields];

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        arguments[i] = XmlHandler.getTagValue(fnode, "name");
        filemasks[i] = XmlHandler.getTagValue(fnode, "filemask");
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionAddResultFilenames.UnableToLoadFromXml"), xe);
    }
  }

  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    int nrErrFiles = 0;
    result.setResult(true);

    if (deleteallbefore) {
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

    if (argFromPrevious) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionAddResultFilenames.FoundPreviousRows",
                String.valueOf((rows != null ? rows.size() : 0))));
      }
    }

    if (argFromPrevious && rows != null) { // Copy the input row to the (command line) arguments
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        resultRow = rows.get(iteration);

        // Get values from previous result
        String fileFolderPrevious = resultRow.getString(0, null);
        String fileMasksPrevious = resultRow.getString(1, null);

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

      for (int i = 0; i < arguments.length && !parentWorkflow.isStopped(); i++) {

        // ok we can process this file/folder
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionAddResultFilenames.ProcessingArg", arguments[i], filemasks[i]));
        }
        if (!processFile(arguments[i], filemasks[i], parentWorkflow, result)) {
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

    boolean rcode = true;
    FileObject filefolder = null;
    String realFilefoldername = resolve(filename);
    String realwildcard = resolve(wildcard);

    try {
      filefolder = HopVfs.getFileObject(realFilefoldername);
      if (filefolder.exists()) {
        // the file or folder exists

        if (filefolder.getType() == FileType.FILE) {
          // Add filename to Resultfilenames ...
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionAddResultFilenames.AddingFileToResult", filefolder.toString()));
          }
          ResultFile resultFile =
              new ResultFile(
                  ResultFile.FILE_TYPE_GENERAL,
                  HopVfs.getFileObject(filefolder.toString()),
                  parentWorkflow.getWorkflowName(),
                  toString());
          result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
        } else {
          FileObject[] list =
              filefolder.findFiles(new TextFileSelector(filefolder.toString(), realwildcard));

          for (int i = 0; i < list.length && !parentWorkflow.isStopped(); i++) {
            // Add filename to Resultfilenames ...
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG, "ActionAddResultFilenames.AddingFileToResult", list[i].toString()));
            }
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    HopVfs.getFileObject(list[i].toString()),
                    parentWorkflow.getWorkflowName(),
                    toString());
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
          }
        }

      } else {
        // File can not be found
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionAddResultFilenames.FileCanNotbeFound", realFilefoldername));
        }
        rcode = false;
      }
    } catch (Exception e) {
      rcode = false;
      logError(
          BaseMessages.getString(
              PKG, "ActionAddResultFilenames.CouldNotProcess", realFilefoldername, e.getMessage()),
          e);
    } finally {
      if (filefolder != null) {
        try {
          filefolder.close();
          filefolder = null;
        } catch (IOException ex) {
          // Ignore
        }
      }
    }

    return rcode;
  }

  private class TextFileSelector implements FileSelector {
    String fileWildcard = null;
    String sourceFolder = null;

    public TextFileSelector(String sourcefolderin, String filewildcard) {
      if (!Utils.isEmpty(sourcefolderin)) {
        sourceFolder = sourcefolderin;
      }

      if (!Utils.isEmpty(filewildcard)) {
        fileWildcard = filewildcard;
      }
    }

    public boolean includeFile(FileSelectInfo info) {
      boolean returncode = false;
      try {
        if (!info.getFile().toString().equals(sourceFolder)) {
          // Pass over the Base folder itself
          String shortFilename = info.getFile().getName().getBaseName();

          if (info.getFile().getParent().equals(info.getBaseFolder())
              || (!info.getFile().getParent().equals(info.getBaseFolder()) && includeSubfolders)) {
            if ((info.getFile().getType() == FileType.FILE && fileWildcard == null)
                || (info.getFile().getType() == FileType.FILE
                    && fileWildcard != null
                    && GetFileWildcard(shortFilename, fileWildcard))) {
              returncode = true;
            }
          }
        }
      } catch (Exception e) {
        logError(
            "Error while finding files ... in ["
                + info.getFile().toString()
                + "]. Exception :"
                + e.getMessage());
        returncode = false;
      }
      return returncode;
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
  private boolean GetFileWildcard(String selectedfile, String wildcard) {
    Pattern pattern = null;
    boolean getIt = true;

    if (!Utils.isEmpty(wildcard)) {
      pattern = Pattern.compile(wildcard);
      // First see if the file matches the regular expression!
      if (pattern != null) {
        Matcher matcher = pattern.matcher(selectedfile);
        getIt = matcher.matches();
      }
    }

    return getIt;
  }

  public void setIncludeSubfolders(boolean includeSubfolders) {
    this.includeSubfolders = includeSubfolders;
  }

  public void setArgumentsPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  public void setDeleteAllBefore(boolean deleteallbefore) {
    this.deleteallbefore = deleteallbefore;
  }

  public boolean isEvaluation() {
    return true;
  }

  public boolean isArgFromPrevious() {
    return argFromPrevious;
  }

  public boolean deleteAllBefore() {
    return deleteallbefore;
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

    if (res == false) {
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
}

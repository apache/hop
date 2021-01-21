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

package org.apache.hop.workflow.actions.checkfilelocked;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'check files locked' action.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
@Action(
    id = "CHECK_FILES_LOCKED",
    name = "i18n::ActionCheckFilesLocked.Name",
    description = "i18n::ActionCheckFilesLocked.Description",
    image = "CheckFilesLocked.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/checkfilelocked.html")
public class ActionCheckFilesLocked extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCheckFilesLocked.class; // For Translator

  public boolean argFromPrevious;

  public boolean includeSubfolders;

  public String[] arguments;

  public String[] filemasks;

  private boolean oneFileLocked;

  public ActionCheckFilesLocked(String n) {
    super(n, "");
    argFromPrevious = false;
    arguments = null;

    includeSubfolders = false;
  }

  public ActionCheckFilesLocked() {
    this("");
  }

  public Object clone() {
    ActionCheckFilesLocked je = (ActionCheckFilesLocked) super.clone();
    if (arguments != null) {
      int nrFields = arguments.length;
      je.allocate(nrFields);
      System.arraycopy(arguments, 0, je.arguments, 0, nrFields);
      System.arraycopy(filemasks, 0, je.filemasks, 0, nrFields);
    }
    return je;
  }

  public void allocate(int nrFields) {
    arguments = new String[nrFields];
    filemasks = new String[nrFields];
  }

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

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      argFromPrevious =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "arg_from_previous"));
      includeSubfolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "include_subfolders"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        arguments[i] = XmlHandler.getTagValue(fnode, "name");
        filemasks[i] = XmlHandler.getTagValue(fnode, "filemask");
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionCheckFilesLocked.UnableToLoadFromXml"), xe);
    }
  }

  public Result execute(Result previousResult, int nr) {

    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    oneFileLocked = false;
    result.setResult(true);

    try {
      if (argFromPrevious) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionCheckFilesLocked.FoundPreviousRows",
                  String.valueOf((rows != null ? rows.size() : 0))));
        }
      }

      if (argFromPrevious && rows != null) {
        // Copy the input row to the (command line) arguments
        for (int iteration = 0;
            iteration < rows.size() && !parentWorkflow.isStopped();
            iteration++) {
          resultRow = rows.get(iteration);

          // Get values from previous result
          String fileFolderPrevious = resultRow.getString(0, "");
          String fileMasksPrevious = resultRow.getString(1, "");

          // ok we can process this file/folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckFilesLocked.ProcessingRow",
                    fileFolderPrevious,
                    fileMasksPrevious));
          }

          ProcessFile(fileFolderPrevious, fileMasksPrevious);
        }
      } else if (arguments != null) {

        for (int i = 0; i < arguments.length && !parentWorkflow.isStopped(); i++) {
          // ok we can process this file/folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.ProcessingArg", arguments[i], filemasks[i]));
          }

          ProcessFile(arguments[i], filemasks[i]);
        }
      }

      if (oneFileLocked) {
        result.setResult(false);
        result.setNrErrors(1);
      }
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionCheckFilesLocked.ErrorRunningAction", e));
    }

    return result;
  }

  private void ProcessFile(String filename, String wildcard) {

    FileObject filefolder = null;
    String realFilefoldername = resolve(filename);
    String realwilcard = resolve(wildcard);

    try {
      filefolder = HopVfs.getFileObject(realFilefoldername);
      FileObject[] files = new FileObject[] {filefolder};
      if (filefolder.exists()) {
        // the file or folder exists
        if (filefolder.getType() == FileType.FOLDER) {
          // It's a folder
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.ProcessingFolder", realFilefoldername));
          }
          // Retrieve all files
          files = filefolder.findFiles(new TextFileSelector(filefolder.toString(), realwilcard));

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.TotalFilesToCheck", String.valueOf(files.length)));
          }
        } else {
          // It's a file
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCheckFilesLocked.ProcessingFile", realFilefoldername));
          }
        }
        // Check files locked
        checkFilesLocked(files);
      } else {
        // We can not find thsi file
        logBasic(
            BaseMessages.getString(PKG, "ActionCheckFilesLocked.FileNotExist", realFilefoldername));
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionCheckFilesLocked.CouldNotProcess", realFilefoldername, e.getMessage()));
    } finally {
      if (filefolder != null) {
        try {
          filefolder.close();
        } catch (IOException ex) {
          // Ignore
        }
      }
    }
  }

  private void checkFilesLocked(FileObject[] files) throws HopException {

    for (int i = 0; i < files.length && !oneFileLocked; i++) {
      FileObject file = files[i];
      String filename = HopVfs.getFilename(file);
      LockFile locked = new LockFile(filename);
      if (locked.isLocked()) {
        oneFileLocked = true;
        logError(BaseMessages.getString(PKG, "JobCheckFilesLocked.Log.FileLocked", filename));
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "JobCheckFilesLocked.Log.FileNotLocked", filename));
        }
      }
    }
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
      FileObject filename = null;
      try {

        if (!info.getFile().toString().equals(sourceFolder)) {
          // Pass over the Base folder itself

          String shortFilename = info.getFile().getName().getBaseName();

          if (!info.getFile().getParent().equals(info.getBaseFolder())) {

            // Not in the Base Folder..Only if include sub folders
            if (includeSubfolders
                && (info.getFile().getType() == FileType.FILE)
                && GetFileWildcard(shortFilename, fileWildcard)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionCheckFilesLocked.CheckingFile", info.getFile().toString()));
              }

              returncode = true;
            }
          } else {
            // In the Base Folder...

            if ((info.getFile().getType() == FileType.FILE)
                && GetFileWildcard(shortFilename, fileWildcard)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionCheckFilesLocked.CheckingFile", info.getFile().toString()));
              }

              returncode = true;
            }
          }
        }

      } catch (Exception e) {
        logError(
            BaseMessages.getString(PKG, "JobCheckFilesLocked.Error.Exception.ProcessError"),
            BaseMessages.getString(
                PKG,
                "JobCheckFilesLocked.Error.Exception.Process",
                info.getFile().toString(),
                e.getMessage()));
        returncode = false;
      } finally {
        if (filename != null) {
          try {
            filename.close();

          } catch (IOException ex) {
            /* Ignore */
          }
        }
      }

      return returncode;
    }

    public boolean traverseDescendents(FileSelectInfo info) {
      return info.getDepth() == 0 || includeSubfolders;
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

  public void setargFromPrevious(boolean argFromPrevious) {
    this.argFromPrevious = argFromPrevious;
  }

  public boolean isEvaluation() {
    return true;
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

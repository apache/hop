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

package org.apache.hop.workflow.actions.copymoveresultfilenames;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileUtil;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'copymoveresultfilenames' action. Its main use would be to copy or move files in
 * the result filenames to a destination folder. that can be used to control the flow in ETL cycles.
 *
 * @author Samatar
 * @since 25-02-2008
 */
@Action(
    id = "COPY_MOVE_RESULT_FILENAMES",
    name = "i18n::ActionCopyMoveResultFilenames.Name",
    description = "i18n::ActionCopyMoveResultFilenames.Description",
    image = "CopyMoveResultFilenames.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/actions/copymoveresultfilenames.html")
public class ActionCopyMoveResultFilenames extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCopyMoveResultFilenames.class; // For Translator

  private String folderName;
  private boolean specifyWildcard;
  private String wildcard;
  private String wildcardExclude;
  private String destinationFolder;
  private String nrErrorsLessThan;

  public String SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED = "success_when_at_least";
  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";
  private String successCondition;
  private Pattern wildcardPattern;
  private Pattern wildcardExcludePattern;

  private boolean addDate;
  private boolean addTime;
  private boolean specifyFormat;
  private String dateTimeFormat;
  private boolean addDateBeforeExtension;
  private String action;

  private boolean overwriteFile;
  private boolean createDestinationFolder;
  boolean removedSourceFilename;
  boolean addDestinationFilename;

  int nrErrors = 0;
  private int nrSuccess = 0;
  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;
  int limitFiles = 0;

  public ActionCopyMoveResultFilenames(String n) {
    super(n, "");
    removedSourceFilename = true;
    addDestinationFilename = true;
    createDestinationFolder = false;
    folderName = null;
    wildcardExclude = null;
    wildcard = null;
    specifyWildcard = false;

    overwriteFile = false;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    dateTimeFormat = null;
    addDateBeforeExtension = false;
    destinationFolder = null;
    nrErrorsLessThan = "10";

    action = "copy";
    successCondition = SUCCESS_IF_NO_ERRORS;
  }

  public ActionCopyMoveResultFilenames() {
    this("");
  }

  public Object clone() {
    ActionCopyMoveResultFilenames je = (ActionCopyMoveResultFilenames) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(500); // 358 chars in just tags and spaces alone

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("foldername", folderName));
    retval.append("      ").append(XmlHandler.addTagValue("specify_wildcard", specifyWildcard));
    retval.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append("      ").append(XmlHandler.addTagValue("wildcardexclude", wildcardExclude));
    retval.append("      ").append(XmlHandler.addTagValue("destination_folder", destinationFolder));
    retval.append("      ").append(XmlHandler.addTagValue("nr_errors_less_than", nrErrorsLessThan));
    retval.append("      ").append(XmlHandler.addTagValue("success_condition", successCondition));
    retval.append("      ").append(XmlHandler.addTagValue("add_date", addDate));
    retval.append("      ").append(XmlHandler.addTagValue("add_time", addTime));
    retval.append("      ").append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    retval.append("      ").append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    retval.append("      ").append(XmlHandler.addTagValue("action", action));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("AddDateBeforeExtension", addDateBeforeExtension));
    retval.append("      ").append(XmlHandler.addTagValue("OverwriteFile", overwriteFile));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("CreateDestinationFolder", createDestinationFolder));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("RemovedSourceFilename", removedSourceFilename));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("AddDestinationFilename", addDestinationFilename));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      folderName = XmlHandler.getTagValue(entrynode, "foldername");
      specifyWildcard = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "specify_wildcard"));
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      wildcardExclude = XmlHandler.getTagValue(entrynode, "wildcardexclude");
      destinationFolder = XmlHandler.getTagValue(entrynode, "destination_folder");
      nrErrorsLessThan = XmlHandler.getTagValue(entrynode, "nr_errors_less_than");
      successCondition = XmlHandler.getTagValue(entrynode, "success_condition");
      addDate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_date"));
      addTime = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "add_time"));
      specifyFormat = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "SpecifyFormat"));
      addDateBeforeExtension =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "AddDateBeforeExtension"));

      dateTimeFormat = XmlHandler.getTagValue(entrynode, "date_time_format");
      action = XmlHandler.getTagValue(entrynode, "action");

      overwriteFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "OverwriteFile"));
      createDestinationFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "CreateDestinationFolder"));
      removedSourceFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "RemovedSourceFilename"));
      addDestinationFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "AddDestinationFilename"));

    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "ActionCopyMoveResultFilenames.CanNotLoadFromXML", xe.getMessage()));
    }
  }

  public void setSpecifyWildcard(boolean specifyWildcard) {
    this.specifyWildcard = specifyWildcard;
  }

  public boolean isSpecifyWildcard() {
    return specifyWildcard;
  }

  public void setFoldername(String folderName) {
    this.folderName = folderName;
  }

  public String getFoldername() {
    return folderName;
  }

  public String getWildcard() {
    return wildcard;
  }

  public String getWildcardExclude() {
    return wildcardExclude;
  }

  public String getRealWildcard() {
    return resolve(getWildcard());
  }

  public void setWildcard(String wildcard) {
    this.wildcard = wildcard;
  }

  public void setWildcardExclude(String wildcardExclude) {
    this.wildcardExclude = wildcardExclude;
  }

  public void setAddDate(boolean adddate) {
    this.addDate = adddate;
  }

  public boolean isAddDate() {
    return addDate;
  }

  public void setAddTime(boolean addtime) {
    this.addTime = addtime;
  }

  public boolean isAddTime() {
    return addTime;
  }

  public void setAddDateBeforeExtension(boolean addDateBeforeExtension) {
    this.addDateBeforeExtension = addDateBeforeExtension;
  }

  public boolean isAddDateBeforeExtension() {
    return addDateBeforeExtension;
  }

  public boolean isOverwriteFile() {
    return overwriteFile;
  }

  public void setOverwriteFile(boolean overwriteFile) {
    this.overwriteFile = overwriteFile;
  }

  public void setCreateDestinationFolder(boolean createDestinationFolder) {
    this.createDestinationFolder = createDestinationFolder;
  }

  public boolean isCreateDestinationFolder() {
    return createDestinationFolder;
  }

  public boolean isRemovedSourceFilename() {
    return removedSourceFilename;
  }

  public void setRemovedSourceFilename(boolean removedSourceFilename) {
    this.removedSourceFilename = removedSourceFilename;
  }

  public void setAddDestinationFilename(boolean addDestinationFilename) {
    this.addDestinationFilename = addDestinationFilename;
  }

  public boolean isAddDestinationFilename() {
    return addDestinationFilename;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat(boolean specifyFormat) {
    this.specifyFormat = specifyFormat;
  }

  public void setDestinationFolder(String destinationFolder) {
    this.destinationFolder = destinationFolder;
  }

  public String getDestinationFolder() {
    return destinationFolder;
  }

  public void setNrErrorsLessThan(String nrErrorsLessThan) {
    this.nrErrorsLessThan = nrErrorsLessThan;
  }

  public String getNrErrorsLessThan() {
    return nrErrorsLessThan;
  }

  public void setSuccessCondition(String successCondition) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public String getAction() {
    return action;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);

    boolean deleteFile = getAction().equals("delete");

    String realdestinationFolder = null;
    if (!deleteFile) {
      realdestinationFolder = resolve(getDestinationFolder());

      if (!createDestinationFolder(realdestinationFolder)) {
        return result;
      }
    }
    if (!Utils.isEmpty(wildcard)) {
      wildcardPattern = Pattern.compile(resolve(wildcard));
    }
    if (!Utils.isEmpty(wildcardExclude)) {
      wildcardExcludePattern = Pattern.compile(resolve(wildcardExclude));
    }

    if (previousResult != null) {
      nrErrors = 0;
      limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);
      nrErrors = 0;
      nrSuccess = 0;
      successConditionBroken = false;
      successConditionBrokenExit = false;

      FileObject file = null;

      try {
        int size = result.getResultFiles().size();
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.log.FilesFound", "" + size));
        }

        List<ResultFile> resultFiles = result.getResultFilesList();
        if (resultFiles != null && resultFiles.size() > 0) {
          for (Iterator<ResultFile> it = resultFiles.iterator();
              it.hasNext() && !parentWorkflow.isStopped(); ) {
            if (successConditionBroken) {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.Error.SuccessConditionbroken",
                      "" + nrErrors));
              throw new Exception(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.Error.SuccessConditionbroken",
                      "" + nrErrors));
            }

            ResultFile resultFile = it.next();
            file = resultFile.getFile();
            if (file != null && file.exists()) {
              if (!specifyWildcard
                  || (CheckFileWildcard(file.getName().getBaseName(), wildcardPattern, true)
                      && !CheckFileWildcard(
                          file.getName().getBaseName(), wildcardExcludePattern, false)
                      && specifyWildcard)) {
                // Copy or Move file
                if (!processFile(file, realdestinationFolder, result, parentWorkflow, deleteFile)) {
                  // Update Errors
                  updateErrors();
                }
              }

            } else {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.log.ErrorCanNotFindFile",
                      file.toString()));
              // Update Errors
              updateErrors();
            }
          } // end for
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionCopyMoveResultFilenames.Error", e.toString()));
      } finally {
        if (file != null) {
          try {
            file.close();
            file = null;
          } catch (Exception ex) {
            /* Ignore */
          }
        }
      }
    }
    // Success Condition
    result.setNrErrors(nrErrors);
    result.setNrLinesWritten(nrSuccess);
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
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }
    return retval;
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ((nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrSuccess >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }

    return retval;
  }

  private boolean createDestinationFolder(String folderName) {
    FileObject folder = null;
    try {
      folder = HopVfs.getFileObject(folderName);

      if (!folder.exists()) {
        logError(
            BaseMessages.getString(
                PKG, "ActionCopyMoveResultFilenames.Log.FolderNotExists", folderName));
        if (isCreateDestinationFolder()) {
          folder.createFolder();
        } else {
          return false;
        }
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.Log.FolderCreated", folderName));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.Log.FolderExists", folderName));
        }
      }
      return true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionCopyMoveResultFilenames.Log.CanNotCreatedFolder",
              folderName,
              e.toString()));

    } finally {
      if (folder != null) {
        try {
          folder.close();
          folder = null;
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
    return false;
  }

  private boolean processFile(
      FileObject sourcefile,
      String destinationFolder,
      Result result,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      boolean deleteFile) {
    boolean retval = false;

    try {
      if (deleteFile) {
        // delete file
        if (sourcefile.delete()) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCopyMoveResultFilenames.log.DeletedFile", sourcefile.toString()));
          }

          // Remove source file from result files list
          result.getResultFiles().remove(sourcefile.toString());
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCopyMoveResultFilenames.RemovedFileFromResult",
                    sourcefile.toString()));
          }

        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionCopyMoveResultFilenames.CanNotDeletedFile", sourcefile.toString()));
        }
      } else {
        // return destination short filename
        String shortfilename = getDestinationFilename(sourcefile.getName().getBaseName());
        // build full destination filename
        String destinationFilename = destinationFolder + Const.FILE_SEPARATOR + shortfilename;
        FileObject destinationfile = HopVfs.getFileObject(destinationFilename);
        boolean filexists = destinationfile.exists();
        if (filexists) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionCopyMoveResultFilenames.Log.FileExists", destinationFilename));
          }
        }
        if ((!filexists) || (filexists && isOverwriteFile())) {
          if (getAction().equals("copy")) {
            // Copy file
            FileUtil.copyContent(sourcefile, destinationfile);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.log.CopiedFile",
                      sourcefile.toString(),
                      destinationFolder));
            }
          } else {
            // Move file
            sourcefile.moveTo(destinationfile);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.log.MovedFile",
                      sourcefile.toString(),
                      destinationFolder));
            }
          }
          if (isRemovedSourceFilename()) {
            // Remove source file from result files list
            result.getResultFiles().remove(sourcefile.toString());
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.RemovedFileFromResult",
                      sourcefile.toString()));
            }
          }
          if (isAddDestinationFilename()) {
            // Add destination filename to Resultfilenames ...
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    HopVfs.getFileObject(destinationfile.toString()),
                    parentWorkflow.getWorkflowName(),
                    toString());
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG,
                      "ActionCopyMoveResultFilenames.AddedFileToResult",
                      destinationfile.toString()));
            }
          }
        }
      }
      retval = true;
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionCopyMoveResultFilenames.Log.ErrorProcessing", e.toString()));
    }

    return retval;
  }

  private String getDestinationFilename(String shortsourcefilename) throws Exception {
    String shortfilename = shortsourcefilename;
    int lenstring = shortsourcefilename.length();
    int lastindexOfDot = shortfilename.lastIndexOf('.');
    if (isAddDateBeforeExtension()) {
      shortfilename = shortfilename.substring(0, lastindexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (isSpecifyFormat() && !Utils.isEmpty(getDateTimeFormat())) {
      daf.applyPattern(getDateTimeFormat());
      String dt = daf.format(now);
      shortfilename += dt;
    } else {
      if (isAddDate()) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        shortfilename += "_" + d;
      }
      if (isAddTime()) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        shortfilename += "_" + t;
      }
    }
    if (isAddDateBeforeExtension()) {
      shortfilename += shortsourcefilename.substring(lastindexOfDot, lenstring);
    }

    return shortfilename;
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param pattern
   * @param include
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean CheckFileWildcard(String selectedfile, Pattern pattern, boolean include) {
    boolean getIt = include;
    if (pattern != null) {
      Matcher matcher = pattern.matcher(selectedfile);
      getIt = matcher.matches();
    }
    return getIt;
  }

  public boolean isEvaluation() {
    return true;
  }

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

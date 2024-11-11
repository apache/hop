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

package org.apache.hop.workflow.actions.xml.xmlwellformed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlCheck;
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
import org.xml.sax.helpers.DefaultHandler;

@Getter
@Setter
/** This defines a 'xml well formed' workflow action. */
@Action(
    id = "XML_WELL_FORMED",
    name = "i18n::XML_WELL_FORMED.Name",
    description = "i18n::XML_WELL_FORMED.Description",
    image = "XFC.svg",
    categoryDescription = "i18n::XML_WELL_FORMED.Category",
    keywords = "i18n::XmlWellFormed.keyword",
    documentationUrl = "/workflow/actions/xmlwellformed.html")
public class XmlWellFormed extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = XmlWellFormed.class;

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_WELL_FORMED = "success_when_at_least";
  public static final String SUCCESS_IF_BAD_FORMED_FILES_LESS = "success_if_bad_formed_files_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  public static final String ADD_ALL_FILENAMES = "all_filenames";
  public static final String ADD_WELL_FORMED_FILES_ONLY = "only_well_formed_filenames";
  public static final String ADD_BAD_FORMED_FILES_ONLY = "only_bad_formed_filenames";
  public static final String CONST_ACTION_XMLWELL_FORMED_ERROR_SUCCESS_CONDITIONBROKEN =
      "ActionXMLWellFormed.Error.SuccessConditionbroken";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_FIELDS = "fields";
  public static final String CONST_FIELD = "field";

  /**
   * @deprecated no longer used
   */
  @Deprecated(since = "2.0")
  @HopMetadataProperty(key = "arg_from_previous")
  public boolean argFromPrevious;

  /**
   * @deprecated no longer used
   */
  @Deprecated(since = "2.0")
  @HopMetadataProperty(key = "include_subfolders")
  public boolean includeSubfolders;

  /**
   * @deprecated no longer used
   */
  @Deprecated(since = "2.0")
  @HopMetadataProperty(key = "field", groupKey = "fields")
  private List<XmlWellFormedField> sourceFileFolders;

  /**
   * @deprecated no longer used
   */
  @Deprecated(since = "2.0")
  public String[] wildcard;

  @HopMetadataProperty(key = "nr_errors_less_than")
  private String nrErrorsLessThan;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  @HopMetadataProperty(key = "resultfilenames")
  private String resultFilenames;

  int nrAllErrors = 0;
  int nrBadFormed = 0;
  int nrWellFormed = 0;
  int limitFiles = 0;
  int nrErrors = 0;

  boolean successConditionBroken = false;
  boolean successConditionBrokenExit = false;

  public XmlWellFormed(String n) {
    super(n, "");
    resultFilenames = ADD_ALL_FILENAMES;
    argFromPrevious = false;
    sourceFileFolders = new ArrayList<>();
    wildcard = null;
    includeSubfolders = false;
    nrErrorsLessThan = "10";
    successCondition = SUCCESS_IF_NO_ERRORS;
  }

  public XmlWellFormed() {
    this("");
  }

  @Override
  public Object clone() {
    XmlWellFormed je = (XmlWellFormed) super.clone();
    return je;
  }

  @Override
  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);

    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    nrErrors = 0;
    nrWellFormed = 0;
    nrBadFormed = 0;
    limitFiles = Const.toInt(resolve(getNrErrorsLessThan()), 10);
    successConditionBroken = false;
    successConditionBrokenExit = false;

    // Get source and destination files, also wildcard
    List<XmlWellFormedField> vSourceFileFolder = sourceFileFolders;
    //    String[] vwildcard = wildcard;

    if (argFromPrevious && isDetailed()) {
      logDetailed(
          BaseMessages.getString(
              PKG,
              "ActionXMLWellFormed.Log.ArgFromPrevious.Found",
              (rows != null ? rows.size() : 0) + ""));
    }
    if (argFromPrevious && rows != null) {
      // Copy the input row to the (command line) arguments
      for (int iteration = 0; iteration < rows.size() && !parentWorkflow.isStopped(); iteration++) {
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG,
                    CONST_ACTION_XMLWELL_FORMED_ERROR_SUCCESS_CONDITIONBROKEN,
                    "" + nrAllErrors));
            successConditionBrokenExit = true;
          }
          result.setEntryNr(nrAllErrors);
          result.setNrLinesRejected(nrBadFormed);
          result.setNrLinesWritten(nrWellFormed);
          return result;
        }

        resultRow = rows.get(iteration);

        // Get source and destination file names, also wildcard
        String vSourceFileFolderPrevious = resultRow.getString(0, null);
        String vWildcardPrevious = resultRow.getString(1, null);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionXMLWellFormed.Log.ProcessingRow",
                  vSourceFileFolderPrevious,
                  vWildcardPrevious));
        }

        processFileFolder(vSourceFileFolderPrevious, vWildcardPrevious, parentWorkflow, result);
      }
    } else if (vSourceFileFolder != null) {
      for (int i = 0; i < vSourceFileFolder.size() && !parentWorkflow.isStopped(); i++) {
        if (successConditionBroken) {
          if (!successConditionBrokenExit) {
            logError(
                BaseMessages.getString(
                    PKG,
                    CONST_ACTION_XMLWELL_FORMED_ERROR_SUCCESS_CONDITIONBROKEN,
                    "" + nrAllErrors));
            successConditionBrokenExit = true;
          }
          result.setEntryNr(nrAllErrors);
          result.setNrLinesRejected(nrBadFormed);
          result.setNrLinesWritten(nrWellFormed);
          return result;
        }

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ActionXMLWellFormed.Log.ProcessingRow",
                  vSourceFileFolder.get(i),
                  vSourceFileFolder.get(i).getWildcard()));
        }

        processFileFolder(
            vSourceFileFolder.get(i).getSourceFilefolder(),
            vSourceFileFolder.get(i).getWildcard(),
            parentWorkflow,
            result);
      }
    }

    // Success Condition
    result.setNrErrors(nrAllErrors);
    result.setNrLinesRejected(nrBadFormed);
    result.setNrLinesWritten(nrWellFormed);
    if (getSuccessStatus()) {
      result.setNrErrors(0);
      result.setResult(true);
    }

    displayResults();

    return result;
  }

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(PKG, "ActionXMLWellFormed.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionXMLWellFormed.Log.Info.FilesInBadFormed", "" + nrBadFormed));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionXMLWellFormed.Log.Info.FilesInWellFormed", "" + nrWellFormed));
      logDetailed("=======================================");
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ((nrAllErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrBadFormed >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_BAD_FORMED_FILES_LESS))) {
      retval = true;
    }
    return retval;
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ((nrAllErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrWellFormed >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_WELL_FORMED))
        || (nrBadFormed < limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_BAD_FORMED_FILES_LESS))) {
      retval = true;
    }

    return retval;
  }

  private void updateErrors() {
    nrErrors++;
    updateAllErrors();
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private void updateAllErrors() {
    nrAllErrors = nrErrors + nrBadFormed;
  }

  public static class XMLTreeHandler extends DefaultHandler {}

  private boolean CheckFile(FileObject file) {
    boolean retval = false;
    try {
      retval = XmlCheck.isXmlFileWellFormed(file);
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionXMLWellFormed.Log.ErrorCheckingFile", file.toString(), e.getMessage()));
    }

    return retval;
  }

  private boolean processFileFolder(
      String sourcefilefoldername, String wildcard, IWorkflowEngine parentWorkflow, Result result) {
    boolean entrystatus = false;
    FileObject sourcefilefolder = null;
    FileObject currentFile = null;

    // Get real source file and wilcard
    String realSourceFilefoldername = resolve(sourcefilefoldername);
    if (Utils.isEmpty(realSourceFilefoldername)) {
      logError(
          BaseMessages.getString(
              PKG, "ActionXMLWellFormed.log.FileFolderEmpty", sourcefilefoldername));
      // Update Errors
      updateErrors();

      return entrystatus;
    }
    String realWildcard = resolve(wildcard);

    try {
      sourcefilefolder = HopVfs.getFileObject(realSourceFilefoldername);

      if (sourcefilefolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionXMLWellFormed.Log.FileExists", sourcefilefolder.toString()));
        }
        if (sourcefilefolder.getType() == FileType.FILE) {
          entrystatus = checkOneFile(sourcefilefolder, result, parentWorkflow);

        } else if (sourcefilefolder.getType() == FileType.FOLDER) {
          FileObject[] fileObjects =
              sourcefilefolder.findFiles(
                  new AllFileSelector() {
                    @Override
                    public boolean traverseDescendents(FileSelectInfo info) {
                      return true;
                    }

                    @Override
                    public boolean includeFile(FileSelectInfo info) {

                      FileObject fileObject = info.getFile();
                      try {
                        if (fileObject == null) {
                          return false;
                        }
                        if (fileObject.getType() != FileType.FILE) {
                          return false;
                        }
                      } catch (Exception ex) {
                        // Upon error don't process the file.
                        return false;
                      } finally {
                        if (fileObject != null) {
                          try {
                            fileObject.close();
                          } catch (IOException ex) {
                            /* Ignore */
                          }
                        }
                      }
                      return true;
                    }
                  });

          if (fileObjects != null) {
            for (int j = 0; j < fileObjects.length && !parentWorkflow.isStopped(); j++) {
              if (successConditionBroken) {
                if (!successConditionBrokenExit) {
                  logError(
                      BaseMessages.getString(
                          PKG,
                          CONST_ACTION_XMLWELL_FORMED_ERROR_SUCCESS_CONDITIONBROKEN,
                          "" + nrAllErrors));
                  successConditionBrokenExit = true;
                }
                return false;
              }
              // Fetch files in list one after one ...
              currentFile = fileObjects[j];

              if (!currentFile.getParent().toString().equals(sourcefilefolder.toString())) {
                // Not in the Base Folder..Only if include sub folders
                if (includeSubfolders && GetFileWildcard(currentFile.toString(), realWildcard)) {
                  checkOneFile(currentFile, result, parentWorkflow);
                }

              } else {
                // In the base folder
                if (GetFileWildcard(currentFile.toString(), realWildcard)) {
                  checkOneFile(currentFile, result, parentWorkflow);
                }
              }
            }
          }
        } else {
          logError(
              BaseMessages.getString(
                  PKG, "ActionXMLWellFormed.Error.UnknowFileFormat", sourcefilefolder.toString()));
          // Update Errors
          updateErrors();
        }
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ActionXMLWellFormed.Error.SourceFileNotExists", realSourceFilefoldername));
        // Update Errors
        updateErrors();
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionXMLWellFormed.Error.Exception.Processing", realSourceFilefoldername, e));
      // Update Errors
      updateErrors();
    } finally {
      if (sourcefilefolder != null) {
        try {
          sourcefilefolder.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
      if (currentFile != null) {
        try {
          currentFile.close();
        } catch (IOException ex) {
          /* Ignore */
        }
      }
    }
    return entrystatus;
  }

  private boolean checkOneFile(FileObject file, Result result, IWorkflowEngine parentWorkflow)
      throws HopException {
    boolean retval = false;
    try {
      // We deal with a file..so let's check if it's well formed
      boolean retformed = CheckFile(file);
      if (!retformed) {
        logError(
            BaseMessages.getString(
                PKG, "ActionXMLWellFormed.Error.FileBadFormed", file.toString()));
        // Update Bad formed files number
        updateBadFormed();
        if (resultFilenames.equals(ADD_ALL_FILENAMES)
            || resultFilenames.equals(ADD_BAD_FORMED_FILES_ONLY)) {
          addFileToResultFilenames(HopVfs.getFilename(file), result, parentWorkflow);
        }
      } else {
        if (isDetailed()) {
          logDetailed("---------------------------");
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionXMLWellFormed.Error.FileWellFormed", file.toString()));
        }
        // Update Well formed files number
        updateWellFormed();
        if (resultFilenames.equals(ADD_ALL_FILENAMES)
            || resultFilenames.equals(ADD_WELL_FORMED_FILES_ONLY)) {
          addFileToResultFilenames(HopVfs.getFilename(file), result, parentWorkflow);
        }
      }

    } catch (Exception e) {
      throw new HopException("Unable to verify file '" + file + "'", e);
    }
    return retval;
  }

  private void updateWellFormed() {
    nrWellFormed++;
  }

  private void updateBadFormed() {
    nrBadFormed++;
    updateAllErrors();
  }

  private void addFileToResultFilenames(
      String fileaddentry, Result result, IWorkflowEngine parentWorkflow) {
    try {
      ResultFile resultFile =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL,
              HopVfs.getFileObject(fileaddentry),
              parentWorkflow.getWorkflowName(),
              toString());
      result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionXMLWellFormed.Log.FileAddedToResultFilesName", fileaddentry));
      }

    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG,
              "ActionXMLWellFormed.Error.AddingToFilenameResult",
              fileaddentry,
              e.getMessage()));
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

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta jobMeta,
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

    for (int i = 0; i < sourceFileFolders.size(); i++) {
      ActionValidatorUtils.andValidator().validate(this, "arguments[" + i + "]", remarks, ctx);
    }
  }
}

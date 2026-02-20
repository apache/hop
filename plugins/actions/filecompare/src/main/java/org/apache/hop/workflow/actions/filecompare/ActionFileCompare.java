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

package org.apache.hop.workflow.actions.filecompare;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopFileException;
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
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;

/**
 * This defines a 'file compare' action. It will compare 2 files in a binary way, and will either
 * follow the true flow upon the files being the same or the false flow otherwise.
 */
@Action(
    id = "FILE_COMPARE",
    name = "i18n::ActionFileCompare.Name",
    description = "i18n::ActionFileCompare.Description",
    image = "FileCompare.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    keywords = "i18n::ActionFileCompare.keyword",
    documentationUrl = "/workflow/actions/filecompare.html")
public class ActionFileCompare extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFileCompare.class;

  @HopMetadataProperty(key = "filename1")
  private String filename1;

  @HopMetadataProperty(key = "filename2")
  private String filename2;

  @HopMetadataProperty(key = "add_filename_result")
  private boolean addFilenameToResult;

  public ActionFileCompare(String n) {
    super(n, "");
    filename1 = null;
    filename2 = null;
    addFilenameToResult = false;
  }

  public ActionFileCompare() {
    this("");
  }

  public ActionFileCompare(ActionFileCompare meta) {
    super(meta.getName(), meta.getDescription(), meta.getPluginId());
    this.filename1 = meta.filename1;
    this.filename2 = meta.filename2;
    this.addFilenameToResult = meta.addFilenameToResult;
  }

  @Override
  public Object clone() {
    return new ActionFileCompare(this);
  }

  public String getRealFilename1() {
    return resolve(getFilename1());
  }

  public String getRealFilename2() {
    return resolve(getFilename2());
  }

  /**
   * Check whether 2 files have the same contents.
   *
   * @param file1 first file to compare
   * @param file2 second file to compare
   * @return true if files are equal, false if they are not
   * @throws org.apache.hop.core.exception.HopFileException upon IO problems
   */
  protected boolean equalFileContents(FileObject file1, FileObject file2) throws HopFileException {
    // Really read the contents and do comparisons.
    try (InputStream in1 =
            new BufferedInputStream(
                HopVfs.getInputStream(HopVfs.getFilename(file1), getVariables()));
        InputStream in2 =
            new BufferedInputStream(
                HopVfs.getInputStream(HopVfs.getFilename(file2), getVariables()))) {

      int b1;
      int b2;
      while (true) {
        b1 = in1.read();
        b2 = in2.read();
        if (b1 == -1 || b2 == -1) {
          break;
        }
        if (b1 != b2) {
          return false;
        }
      }
      // Both streams must be at EOF for files to be equal
      return b1 == -1 && b2 == -1;
    } catch (IOException e) {
      throw new HopFileException(e);
    }
    // Nothing to do here
    // Nothing to see here...
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    String realFilename1 = getRealFilename1();
    String realFilename2 = getRealFilename2();

    FileObject file1 = null;
    FileObject file2 = null;
    try {
      if (filename1 != null && filename2 != null) {

        file1 = HopVfs.getFileObject(realFilename1, getVariables());
        file2 = HopVfs.getFileObject(realFilename2, getVariables());

        if (file1.exists() && file2.exists()) {
          result.setResult(equalFileContents(file1, file2));

          // add filename to result filenames
          if (addFilenameToResult
              && file1.getType() == FileType.FILE
              && file2.getType() == FileType.FILE) {
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    file1,
                    parentWorkflow.getWorkflowName(),
                    toString());
            resultFile.setComment(BaseMessages.getString(PKG, "ActionWaitForFile.FilenameAdded"));
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
            resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    file2,
                    parentWorkflow.getWorkflowName(),
                    toString());
            resultFile.setComment(BaseMessages.getString(PKG, "ActionWaitForFile.FilenameAdded"));
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
          }
        } else {
          if (!file1.exists()) {
            logError(
                BaseMessages.getString(
                    PKG, "ActionFileCompare.ERROR_0004_File1_Does_Not_Exist", realFilename1));
          }
          if (!file2.exists()) {
            logError(
                BaseMessages.getString(
                    PKG, "ActionFileCompare.ERROR_0005_File2_Does_Not_Exist", realFilename2));
          }
          result.setResult(false);
          result.setNrErrors(1);
        }
      } else {
        logError(BaseMessages.getString(PKG, "ActionFileCompare.ERROR_0006_Need_Two_Filenames"));
      }
    } catch (Exception e) {
      result.setResult(false);
      result.setNrErrors(1);
      logError(
          BaseMessages.getString(
              PKG,
              "ActionFileCompare.ERROR_0007_Comparing_Files",
              realFilename1,
              realFilename2,
              e.getMessage()));
    } finally {
      try {
        if (file1 != null) {
          file1.close();
          file1 = null;
        }

        if (file2 != null) {
          file2.close();
          file2 = null;
        }
      } catch (IOException e) {
        // Ignore errors
      }
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  public void setFilename1(String filename) {
    this.filename1 = filename;
  }

  public String getFilename1() {
    return filename1;
  }

  public void setFilename2(String filename) {
    this.filename2 = filename;
  }

  public String getFilename2() {
    return filename2;
  }

  public boolean isAddFilenameToResult() {
    return addFilenameToResult;
  }

  public void setAddFilenameToResult(boolean addFilenameToResult) {
    this.addFilenameToResult = addFilenameToResult;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if ((!Utils.isEmpty(filename1)) && (!Utils.isEmpty(filename2))) {
      String realFilename1 = resolve(filename1);
      String realFilename2 = resolve(filename2);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realFilename1, ResourceType.FILE));
      reference.getEntries().add(new ResourceEntry(realFilename2, ResourceType.FILE));
      references.add(reference);
    }
    return references;
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
    ActionValidatorUtils.andValidator().validate(this, "filename1", remarks, ctx);
    ActionValidatorUtils.andValidator().validate(this, "filename2", remarks, ctx);
  }
}

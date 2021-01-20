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

package org.apache.hop.workflow.actions.deleteresultfilenames;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
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

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines a 'deleteresultfilenames' action. Its main use would be to create empty folder that
 * can be used to control the flow in ETL cycles.
 *
 * @author Samatar
 * @since 26-10-2007
 */
@Action(
    id = "DELETE_RESULT_FILENAMES",
    name = "i18n::ActionDeleteResultFilenames.Name",
    description = "i18n::ActionDeleteResultFilenames.Description",
    image = "DeleteResultFilenames.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileManagement",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/actions/deleteresultfilenames.html")
public class ActionDeleteResultFilenames extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionDeleteResultFilenames.class; // For Translator

  private String folderName;
  private boolean specifyWildcard;
  private String wildcard;
  private String wildcardExclude;

  public ActionDeleteResultFilenames(String n) {
    super(n, "");
    folderName = null;
    wildcardExclude = null;
    wildcard = null;
    specifyWildcard = false;
  }

  public ActionDeleteResultFilenames() {
    this("");
  }

  public Object clone() {
    ActionDeleteResultFilenames je = (ActionDeleteResultFilenames) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(100); // 75 chars in just tag names and spaces

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("foldername", folderName));
    retval.append("      ").append(XmlHandler.addTagValue("specify_wildcard", specifyWildcard));
    retval.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append("      ").append(XmlHandler.addTagValue("wildcardexclude", wildcardExclude));

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

    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "ActionDeleteResultFilenames.CanNotLoadFromXML", xe.getMessage()));
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

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);

    if (previousResult != null) {
      try {
        int size = previousResult.getResultFiles().size();
        if (log.isBasic()) {
          logBasic(
              BaseMessages.getString(PKG, "ActionDeleteResultFilenames.log.FilesFound", "" + size));
        }
        if (!specifyWildcard) {
          // Delete all files
          previousResult.getResultFiles().clear();
          if (log.isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionDeleteResultFilenames.log.DeletedFiles", "" + size));
          }
        } else {

          List<ResultFile> resultFiles = result.getResultFilesList();
          if (resultFiles != null && resultFiles.size() > 0) {
            for (Iterator<ResultFile> it = resultFiles.iterator();
                it.hasNext() && !parentWorkflow.isStopped(); ) {
              ResultFile resultFile = it.next();
              FileObject file = resultFile.getFile();
              if (file != null && file.exists()) {
                if (CheckFileWildcard(file.getName().getBaseName(), resolve(wildcard), true)
                    && !CheckFileWildcard(
                        file.getName().getBaseName(), resolve(wildcardExclude), false)) {
                  // Remove file from result files list
                  result.getResultFiles().remove(resultFile.getFile().toString());

                  if (log.isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG, "ActionDeleteResultFilenames.log.DeletedFile", file.toString()));
                  }
                }
              }
            }
          }
        }
        result.setResult(true);
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionDeleteResultFilenames.Error", e.toString()));
      }
    }
    return result;
  }

  /**********************************************************
   *
   * @param selectedfile
   * @param wildcard
   * @return True if the selectedfile matches the wildcard
   **********************************************************/
  private boolean CheckFileWildcard(String selectedfile, String wildcard, boolean include) {
    Pattern pattern = null;
    boolean getIt = include;

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

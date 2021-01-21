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

package org.apache.hop.workflow.actions.abort;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Action type to abort a workflow.
 *
 * @author Samatar
 * @since 12-02-2007
 */
@Action(
    id = "ABORT",
    name = "i18n::ActionAbort.Name",
    description = "i18n::ActionAbort.Description",
    image = "Abort.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/abort.html")
public class ActionAbort extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionAbort.class; // For Translator

  private String message;

  public ActionAbort(String name, String description) {
    super(name, description);
    message = null;
  }

  public ActionAbort() {
    this("", "");
  }

  public ActionAbort(ActionAbort other) {
    this("", "");
    this.message = other.message;
  }

  public Object clone() {
    return new ActionAbort(this);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(super.getXml());
    retval.append(XmlHandler.addTagValue("message", message));

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      message = XmlHandler.getTagValue(entrynode, "message");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionAbort.UnableToLoadFromXml.Label"), e);
    }
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param result The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result result, int nr) {
    try {
      String msg = resolve(getMessageAbort());

      if (msg == null) {
        msg = BaseMessages.getString(PKG, "ActionAbort.Meta.CheckResult.Label");
      }

      result.setNrErrors(1);
      result.setResult(false);
      logError(msg);
    } catch (Exception e) {
      result.setNrErrors(1);
      result.setResult(false);
      logError(
          BaseMessages.getString(PKG, "ActionAbort.Meta.CheckResult.CouldNotExecute")
              + e.toString());
    }

    // we fail so stop workflow execution
    parentWorkflow.stopExecution();
    return result;
  }

  @Override
  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  /**
   * Set the message to display in the log
   *
   * @param message
   */
  public void setMessageAbort(String message) {
    this.message = message;
  }

  /**
   * Get the message to display in the log
   *
   * @return the message
   */
  public String getMessageAbort() {
    return message;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.addOkRemark(this, "messageabort", remarks);
  }
}

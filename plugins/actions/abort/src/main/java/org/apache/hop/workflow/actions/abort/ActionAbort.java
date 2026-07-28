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

package org.apache.hop.workflow.actions.abort;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.xml.ILegacyXml;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.w3c.dom.Node;

/** Action type to abort a workflow. */
@Action(
    id = "ABORT",
    name = "i18n::ActionAbort.Name",
    description = "i18n::ActionAbort.Description",
    image = "Abort.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    keywords = "i18n::ActionAbort.keyword",
    documentationUrl = "/workflow/actions/abort.html")
public class ActionAbort extends ActionBase implements Cloneable, IAction, ILegacyXml {
  private static final Class<?> PKG = ActionAbort.class;

  @Setter
  @Getter
  @HopMetadataProperty(key = "message")
  private String messageAbort;

  @Setter
  @HopMetadataProperty(key = "loglevel", storeWithCode = true)
  private LogLevel messageLogLevel;

  public ActionAbort(String name, String description) {
    super(name, description);
    messageAbort = null;
    messageLogLevel = LogLevel.ERROR;
  }

  public ActionAbort() {
    this("", "");
  }

  public ActionAbort(ActionAbort other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.messageAbort = other.messageAbort;
    this.messageLogLevel = other.messageLogLevel;
  }

  @Override
  public Object clone() {
    return new ActionAbort(this);
  }

  /**
   * Backward compatible code.
   *
   * <p>Before the log level of the message became configurable, an "always_log_rows" flag decided
   * between minimal and error logging. That flag also determined whether the workflow was marked as
   * failed, which made the outcome of the abort depend on the action preceding it. Aborting now
   * always fails the workflow, the old flag only maps onto the log level of the message.
   */
  @Override
  public void convertLegacyXml(Node node) {
    if (XmlHandler.getTagValue(node, "loglevel") == null) {
      messageLogLevel =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "always_log_rows"))
              ? LogLevel.MINIMAL
              : LogLevel.ERROR;
    } else if (messageLogLevel == null) {
      messageLogLevel = LogLevel.ERROR;
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

      logMessage(msg);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionAbort.Meta.CheckResult.CouldNotExecute") + e);
    }

    // Aborting always fails the workflow, no matter what the previous action returned.
    //
    result.setNrErrors(1);
    result.setResult(false);

    // we fail so stop workflow execution
    parentWorkflow.stopExecution();
    return result;
  }

  /** Writes the abort message to the log with the configured log level. */
  private void logMessage(String msg) {
    switch (getMessageLogLevel()) {
      case ERROR -> logError(msg);
      case MINIMAL -> logMinimal(msg);
      case BASIC -> logBasic(msg);
      case DETAILED -> logDetailed(msg);
      case DEBUG -> logDebug(msg);
      case ROWLEVEL -> logRowlevel(msg);
      case NOTHING -> {
        // Don't log the message at all
      }
    }
  }

  @Override
  public boolean resetErrorsBeforeExecution() {
    // Leave the errors of the previous action alone, execute() sets the result of the abort itself.
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
   * Get the log level to write the message with
   *
   * @return the log level of the message
   */
  public LogLevel getMessageLogLevel() {
    return messageLogLevel == null ? LogLevel.ERROR : messageLogLevel;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.addOkRemark(this, "messageAbort", remarks);
  }
}

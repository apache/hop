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

package org.apache.hop.workflow.actions.msgboxinfo;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.gui.GuiFactory;
import org.apache.hop.core.gui.IThreadDialogs;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;

/** Action type to display a message box. */
@Action(
    id = "MSGBOX_INFO",
    name = "i18n::ActionMsgBoxInfo.Name",
    description = "i18n::ActionMsgBoxInfo.Description",
    image = "MsgBoxInfo.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionMsgBoxInfo.keyword",
    documentationUrl = "/workflow/actions/msgboxinfo.html")
@Getter
@Setter
public class ActionMsgBoxInfo extends ActionBase implements Cloneable, IAction {
  @HopMetadataProperty(key = "bodymessage")
  private String bodyMessage;

  @HopMetadataProperty(key = "titremessage")
  private String titleMessage;

  public ActionMsgBoxInfo(String n, String scr) {
    super(n, "");
    bodyMessage = null;
    titleMessage = null;
  }

  public ActionMsgBoxInfo() {
    this("", "");
  }

  @Override
  public Object clone() {
    ActionMsgBoxInfo je = (ActionMsgBoxInfo) super.clone();
    return je;
  }

  /** Display the Message Box. */
  public boolean evaluate(Result result) {
    try {
      // default to ok

      // Try to display MSGBOX
      boolean response = true;

      IThreadDialogs dialogs = GuiFactory.getThreadDialogs();
      if (dialogs != null) {
        response =
            dialogs.threadMessageBox(
                Const.NVL(getRealBodyMessage(), "") + Const.CR,
                Const.NVL(getRealTitleMessage(), ""),
                true,
                Const.INFO);
      }

      return response;

    } catch (Exception e) {
      result.setNrErrors(1);
      logError("Couldn't display message box: " + e.toString());
      return false;
    }
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param prevResult The result of the previous execution
   * @return The Result of the execution.
   */
  @Override
  public Result execute(Result prevResult, int nr) {
    prevResult.setResult(evaluate(prevResult));
    return prevResult;
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

  public String getRealTitleMessage() {
    return resolve(getTitleMessage());
  }

  public String getRealBodyMessage() {
    return resolve(getBodyMessage());
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.addOkRemark(this, "bodyMessage", remarks);
    ActionValidatorUtils.addOkRemark(this, "titleMessage", remarks);
  }
}

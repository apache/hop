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

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.GuiFactory;
import org.apache.hop.core.gui.IThreadDialogs;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Action type to display a message box.
 *
 * @author Samatar
 * @since 12-02-2007
 */
@Action(
    id = "MSGBOX_INFO",
    name = "i18n::ActionMsgBoxInfo.Name",
    description = "i18n::ActionMsgBoxInfo.Description",
    image = "MsgBoxInfo.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/msgboxinfo.html")
public class ActionMsgBoxInfo extends ActionBase implements Cloneable, IAction {
  private String bodymessage;
  private String titremessage;

  public ActionMsgBoxInfo(String n, String scr) {
    super(n, "");
    bodymessage = null;
    titremessage = null;
  }

  public ActionMsgBoxInfo() {
    this("", "");
  }

  public Object clone() {
    ActionMsgBoxInfo je = (ActionMsgBoxInfo) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(50);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("bodymessage", bodymessage));
    retval.append("      ").append(XmlHandler.addTagValue("titremessage", titremessage));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      bodymessage = XmlHandler.getTagValue(entrynode, "bodymessage");
      titremessage = XmlHandler.getTagValue(entrynode, "titremessage");
    } catch (Exception e) {
      throw new HopXmlException("Unable to load action of type 'Msgbox Info' from XML node", e);
    }
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
                getRealBodyMessage() + Const.CR, getRealTitleMessage(), true, Const.INFO);
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
  public Result execute(Result prevResult, int nr) {
    prevResult.setResult(evaluate(prevResult));
    return prevResult;
  }

  public boolean resetErrorsBeforeExecution() {
    // we should be able to evaluate the errors in
    // the previous action.
    return false;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public String getRealTitleMessage() {
    return resolve(getTitleMessage());
  }

  public String getRealBodyMessage() {
    return resolve(getBodyMessage());
  }

  public String getTitleMessage() {
    if (titremessage == null) {
      titremessage = "";
    }
    return titremessage;
  }

  public String getBodyMessage() {
    if (bodymessage == null) {
      bodymessage = "";
    }
    return bodymessage;
  }

  public void setBodyMessage(String s) {

    bodymessage = s;
  }

  public void setTitleMessage(String s) {

    titremessage = s;
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

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

package org.apache.hop.workflow;

import java.util.List;
import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.action.ActionMeta;
import org.w3c.dom.Node;

/** This class defines a hop from one action copy to another. */
public class WorkflowHopMeta extends BaseHopMeta<ActionMeta> implements Cloneable {
  private static final Class<?> PKG = WorkflowHopMeta.class;

  public static final String XML_EVALUATION_TAG = "evaluation";
  public static final String XML_UNCONDITIONAL_TAG = "unconditional";
  private static final String CONST_SPACE = "      ";

  private boolean evaluation;
  private boolean unconditional;

  public WorkflowHopMeta() {
    super(false, null, null, true, true, false);
  }

  public WorkflowHopMeta(WorkflowHopMeta hop) {
    super(
        hop.isSplit(),
        hop.getFromAction(),
        hop.getToAction(),
        hop.isEnabled(),
        hop.hasChanged(),
        hop.isErrorHop());
    evaluation = hop.evaluation;
    unconditional = hop.unconditional;
  }

  public WorkflowHopMeta(ActionMeta from, ActionMeta to) {
    this.from = from;
    this.to = to;
    enabled = true;
    split = false;
    evaluation = true;
    unconditional = false;

    if (from != null && from.isStart()) {
      setUnconditional();
    }
  }

  public WorkflowHopMeta(Node hopNode, List<ActionMeta> actions) throws HopXmlException {
    try {
      this.from = searchAction(actions, XmlHandler.getTagValue(hopNode, XML_FROM_TAG));
      this.to = searchAction(actions, XmlHandler.getTagValue(hopNode, XML_TO_TAG));
      this.enabled = getTagValueAsBoolean(hopNode, XML_ENABLED_TAG, true);
      this.evaluation = getTagValueAsBoolean(hopNode, XML_EVALUATION_TAG, true);
      this.unconditional = getTagValueAsBoolean(hopNode, XML_UNCONDITIONAL_TAG, false);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "WorkflowHopMeta.Exception.UnableToLoadHopInfo"), e);
    }
  }

  @Override
  public WorkflowHopMeta clone() {
    return new WorkflowHopMeta(this);
  }

  @Override
  public String toString() {
    String strFrom = (this.from == null) ? "(empty)" : this.from.getName();
    String strTo = (this.to == null) ? "(empty)" : this.to.getName();
    String strEnabled = enabled ? "enabled" : "disabled";
    String strEvaluation =
        unconditional ? XML_UNCONDITIONAL_TAG : evaluation ? "success" : "failure";
    return strFrom + " --> " + strTo + " [" + strEnabled + ", " + strEvaluation + ")";
  }

  private ActionMeta searchAction(List<ActionMeta> actions, String name) {
    for (ActionMeta action : actions) {
      if (action.getName().equalsIgnoreCase(name)) {
        return action;
      }
    }
    return null;
  }

  public WorkflowHopMeta(Node hopNode, WorkflowMeta workflow) throws HopXmlException {
    try {
      String fromName = XmlHandler.getTagValue(hopNode, XML_FROM_TAG);
      String toName = XmlHandler.getTagValue(hopNode, XML_TO_TAG);

      this.from = workflow.findAction(fromName);
      this.to = workflow.findAction(toName);
      this.enabled = getTagValueAsBoolean(hopNode, XML_ENABLED_TAG, true);
      this.evaluation = getTagValueAsBoolean(hopNode, XML_EVALUATION_TAG, true);
      this.unconditional = getTagValueAsBoolean(hopNode, XML_UNCONDITIONAL_TAG, false);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "WorkflowHopMeta.Exception.UnableToLoadHopInfoXML"), e);
    }
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder(200);
    if ((null != this.from) && (null != this.to)) {
      xml.append("    ").append(XmlHandler.openTag(XML_HOP_TAG)).append(Const.CR);
      xml.append(CONST_SPACE).append(XmlHandler.addTagValue(XML_FROM_TAG, this.from.getName()));
      xml.append(CONST_SPACE).append(XmlHandler.addTagValue(XML_TO_TAG, this.to.getName()));
      xml.append(CONST_SPACE).append(XmlHandler.addTagValue(XML_ENABLED_TAG, this.enabled));
      xml.append(CONST_SPACE).append(XmlHandler.addTagValue(XML_EVALUATION_TAG, this.evaluation));
      xml.append(CONST_SPACE)
          .append(XmlHandler.addTagValue(XML_UNCONDITIONAL_TAG, this.unconditional));
      xml.append("    ").append(XmlHandler.closeTag(XML_HOP_TAG)).append(Const.CR);
    }

    return xml.toString();
  }

  /**
   * @deprecated replaced by isEvaluation()
   * @see #isEvaluation()
   * @return the evaluation
   */
  @Deprecated(since = "2.10")
  public boolean getEvaluation() {
    return evaluation;
  }

  /**
   * @return the evaluation
   */
  @SuppressWarnings("java:S4144")
  public boolean isEvaluation() {
    return evaluation;
  }

  public void setEvaluation() {
    if (!evaluation) {
      setChanged();
    }
    setEvaluation(true);
  }

  public void setEvaluation(boolean e) {
    if (evaluation != e) {
      setChanged();
    }
    evaluation = e;
  }

  public void setUnconditional() {
    if (!unconditional) {
      setChanged();
    }
    unconditional = true;
  }

  public void setConditional() {
    if (unconditional) {
      setChanged();
    }
    unconditional = false;
  }

  public boolean isUnconditional() {
    return unconditional;
  }

  public String getDescription() {
    if (isUnconditional()) {
      return BaseMessages.getString(PKG, "WorkflowHopMeta.Msg.ExecNextActionUncondition");
    } else {
      if (isEvaluation()) {
        return BaseMessages.getString(PKG, "WorkflowHopMeta.Msg.ExecNextActionFlawLess");
      } else {
        return BaseMessages.getString(PKG, "WorkflowHopMeta.Msg.ExecNextActionFailed");
      }
    }
  }

  public ActionMeta getFromAction() {
    return this.from;
  }

  public void setFromAction(ActionMeta fromAction) {
    this.from = fromAction;
    changed = true;
  }

  public ActionMeta getToAction() {
    return this.to;
  }

  public void setToAction(ActionMeta toAction) {
    this.to = toAction;
    changed = true;
  }

  /**
   * @param unconditional the unconditional to set
   */
  public void setUnconditional(boolean unconditional) {
    if (this.unconditional != unconditional) {
      setChanged();
    }
    this.unconditional = unconditional;
  }
}

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

package org.apache.hop.workflow.actions.success;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Action type to success a workflow.
 *
 * @author Samatar
 * @since 12-02-2007
 */
@Action(
    id = "SUCCESS",
    name = "i18n::ActionSuccess.Name",
    description = "i18n::ActionSuccess.Description",
    image = "Success.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/success.html")
public class ActionSuccess extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSuccess.class; // For Translator

  public ActionSuccess(String n, String scr) {
    super(n, "");
  }

  public ActionSuccess() {
    this("", "");
  }

  public Object clone() {
    ActionSuccess je = (ActionSuccess) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append(super.getXml());

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionSuccess.Meta.UnableToLoadFromXML"), e);
    }
  }

  /**
   * Execute this action and return the result. In this case it means, just set the result boolean
   * in the Result class.
   *
   * @param previousResult The result of the previous execution
   * @return The Result of the execution.
   */
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setNrErrors(0);
    result.setResult(true);

    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {}
}

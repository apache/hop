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

package org.apache.hop.workflow.actions.repeat;

import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

@Action(
    id = "EndRepeat",
    name = "End Repeat",
    description = "End repeated execution of a workflow or a transformation",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    image = "endrepeat.svg",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/repeat-end.html")
public class EndRepeat extends ActionBase implements IAction, Cloneable {

  public EndRepeat(String name, String description) {
    super(name, description);
  }

  public EndRepeat() {
    this("", "");
  }

  /**
   * Simply set a flag in the parent workflow, this is also a success
   *
   * @param prevResult
   * @param nr
   * @return
   * @throws HopException
   */
  @Override
  public Result execute(Result prevResult, int nr) throws HopException {

    parentWorkflow.getExtensionDataMap().put(Repeat.REPEAT_END_LOOP, getName());

    // Force success.
    //
    prevResult.setResult(true);
    prevResult.setNrErrors(0);

    return prevResult;
  }

  @Override
  public EndRepeat clone() {
    return (EndRepeat) super.clone();
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append(super.getXml());
    return xml.toString();
  }

  public void loadXml( Node actionNode, IHopMetadataProvider metadataProvider, IVariables variables )
      throws HopXmlException {
    try {
      super.loadXml(actionNode);
    } catch (Exception e) {
      throw new HopXmlException("Unable to load End Repeat workflow entry metadata from XML", e);
    }
  }
}

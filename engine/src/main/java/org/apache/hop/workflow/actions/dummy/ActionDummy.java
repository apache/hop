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

package org.apache.hop.workflow.actions.dummy;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.w3c.dom.Node;

import java.util.List;

@Action(
    id = "DUMMY",
    image = "ui/images/dummy.svg",
    name = "i18n::ActionDummy.Name",
    description = "i18n::ActionDummy.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/dummy.html")
public class ActionDummy extends ActionBase implements IAction {

  public ActionDummy() {
    this("");
  }

  public ActionDummy(String name) {
     super(name, "");
  }
  
  public Result execute(Result prevResult, int nr) throws HopException {
    prevResult.setResult(true);
    prevResult.setNrErrors(0);

    return prevResult;
  }

  public String getXml() {
    return super.getXml();
  }

  public void loadXml(Node actionNode, IHopMetadataProvider metadataProvider, IVariables variables) throws HopXmlException {     
    super.loadXml(actionNode);
  }

  public void check(List<ICheckResult> remarks, WorkflowMeta workflowMeta, IVariables variables, IHopMetadataProvider metadataProvider) {
      
  }
}
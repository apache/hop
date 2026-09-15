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

package org.apache.hop.naming.gui;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.naming.engine.NamingSchemeCheckRemarks;
import org.apache.hop.workflow.CheckActionsExtension;
import org.apache.hop.workflow.WorkflowMeta;

/** Adds naming-scheme errors to workflow Verify (same rules as {@code hop naming-check}). */
@ExtensionPoint(
    id = "NamingSchemeAfterCheckActions",
    description = "Add naming-scheme remarks after workflow verify",
    extensionPointId = "AfterCheckActions")
public class NamingSchemeWorkflowCheckExtension implements IExtensionPoint<CheckActionsExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, CheckActionsExtension extension) throws HopException {
    if (extension == null || extension.getWorkflowMeta() == null) {
      return;
    }
    WorkflowMeta workflow = extension.getWorkflowMeta();
    String location = Const.NVL(workflow.getFilename(), Const.NVL(workflow.getName(), "workflow"));
    NamingSchemeCheckRemarks.addRemarks(
        workflow, location, extension.getRemarks(), extension.getMetadataProvider());
  }
}

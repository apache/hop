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

package org.apache.hop.lint;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;

/**
 * Re-lints every open editor after a Problems-tab force refresh.
 *
 * <p>The button clears the database cache and then checks the editor it belongs to. That check
 * fills the Problems tab, but it republishes lint markers only when lint is included in pipeline
 * verify, and a workflow check does not republish them. Linting every open editor here, including
 * the active one, replaces the markers from before the clear.
 */
@ExtensionPoint(
    id = "ProblemsForceRefreshLintExtension",
    extensionPointId = "ProblemsForceRefresh",
    description = "Re-lints every open editor after a Problems-tab force refresh")
public class ProblemsForceRefreshLintExtension implements IExtensionPoint<HopGuiAbstractGraph> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, HopGuiAbstractGraph active)
      throws HopException {
    BackgroundLintService.getInstance().refreshOpenEditorsExcept(active);
  }
}

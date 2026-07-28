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

package org.apache.hop.projects.xp;

import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.projects.resources.ResourceAttributes;
import org.apache.hop.projects.resources.SystemResourceCheckResult;
import org.apache.hop.projects.resources.SystemResourceChecker;
import org.apache.hop.projects.resources.SystemResourceRequirement;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

/**
 * When a project lifecycle environment is enabled, optionally validate JVM memory, free disk space
 * on local folders, and CPU cores against settings stored under {@link ResourceAttributes#GROUP}.
 */
@ExtensionPoint(
    id = "ResourceProjectEnvironmentAfterEnabled",
    description = "Validate system resource requirements when a lifecycle environment is enabled",
    extensionPointId = "HopProjectEnvironmentAfterEnabled")
public class ResourceProjectEnvironmentAfterEnabledExtensionPoint
    implements IExtensionPoint<AttributesContext> {

  private static final Class<?> PKG = ResourceAttributes.class;

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, AttributesContext context)
      throws HopException {
    if (context == null) {
      return;
    }

    String onEnable = ResourceAttributes.resolveOnEnable(context, context.getPurpose());
    if (ResourceAttributes.ON_ENABLE_OFF.equals(onEnable)) {
      log.logDetailed(
          "System resource check skipped (onEnable=off) for environment '"
              + Const.NVL(context.getEnvironmentName(), "")
              + "'");
      return;
    }

    SystemResourceRequirement requirement = ResourceAttributes.toRequirement(context);
    if (!requirement.hasAnyRequirement()) {
      log.logDetailed(
          "System resource check skipped (no thresholds) for environment '"
              + Const.NVL(context.getEnvironmentName(), "")
              + "'");
      return;
    }

    SystemResourceCheckResult result = SystemResourceChecker.checkLive(requirement, variables);
    String envName = Const.NVL(context.getEnvironmentName(), "");
    if (!result.hasViolations()) {
      log.logBasic(BaseMessages.getString(PKG, "ResourceLifecycleEnv.Check.Passed", envName));
      return;
    }

    String report =
        BaseMessages.getString(
            PKG, "ResourceLifecycleEnv.Check.Failed", envName, result.formatReport());

    if (ResourceAttributes.ON_ENABLE_ENFORCE.equals(onEnable)) {
      throw new HopException("FATAL: " + report);
    }

    // warn
    log.logError(report);
    if ("GUI".equalsIgnoreCase(Const.getHopPlatformRuntime())) {
      try {
        MessageBox box = new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_WARNING);
        box.setText(BaseMessages.getString(PKG, "ResourceLifecycleEnv.Check.Title"));
        box.setMessage(report);
        box.open();
      } catch (Exception e) {
        // headless or shell unavailable
      }
    }
  }
}

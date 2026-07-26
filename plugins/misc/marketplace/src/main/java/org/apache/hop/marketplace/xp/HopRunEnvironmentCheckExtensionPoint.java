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

package org.apache.hop.marketplace.xp;

import java.nio.file.Path;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.EnvironmentDrift;
import org.apache.hop.marketplace.env.HopEnvironmentLoader;
import org.apache.hop.marketplace.env.HopEnvironmentSpec;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.run.HopRunBase;

/**
 * Optional drift check at hop-run start when a hop-env file is present and enforcement is enabled
 * ({@code enforceOnRun: true} in the file, or {@code -Dhop.env.enforce=true}).
 */
@ExtensionPoint(
    id = "HopRunEnvironmentCheckExtensionPoint",
    description = "Validate hop-env.yaml against the local plugin install before hop-run executes",
    extensionPointId = "HopRunStart")
public class HopRunEnvironmentCheckExtensionPoint implements IExtensionPoint<HopRunBase> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, HopRunBase hopRun)
      throws HopException {
    Path hopHome;
    try {
      hopHome = HopHome.resolve();
    } catch (HopException e) {
      // hop-run from unusual cwd: skip silently
      return;
    }

    String explicit = variables != null ? variables.getVariable("HOP_ENV_FILE") : null;
    if (StringUtils.isBlank(explicit)) {
      explicit = System.getProperty("hop.env.file");
    }
    Path envFile = EnvironmentApplier.resolveEnvironmentFile(hopHome, explicit);
    if (envFile == null) {
      return;
    }

    HopEnvironmentSpec env = HopEnvironmentLoader.load(envFile);
    boolean enforce =
        env.isEnforceOnRun()
            || "true".equalsIgnoreCase(System.getProperty("hop.env.enforce", "false"))
            || "Y".equalsIgnoreCase(System.getProperty("hop.env.enforce", "N"));
    if (!enforce) {
      log.logDetailed(
          "Found environment file "
              + envFile
              + " (enforceOnRun=false); skipping drift check. Use hop marketplace validate -f "
              + envFile.getFileName());
      return;
    }

    EnvironmentApplier applier = new EnvironmentApplier(log, hopHome, MarketplaceConfig.load());
    EnvironmentDrift drift = applier.validate(env);
    // For enforceOnRun we care about missing plugins/deps and version mismatches, not extras
    boolean hard =
        !drift.getMissingPlugins().isEmpty()
            || !drift.getVersionMismatches().isEmpty()
            || !drift.getMissingDependencies().isEmpty();
    if (hard) {
      throw new HopException(
          "FATAL: Environment drift detected against "
              + envFile
              + ":\n"
              + drift.formatReport()
              + "Run 'hop marketplace apply -f "
              + envFile
              + "' to fix your environment.");
    }
    log.logBasic("Environment file " + envFile + " matches local install.");
  }
}

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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
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
import org.apache.hop.marketplace.env.MarketplaceAttributes;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

/**
 * When a project lifecycle environment is enabled, optionally validate (or apply) the marketplace
 * hop-env file against the local install. Settings come from {@link AttributesContext} group {@link
 * MarketplaceAttributes#GROUP} — no dependency on the Projects plugin classes.
 */
@ExtensionPoint(
    id = "MarketplaceProjectEnvironmentAfterEnabled",
    description =
        "Validate hop-env against the local install when a lifecycle environment is enabled",
    extensionPointId = "HopProjectEnvironmentAfterEnabled")
public class ProjectEnvironmentAfterEnabledExtensionPoint
    implements IExtensionPoint<AttributesContext> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, AttributesContext context)
      throws HopException {
    if (context == null) {
      return;
    }

    String onEnable = MarketplaceAttributes.resolveOnEnable(context, context.getPurpose());
    if (MarketplaceAttributes.ON_ENABLE_OFF.equals(onEnable)) {
      log.logDetailed(
          "Marketplace environment check skipped (onEnable=off) for environment '"
              + Const.NVL(context.getEnvironmentName(), "")
              + "'");
      return;
    }

    Path hopHome;
    try {
      hopHome = HopHome.resolve();
    } catch (HopException e) {
      log.logDetailed("Marketplace environment check skipped: Hop home not resolved");
      return;
    }

    Path envFile = resolveEnvFile(context, variables, hopHome);
    if (envFile == null) {
      String msg =
          "Marketplace environment file not found for environment '"
              + Const.NVL(context.getEnvironmentName(), "")
              + "'. Set marketplace attribute envFile or place hop-env.yaml under the project home.";
      if (MarketplaceAttributes.ON_ENABLE_ENFORCE.equals(onEnable)) {
        throw new HopException(msg);
      }
      log.logBasic(msg);
      return;
    }

    try {
      MarketplaceConfig config = MarketplaceConfig.load();
      HopEnvironmentSpec env = HopEnvironmentLoader.load(envFile);
      EnvironmentApplier applier = new EnvironmentApplier(log, hopHome, config);
      EnvironmentDrift drift = applier.validate(env);

      if (MarketplaceAttributes.isStrict(context)) {
        populateExtraPlugins(hopHome, env, drift);
      }

      boolean hard =
          !drift.getMissingPlugins().isEmpty()
              || !drift.getVersionMismatches().isEmpty()
              || !drift.getMissingDependencies().isEmpty()
              || (MarketplaceAttributes.isStrict(context)
                  && !drift.getExtraMarketplacePlugins().isEmpty());

      if (!hard) {
        log.logBasic("Marketplace environment file " + envFile + " matches local install.");
        return;
      }

      String report =
          "Environment drift for '"
              + Const.NVL(context.getEnvironmentName(), "")
              + "' against "
              + envFile
              + ":\n"
              + drift.formatReport()
              + "Run 'hop marketplace apply -f "
              + envFile
              + "' to fix your environment.";

      if (MarketplaceAttributes.isAutoApply(context) && config.isEnabled()) {
        log.logBasic("Auto-applying marketplace environment file " + envFile);
        applier.apply(env, false);
        return;
      }

      if (MarketplaceAttributes.ON_ENABLE_ENFORCE.equals(onEnable)) {
        throw new HopException("FATAL: " + report);
      }

      // warn
      log.logError(report);
      if ("GUI".equalsIgnoreCase(Const.getHopPlatformRuntime())) {
        try {
          MessageBox box =
              new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_WARNING);
          box.setText("Marketplace environment drift");
          box.setMessage(report);
          box.open();
        } catch (Exception e) {
          // headless or shell unavailable
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to validate marketplace environment file " + envFile, e);
    }
  }

  private static Path resolveEnvFile(
      AttributesContext context, IVariables variables, Path hopHome) {
    String explicit = MarketplaceAttributes.envFile(context);
    if (StringUtils.isNotBlank(explicit)) {
      String resolved = variables != null ? variables.resolve(explicit.trim()) : explicit.trim();
      Path path = Path.of(resolved).toAbsolutePath().normalize();
      if (Files.isRegularFile(path)) {
        return path;
      }
    }

    // Project home hop-env.yaml
    if (StringUtils.isNotBlank(context.getProjectHome())) {
      Path candidate = Path.of(context.getProjectHome()).resolve("hop-env.yaml");
      if (Files.isRegularFile(candidate)) {
        return candidate.toAbsolutePath().normalize();
      }
      candidate = Path.of(context.getProjectHome()).resolve("hop-env.yml");
      if (Files.isRegularFile(candidate)) {
        return candidate.toAbsolutePath().normalize();
      }
    }

    String varFile = variables != null ? variables.getVariable("HOP_ENV_FILE") : null;
    return EnvironmentApplier.resolveEnvironmentFile(hopHome, varFile);
  }

  private static void populateExtraPlugins(
      Path hopHome, HopEnvironmentSpec env, EnvironmentDrift drift) throws Exception {
    Set<String> desired = new HashSet<>();
    if (env.getPlugins() != null) {
      for (HopEnvironmentSpec.PluginRef ref : env.getPlugins()) {
        if (ref.getArtifactId() != null) {
          desired.add(ref.getArtifactId());
        }
      }
    }
    Path receipts = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
    if (!Files.isDirectory(receipts)) {
      return;
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(receipts, "*.json")) {
      for (Path f : stream) {
        String name = f.getFileName().toString();
        String id = name.substring(0, name.length() - ".json".length());
        if (!desired.contains(id)) {
          drift.getExtraMarketplacePlugins().add(id);
        }
      }
    }
  }
}

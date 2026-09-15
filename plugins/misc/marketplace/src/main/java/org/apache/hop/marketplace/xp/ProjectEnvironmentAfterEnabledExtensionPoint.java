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
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.AttributesContext;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.Permission;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.EnvironmentDrift;
import org.apache.hop.marketplace.env.HopInstallSpec;
import org.apache.hop.marketplace.env.HopInstallSpecFiles;
import org.apache.hop.marketplace.env.HopInstallSpecLoader;
import org.apache.hop.marketplace.env.MarketplaceAttributes;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;

/**
 * When a project lifecycle environment is enabled, optionally validate (or apply) the marketplace
 * hop-env file against the local install.
 *
 * <p><strong>Hop GUI:</strong> this extension <em>never</em> throws. Missing config, missing
 * hop-env, parse errors, and drift only log (and optionally show a warning dialog). Blocking enable
 * would trap users out of their environment with no GUI recovery path (issue #7656).
 *
 * <p><strong>Non-GUI:</strong> {@code onEnable=enforce} may still throw so automation can hard-fail
 * (prefer {@code hop marketplace validate} / hop-run checks for CI).
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
    // Outer guard: never let marketplace break environment enable in the GUI.
    try {
      runCheck(log, variables, context);
    } catch (Exception e) {
      if (isGuiRuntime()) {
        if (log != null) {
          log.logError(
              "Marketplace environment check failed but environment enable continues: "
                  + e.getMessage(),
              e);
        }
        warnGui(
            "Marketplace check failed",
            "The marketplace could not validate hop-env for this environment.\n\n"
                + Const.NVL(e.getMessage(), e.getClass().getSimpleName())
                + "\n\nThe environment was still enabled.");
        return;
      }
      if (e instanceof HopException he) {
        throw he;
      }
      throw new HopException("Marketplace environment check failed", e);
    }
  }

  private void runCheck(ILogChannel log, IVariables variables, AttributesContext context)
      throws Exception {
    if (context == null) {
      return;
    }

    String onEnable = MarketplaceAttributes.resolveOnEnable(context, context.getPurpose());
    if (MarketplaceAttributes.ON_ENABLE_OFF.equals(onEnable)) {
      if (log != null) {
        log.logDetailed(
            "Marketplace environment check skipped (onEnable=off) for environment '"
                + Const.NVL(context.getEnvironmentName(), "")
                + "'");
      }
      return;
    }

    Path hopHome;
    try {
      hopHome = HopHome.resolve();
    } catch (HopException e) {
      if (log != null) {
        log.logDetailed("Marketplace environment check skipped: Hop home not resolved");
      }
      return;
    }

    boolean explicitEnvFile = MarketplaceAttributes.hasExplicitEnvFile(context);
    Path envFile = resolveEnvFile(context, variables, hopHome);
    if (envFile == null) {
      // No hop-env configured and none found: silent skip (issue #7656).
      if (!explicitEnvFile) {
        if (log != null) {
          log.logDetailed(
              "Marketplace environment check skipped (no envFile attribute and no hop-env.yaml) for '"
                  + Const.NVL(context.getEnvironmentName(), "")
                  + "'");
        }
        return;
      }
      String msg =
          "Marketplace environment file not found for environment '"
              + Const.NVL(context.getEnvironmentName(), "")
              + "': "
              + Const.NVL(MarketplaceAttributes.envFile(context), "")
              + ". Fix marketplace attribute envFile or place hop-env.yaml under the project home.";
      // GUI: never block open — warn only. Non-GUI enforce may throw.
      if (MarketplaceAttributes.ON_ENABLE_ENFORCE.equals(onEnable) && !isGuiRuntime()) {
        throw new HopException(msg);
      }
      if (log != null) {
        log.logBasic(msg);
      }
      if (isGuiRuntime()) {
        warnGui("Marketplace environment file missing", msg);
      }
      return;
    }

    MarketplaceConfig config = MarketplaceConfig.load();
    HopInstallSpec env = HopInstallSpecLoader.load(envFile);
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
      if (log != null) {
        log.logBasic("Marketplace environment file " + envFile + " matches local install.");
      }
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
      if (!HopSecurity.allows(Permission.PLUGIN_MANAGE)) {
        String denied =
            "Marketplace auto-apply skipped: session lacks plugin.manage (administrator). "
                + report
                + "\nSign in as an administrator or run 'hop marketplace apply -f "
                + envFile
                + "' on the server.";
        if (log != null) {
          log.logBasic(denied);
        }
        if (isGuiRuntime()) {
          warnGui("Marketplace auto-apply not permitted", denied);
        }
        return;
      }
      if (log != null) {
        log.logBasic("Auto-applying marketplace environment file " + envFile);
      }
      try {
        applier.apply(env, false);
      } catch (Exception applyEx) {
        // Auto-apply must not strand the GUI without an open environment.
        if (isGuiRuntime()) {
          if (log != null) {
            log.logError("Marketplace auto-apply failed; environment remains enabled", applyEx);
          }
          warnGui(
              "Marketplace auto-apply failed",
              report
                  + "\n\nAuto-apply error: "
                  + Const.NVL(applyEx.getMessage(), applyEx.getClass().getSimpleName())
                  + "\n\nThe environment was still enabled.");
          return;
        }
        throw applyEx;
      }
      return;
    }

    if (MarketplaceAttributes.ON_ENABLE_ENFORCE.equals(onEnable) && !isGuiRuntime()) {
      throw new HopException("FATAL: " + report);
    }

    // warn (and in GUI always warn-only, even if onEnable was enforce)
    if (log != null) {
      log.logError(report);
      if (MarketplaceAttributes.ON_ENABLE_ENFORCE.equals(onEnable) && isGuiRuntime()) {
        log.logBasic(
            "Marketplace onEnable=enforce is treated as warn in Hop GUI so environments can always"
                + " be opened. Use hop-run / marketplace validate for hard fail.");
      }
    }
    if (isGuiRuntime()) {
      warnGui("Marketplace environment drift", report);
    }
  }

  static boolean isGuiRuntime() {
    return "GUI".equalsIgnoreCase(Const.getHopPlatformRuntime());
  }

  private static void warnGui(String title, String message) {
    try {
      MessageBox box = new MessageBox(HopGui.getInstance().getShell(), SWT.OK | SWT.ICON_WARNING);
      box.setText(title);
      box.setMessage(message);
      box.open();
    } catch (Exception e) {
      // headless or shell unavailable
    }
  }

  private static Path resolveEnvFile(
      AttributesContext context, IVariables variables, Path hopHome) {
    String explicit = MarketplaceAttributes.envFile(context);
    if (StringUtils.isNotBlank(explicit)) {
      // A relative reference belongs to the project, not to the Hop install (issue #8012).
      Path found =
          existingSpecPath(
              HopInstallSpecFiles.resolveInProject(explicit, variables, context.getProjectHome()),
              variables);
      if (found != null) {
        return found;
      }
    }

    // Project home hop-env.yaml
    if (StringUtils.isNotBlank(context.getProjectHome())) {
      Path yaml = existingSpecPath(context.getProjectHome() + "/hop-env.yaml", variables);
      if (yaml != null) {
        return yaml;
      }
      Path yml = existingSpecPath(context.getProjectHome() + "/hop-env.yml", variables);
      if (yml != null) {
        return yml;
      }
    }

    String varFile = variables != null ? variables.getVariable("HOP_ENV_FILE") : null;
    return EnvironmentApplier.resolveEnvironmentFile(hopHome, varFile);
  }

  private static Path existingSpecPath(String filename, IVariables variables) {
    if (!HopInstallSpecFiles.exists(filename, variables)) {
      return null;
    }
    try {
      String resolved = HopInstallSpecFiles.resolve(filename, variables);
      FileObject fileObject = HopVfs.getFileObject(resolved, variables);
      String local = HopVfs.getFilename(fileObject);
      if (StringUtils.isNotBlank(local)) {
        Path path = Path.of(local);
        if (Files.isRegularFile(path)) {
          return path.toAbsolutePath().normalize();
        }
      }
    } catch (Exception e) {
      // Fall through to java.nio for local paths that Path.of can still see.
    }
    try {
      String resolved = HopInstallSpecFiles.resolve(filename, variables);
      Path path = Path.of(resolved).toAbsolutePath().normalize();
      return Files.isRegularFile(path) ? path : null;
    } catch (Exception e) {
      return null;
    }
  }

  private static void populateExtraPlugins(Path hopHome, HopInstallSpec env, EnvironmentDrift drift)
      throws Exception {
    Set<String> desired = new HashSet<>();
    if (env.getPlugins() != null) {
      for (HopInstallSpec.PluginRef ref : env.getPlugins()) {
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

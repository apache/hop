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

package org.apache.hop.marketplace.command;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Getter
@Setter
@Command(
    name = "marketplace",
    mixinStandardHelpOptions = true,
    description = "Install and manage optional Hop plugins from a Maven repository",
    subcommands = {
      MarketplaceCommand.InstallCommand.class,
      MarketplaceCommand.UninstallCommand.class,
      MarketplaceCommand.ListCommand.class
    })
@HopCommand(id = "marketplace", description = "Hop plugin marketplace")
public class MarketplaceCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {

  private ILogChannel log;
  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("Marketplace");
    // Wire nested subcommands with shared context
    for (CommandLine sub : cmd.getSubcommands().values()) {
      Object userObject = sub.getCommand();
      if (userObject instanceof MarketplaceSubCommand nested) {
        nested.init(log, variables);
      }
    }
  }

  @Override
  public void run() {
    cmd.usage(System.out);
  }

  static String resolveDefaultVersion(MarketplaceConfig config) {
    if (StringUtils.isNotBlank(config.getDefaultVersion())) {
      return config.getDefaultVersion();
    }
    String[] versions = new HopVersionProvider().getVersion();
    if (versions != null && versions.length > 0 && StringUtils.isNotBlank(versions[0])) {
      return versions[0];
    }
    // Development builds often have null Implementation-Version
    return System.getProperty("hop.version", "2.19.0-SNAPSHOT");
  }

  abstract static class MarketplaceSubCommand implements Runnable {
    protected ILogChannel log;
    protected IVariables variables;

    void init(ILogChannel log, IVariables variables) {
      this.log = log;
      this.variables = variables;
    }
  }

  @Command(name = "install", description = "Download and install a plugin zip by Maven coordinates")
  static class InstallCommand extends MarketplaceSubCommand {
    @Parameters(
        index = "0",
        paramLabel = "COORDINATE",
        description =
            "artifactId, artifactId:version, or groupId:artifactId:version (e.g. hop-tech-parquet)")
    private String coordinate;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        if (!config.isEnabled()) {
          throw new HopException("Marketplace is disabled in hop-config.json");
        }
        Path hopHome = HopHome.resolve();
        // Activate any previously staged plugins first
        new PluginInstaller(log, hopHome, config).activateAllPending();

        MavenCoordinates gav =
            MavenCoordinates.parse(coordinate, config.getGroupId(), resolveDefaultVersion(config));
        new PluginInstaller(log, hopHome, config).install(gav, true);
        System.out.println(
            "Plugin " + gav.gav() + " installed under " + hopHome + ". Restart Hop to load it.");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        if (log != null) {
          log.logError("Marketplace install failed", e);
        }
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "install failed" : e.getMessage(), e);
      }
    }
  }

  @Command(
      name = "uninstall",
      description = "Remove a plugin previously installed via the marketplace")
  static class UninstallCommand extends MarketplaceSubCommand {
    @Parameters(index = "0", paramLabel = "ARTIFACT_ID", description = "e.g. hop-tech-parquet")
    private String artifactId;

    @Override
    public void run() {
      try {
        Path hopHome = HopHome.resolve();
        new PluginUninstaller(log, hopHome).uninstall(artifactId);
        System.out.println("Uninstalled " + artifactId + ". Restart Hop.");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "uninstall failed" : e.getMessage(), e);
      }
    }
  }

  @Command(name = "list", description = "List marketplace-installed plugins (receipts)")
  static class ListCommand extends MarketplaceSubCommand {
    @Override
    public void run() {
      try {
        Path hopHome = HopHome.resolve();
        Path receipts = hopHome.resolve(PluginInstaller.RECEIPTS_DIR);
        if (!Files.isDirectory(receipts)) {
          System.out.println("No marketplace-installed plugins.");
          return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(receipts, "*.json")) {
          boolean any = false;
          for (Path file : stream) {
            any = true;
            InstallReceipt receipt =
                org.apache.hop.core.json.HopJson.newMapper()
                    .readValue(file.toFile(), InstallReceipt.class);
            System.out.printf(
                "%s  %s:%s:%s%s%n",
                receipt.getArtifactId(),
                receipt.getGroupId(),
                receipt.getArtifactId(),
                receipt.getVersion(),
                receipt.isPendingActivation() ? " (pending activation)" : "");
          }
          if (!any) {
            System.out.println("No marketplace-installed plugins.");
          }
        }
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "list failed" : e.getMessage(), e);
      }
    }
  }
}

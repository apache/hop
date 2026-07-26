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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.marketplace.catalog.OptionalPluginCatalog;
import org.apache.hop.marketplace.catalog.OptionalPluginInfo;
import org.apache.hop.marketplace.catalog.PluginDiscovery;
import org.apache.hop.marketplace.config.MarketplaceConfig;
import org.apache.hop.marketplace.config.MarketplaceRepository;
import org.apache.hop.marketplace.config.MarketplaceRepositoryDefinition;
import org.apache.hop.marketplace.env.EnvironmentApplier;
import org.apache.hop.marketplace.env.EnvironmentDrift;
import org.apache.hop.marketplace.env.HopEnvironmentLoader;
import org.apache.hop.marketplace.env.HopEnvironmentSpec;
import org.apache.hop.marketplace.install.HopHome;
import org.apache.hop.marketplace.install.InstallReceipt;
import org.apache.hop.marketplace.install.PluginInstaller;
import org.apache.hop.marketplace.install.PluginUninstaller;
import org.apache.hop.marketplace.resolve.MavenCoordinates;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
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
      MarketplaceCommand.ListCommand.class,
      MarketplaceCommand.QueryCommand.class,
      MarketplaceCommand.ApplyCommand.class,
      MarketplaceCommand.ValidateCommand.class,
      MarketplaceCommand.RepoCommand.class
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
    // Wire nested subcommands (including marketplace repo *) with shared context
    wireSubcommands(cmd, log, variables);
  }

  private static void wireSubcommands(
      CommandLine commandLine, ILogChannel log, IVariables variables) {
    for (CommandLine sub : commandLine.getSubcommands().values()) {
      Object userObject = sub.getCommand();
      if (userObject instanceof MarketplaceSubCommand nested) {
        nested.init(log, variables);
      }
      wireSubcommands(sub, log, variables);
    }
  }

  @Override
  public void run() {
    cmd.usage(System.out);
  }

  public static String resolveDefaultVersion(MarketplaceConfig config) {
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

  @Command(
      mixinStandardHelpOptions = true,
      name = "install",
      description =
          "Download and install a plugin zip. Short names resolve via the same discovery as query"
              + " (e.g. datavault → hop-datavault:0.4.0-SNAPSHOT from a browse repo).")
  static class InstallCommand extends MarketplaceSubCommand {
    @Parameters(
        index = "0",
        paramLabel = "COORDINATE",
        description =
            "Short name, artifactId, artifactId:version, or groupId:artifactId:version"
                + " (e.g. datavault, hop-tech-parquet, hop-datavault:0.4.0-SNAPSHOT)")
    private String coordinate;

    @Option(
        names = {"--repo"},
        description =
            "Use only this repository id (skip fallback chain). Default: prefer discovery source,"
                + " then primary and other enabled repos.")
    private String repoId;

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

        PluginDiscovery.InstallTarget target =
            PluginDiscovery.resolveInstall(
                coordinate, config.getGroupId(), resolveDefaultVersion(config), config, log);
        MavenCoordinates gav = target.coordinates();
        if (StringUtils.isNotBlank(target.preferredRepoId()) && StringUtils.isBlank(repoId)) {
          System.out.println(
              "Resolved "
                  + coordinate
                  + " → "
                  + gav.gav()
                  + " (prefer repo '"
                  + target.preferredRepoId()
                  + "')");
        } else if (!coordinate.trim().equals(gav.artifactId()) || !coordinate.contains(":")) {
          System.out.println("Resolved " + coordinate + " → " + gav.gav());
        }
        InstallReceipt receipt =
            new PluginInstaller(log, hopHome, config)
                .install(gav, true, repoId, target.preferredRepoId());
        System.out.println(
            "Plugin "
                + gav.gav()
                + " installed under "
                + hopHome
                + (receipt.getRepositoryId() != null
                    ? " from repo '" + receipt.getRepositoryId() + "'"
                    : "")
                + ". Restart Hop to load it.");
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
      mixinStandardHelpOptions = true,
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

  @Command(
      mixinStandardHelpOptions = true,
      name = "list",
      description = "List marketplace-installed plugins (receipts)")
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
                HopJson.newMapper().readValue(file.toFile(), InstallReceipt.class);
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

  @Command(
      mixinStandardHelpOptions = true,
      name = "query",
      description =
          "Search plugins: bundled Apache optional catalog plus every enabled repository with"
              + " browse=true (live Nexus zip list). Filter is a case-insensitive substring."
              + " Default output is an ASCII table; use --csv for machine-readable CSV.")
  static class QueryCommand extends MarketplaceSubCommand {
    private static final int TABLE_DESCRIPTION_MAX = 56;

    @Parameters(
        index = "0",
        arity = "0..1",
        paramLabel = "FILTER",
        description =
            "Case-insensitive substring to match. Omit or use empty string to list matches.")
    private String filter;

    @Option(
        names = {"--repo"},
        description =
            "Only include this repository id among browse-enabled remotes (Apache catalog still"
                + " included).")
    private String repoId;

    @Option(
        names = {"--csv"},
        description = "Print results as CSV (header + rows) instead of an ASCII table")
    private boolean csv;

    @Option(
        names = {"--include-gav"},
        description = "Include a GAV (groupId:artifactId:version) column in the output")
    private boolean includeGav;

    @Override
    public void run() {
      try {
        Path hopHome = null;
        try {
          hopHome = HopHome.resolve();
        } catch (Exception ignored) {
          // installed status is optional
        }
        MarketplaceConfig config = MarketplaceConfig.load();
        List<OptionalPluginInfo> matches = PluginDiscovery.query(filter, repoId, config, log);

        List<String> headers = queryHeaders(includeGav);
        List<List<String>> rows = new ArrayList<>();
        for (OptionalPluginInfo info : matches) {
          rows.add(queryRow(info, hopHome, includeGav, csv));
        }

        if (csv) {
          // Always emit header so scripts get stable columns even when empty
          CliTable.printCsv(System.out, headers, rows);
          return;
        }

        if (matches.isEmpty()) {
          System.out.println(
              "No plugins match filter: " + (StringUtils.isBlank(filter) ? "(all)" : filter));
          return;
        }
        CliTable.printTable(System.out, headers, rows);
        System.out.println(matches.size() + " plugin(s) matched.");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "query failed" : e.getMessage(), e);
      }
    }

    static List<String> queryHeaders(boolean includeGav) {
      List<String> headers = new ArrayList<>();
      headers.add("ARTIFACT");
      headers.add("VERSION");
      headers.add("CATEGORY");
      headers.add("SOURCE");
      if (includeGav) {
        headers.add("GAV");
      }
      headers.add("INSTALLED");
      headers.add("UPDATED");
      headers.add("DESCRIPTION");
      return headers;
    }

    static List<String> queryRow(
        OptionalPluginInfo info, Path hopHome, boolean includeGav, boolean fullDescription) {
      List<String> row = new ArrayList<>();
      row.add(Const.NVL(info.getArtifactId(), ""));
      row.add(Const.NVL(info.getVersion(), ""));
      row.add(Const.NVL(info.getCategory(), ""));
      row.add(Const.NVL(info.getSource(), "apache"));
      if (includeGav) {
        row.add(formatGav(info));
      }
      boolean onDisk = hopHome != null && OptionalPluginCatalog.isInstalledOnDisk(hopHome, info);
      row.add(onDisk ? "yes" : "");
      row.add(Const.NVL(info.getLastUpdated(), ""));
      String description = Const.NVL(info.getDescription(), "");
      if (!fullDescription) {
        description = CliTable.truncate(description, TABLE_DESCRIPTION_MAX);
      }
      row.add(description);
      return row;
    }

    static String formatGav(OptionalPluginInfo info) {
      String group =
          StringUtils.isNotBlank(info.getGroupId()) ? info.getGroupId() : "org.apache.hop";
      String artifact = Const.NVL(info.getArtifactId(), "");
      String version = StringUtils.isNotBlank(info.getVersion()) ? info.getVersion() : "?";
      return group + ":" + artifact + ":" + version;
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "apply",
      description =
          "Install plugins and dependencies declared in hop-env.yaml (or .json). Optional --prune"
              + " removes marketplace plugins not listed in the file.")
  static class ApplyCommand extends MarketplaceSubCommand {
    @Option(
        names = {"-f", "--file"},
        required = true,
        description = "Path to hop-env.yaml or hop-env.json")
    private String file;

    @Option(
        names = {"--prune"},
        description =
            "Uninstall marketplace-installed plugins that are not listed in the environment file")
    private boolean prune;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        if (!config.isEnabled()) {
          throw new HopException("Marketplace is disabled in hop-config.json");
        }
        Path hopHome = HopHome.resolve();
        Path envPath = Path.of(file).toAbsolutePath().normalize();
        HopEnvironmentSpec env = HopEnvironmentLoader.load(envPath);
        new EnvironmentApplier(log, hopHome, config).apply(env, prune);
        System.out.println(
            "Environment applied from "
                + envPath
                + ". Restart Hop (or re-run) so new plugins are loaded.");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        if (log != null) {
          log.logError("Marketplace apply failed", e);
        }
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "apply failed" : e.getMessage(), e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "validate",
      description =
          "Check the local install against hop-env.yaml without installing. Exit code 1 on drift.")
  static class ValidateCommand extends MarketplaceSubCommand {
    @Option(
        names = {"-f", "--file"},
        description =
            "Path to hop-env.yaml or hop-env.json (default: discover hop-env.* under project/Hop"
                + " home)")
    private String file;

    @Option(
        names = {"--strict"},
        description = "Also fail if extra marketplace plugins are installed beyond the env file")
    private boolean strict;

    @Override
    public void run() {
      try {
        Path hopHome = HopHome.resolve();
        Path envPath = EnvironmentApplier.resolveEnvironmentFile(hopHome, file);
        if (envPath == null) {
          throw new HopException(
              "No environment file found. Pass -f hop-env.yaml or set HOP_ENV_FILE.");
        }
        HopEnvironmentSpec env = HopEnvironmentLoader.load(envPath);
        EnvironmentDrift drift =
            new EnvironmentApplier(log, hopHome, MarketplaceConfig.load()).validate(env);
        boolean hard =
            !drift.getMissingPlugins().isEmpty()
                || !drift.getVersionMismatches().isEmpty()
                || !drift.getMissingDependencies().isEmpty()
                || (strict && !drift.getExtraMarketplacePlugins().isEmpty());
        // populate extras only when strict (validate() currently does not; add here)
        if (strict) {
          addExtraPlugins(hopHome, env, drift);
          hard = hard || !drift.getExtraMarketplacePlugins().isEmpty();
        }
        if (!hard) {
          System.out.println("OK: environment matches " + envPath);
          return;
        }
        System.err.println("Environment drift against " + envPath + ":");
        System.err.print(drift.formatReport());
        System.err.println("Run: hop marketplace apply -f " + envPath);
        throw new CommandLine.ExecutionException(new CommandLine(this), "environment drift");
      } catch (CommandLine.ExecutionException e) {
        throw e;
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "validate failed" : e.getMessage(), e);
      }
    }

    private static void addExtraPlugins(
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

  @Command(
      mixinStandardHelpOptions = true,
      name = "repo",
      description = "Manage marketplace Maven repositories in hop-config.json",
      subcommands = {
        MarketplaceCommand.RepoListCommand.class,
        MarketplaceCommand.RepoAddCommand.class,
        MarketplaceCommand.RepoRemoveCommand.class,
        MarketplaceCommand.RepoSetPrimaryCommand.class,
        MarketplaceCommand.RepoEnableCommand.class,
        MarketplaceCommand.RepoDisableCommand.class,
        MarketplaceCommand.RepoResetDefaultsCommand.class,
        MarketplaceCommand.RepoExportCommand.class,
        MarketplaceCommand.RepoImportCommand.class
      })
  static class RepoCommand extends MarketplaceSubCommand {
    @Override
    public void run() {
      // picocli shows usage when no subcommand
      new CommandLine(this).usage(System.out);
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "list",
      description = "List configured marketplace repositories")
  static class RepoListCommand extends MarketplaceSubCommand {
    @Override
    public void run() {
      MarketplaceConfig config = MarketplaceConfig.load();
      System.out.printf(
          "%-8s %-10s %-8s %-14s %-20s %s%n", "PRIMARY", "ENABLED", "BROWSE", "ID", "NAME", "URL");
      for (MarketplaceRepository repo : config.getRepositories()) {
        if (repo == null) {
          continue;
        }
        System.out.printf(
            "%-8s %-10s %-8s %-14s %-20s %s%n",
            repo.isPrimary() ? "*" : "",
            repo.isEnabled() ? "yes" : "no",
            repo.isBrowse() ? "yes" : "no",
            Const.NVL(repo.getId(), ""),
            Const.NVL(repo.displayName(), ""),
            repo.normalizedUrl());
        if (StringUtils.isNotBlank(repo.getCatalogUrl())) {
          System.out.println("         catalog: " + repo.getCatalogUrl());
        }
      }
      System.out.println();
      System.out.println(
          "Install order: "
              + config.orderedRepositories().stream()
                  .map(MarketplaceRepository::getId)
                  .collect(Collectors.joining(" → ")));
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "add",
      description = "Add a marketplace repository and save hop-config.json")
  static class RepoAddCommand extends MarketplaceSubCommand {
    @Option(
        names = {"--id"},
        required = true,
        description = "Stable repository id (e.g. local-nexus)")
    private String id;

    @Option(
        names = {"--url"},
        required = true,
        description = "Maven base URL (…/repository/hop-plugins/)")
    private String url;

    @Option(
        names = {"--name"},
        description = "Display name")
    private String name;

    @Option(
        names = {"--primary"},
        description = "Make this the primary repository")
    private boolean primary;

    @Option(
        names = {"--username"},
        description = "Optional Basic auth username")
    private String username;

    @Option(
        names = {"--password"},
        description = "Optional Basic auth password (prefer env HOP_MARKETPLACE_PASSWORD)")
    private String password;

    @Option(
        names = {"--browse"},
        description = "Include this repository in marketplace query/GUI discovery")
    private boolean browse;

    @Option(
        names = {"--catalog-url"},
        description = "Optional hop-marketplace-catalog.yaml URL for discovery")
    private String catalogUrl;

    @Option(
        names = {"--search-query"},
        description = "Optional discovery filter for this repository")
    private String searchQuery;

    @Option(
        names = {"--group-id-filter"},
        description = "Optional Maven groupId filter for discovery")
    private String groupIdFilter;

    @Option(
        names = {"--no-snapshots"},
        description = "Hide SNAPSHOT versions in discovery for this repository")
    private boolean noSnapshots;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        MarketplaceRepository repo =
            new MarketplaceRepository(id, StringUtils.isNotBlank(name) ? name : id, url, primary);
        repo.setUsername(username);
        repo.setPassword(password);
        repo.setBrowse(browse);
        repo.setCatalogUrl(catalogUrl);
        repo.setSearchQuery(searchQuery);
        repo.setGroupIdFilter(groupIdFilter);
        repo.setIncludeSnapshots(!noSnapshots);
        config.addRepository(repo);
        config.save();
        System.out.println(
            "Added repository '"
                + id
                + "'"
                + (primary ? " (primary)" : "")
                + (browse ? " (browse)" : "")
                + " → "
                + repo.normalizedUrl());
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this), e.getMessage() == null ? "repo add failed" : e.getMessage(), e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "export",
      description =
          "Export a repository definition YAML/JSON (shareable; passwords omitted). For company"
              + " marketplace onboarding.")
  static class RepoExportCommand extends MarketplaceSubCommand {
    @Parameters(index = "0", paramLabel = "ID", description = "Configured repository id")
    private String id;

    @Option(
        names = {"-o", "--output"},
        required = true,
        description = "Output file (.yaml / .yml / .json)")
    private String output;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        MarketplaceRepository repo = config.findRepository(id);
        if (repo == null) {
          throw new HopException("Unknown repository id: " + id);
        }
        Path out = Path.of(output);
        MarketplaceRepositoryDefinition.save(out, repo);
        System.out.println("Wrote repository definition: " + out.toAbsolutePath().normalize());
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo export failed" : e.getMessage(),
            e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "import",
      description =
          "Import a hop-marketplace-repo.yaml (or URL) into hop-config.json. Upserts by repository"
              + " id.")
  static class RepoImportCommand extends MarketplaceSubCommand {
    @Parameters(
        index = "0",
        paramLabel = "FILE_OR_URL",
        description = "Path or https URL to hop-marketplace-repo.yaml")
    private String fileOrUrl;

    @Option(
        names = {"--primary"},
        description = "Make the imported repository primary")
    private boolean primary;

    @Override
    public void run() {
      try {
        MarketplaceRepository imported = MarketplaceRepositoryDefinition.loadFromUri(fileOrUrl);
        MarketplaceConfig config = MarketplaceConfig.load();
        boolean existed = config.findRepository(imported.getId()) != null;
        MarketplaceRepositoryDefinition.applyToConfig(config, imported, primary);
        config.save();
        System.out.println(
            (existed ? "Updated" : "Imported")
                + " repository '"
                + imported.getId()
                + "' → "
                + imported.normalizedUrl()
                + (imported.isBrowse() ? " (browse enabled)" : ""));
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo import failed" : e.getMessage(),
            e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "remove",
      description = "Remove a marketplace repository by id")
  static class RepoRemoveCommand extends MarketplaceSubCommand {
    @Parameters(index = "0", paramLabel = "ID", description = "Repository id")
    private String id;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        config.removeRepository(id);
        config.save();
        System.out.println("Removed repository '" + id + "'");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo remove failed" : e.getMessage(),
            e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "set-primary",
      description = "Set the primary marketplace repository")
  static class RepoSetPrimaryCommand extends MarketplaceSubCommand {
    @Parameters(index = "0", paramLabel = "ID", description = "Repository id")
    private String id;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        config.setPrimary(id);
        config.save();
        System.out.println("Primary marketplace repository is now '" + id + "'");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo set-primary failed" : e.getMessage(),
            e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "enable",
      description = "Enable a repository in the fallback chain")
  static class RepoEnableCommand extends MarketplaceSubCommand {
    @Parameters(index = "0", paramLabel = "ID")
    private String id;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        config.setEnabled(id, true);
        config.save();
        System.out.println("Enabled repository '" + id + "'");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo enable failed" : e.getMessage(),
            e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "disable",
      description = "Disable a repository (skip in fallback chain)")
  static class RepoDisableCommand extends MarketplaceSubCommand {
    @Parameters(index = "0", paramLabel = "ID")
    private String id;

    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        config.setEnabled(id, false);
        config.save();
        System.out.println("Disabled repository '" + id + "'");
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo disable failed" : e.getMessage(),
            e);
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "reset-defaults",
      description = "Reset repositories to ASF primary + Maven Central fallback")
  static class RepoResetDefaultsCommand extends MarketplaceSubCommand {
    @Override
    public void run() {
      try {
        MarketplaceConfig config = MarketplaceConfig.load();
        config.resetToDefaults();
        config.save();
        System.out.println(
            "Marketplace repositories reset to ASF primary + Maven Central fallback.");
        for (MarketplaceRepository repo : config.getRepositories()) {
          System.out.println(
              (repo.isPrimary() ? "* " : "  ") + repo.getId() + "  " + repo.normalizedUrl());
        }
      } catch (Exception e) {
        System.err.println("ERROR: " + e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "repo reset-defaults failed" : e.getMessage(),
            e);
      }
    }
  }
}

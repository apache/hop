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

package org.apache.hop.search;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.*;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;

import java.util.*;

public class HopSearch implements Runnable, IHasHopMetadataProvider {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested;

  @Parameters(description = "The string to search for")
  private String searchString;

  @Option(
      names = {"-i", "--case-insensitive"},
      description = "Perform a case insensitive search")
  private Boolean caseInsensitive;

  @Option(
      names = {"-x", "--regular-expression"},
      description = "The specified search string is a regular expression")
  private Boolean regularExpression;

  @Option(
      names = {"-l", "--print-locations"},
      description = "Print which locations are being looked at")
  private Boolean printLocations;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  protected List<ISearchablesLocation> searchablesLocations;

  public HopSearch() {
    searchablesLocations = new ArrayList<>();
  }

  @Override
  public void run() {

    try {
      LogChannel logChannel = new LogChannel("hop-search");
      logChannel.setSimplified(true);
      ILogChannel log = logChannel;
      variables = Variables.getADefaultVariableSpace();
      buildMetadataProvider();

      boolean actionTaken = false;

      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get(key);
        if (mixin instanceof IConfigOptions) {
          IConfigOptions configOptions = (IConfigOptions) mixin;

          actionTaken = configOptions.handleOption(log, this, variables) || actionTaken;
        }
      }

      if (!actionTaken || StringUtils.isEmpty(searchString)) {
        cmd.usage(System.out);
        System.exit(1);
      }

      if (searchablesLocations.isEmpty()) {
        System.out.println(
            "There were no locations found to search. Specify an option so that Hop knows where to look.");
        System.exit(3);
      }

      boolean isCaseSensitive = caseInsensitive == null || !caseInsensitive;
      boolean isRegularExpression = regularExpression != null && regularExpression;

      SearchQuery searchQuery = new SearchQuery(searchString, isCaseSensitive, isRegularExpression);
      System.out.println(
          "Searching for ["
              + searchString
              + "]  Case sensitive? "
              + isCaseSensitive
              + "  Regular expression? "
              + isRegularExpression);

      // Get all the searchable analysers from the plugin registry...
      //
      Map<Class<ISearchableAnalyser>, ISearchableAnalyser> searchableAnalyserMap = new HashMap<>();
      PluginRegistry registry = PluginRegistry.getInstance();
      for (IPlugin analyserPlugin : registry.getPlugins(SearchableAnalyserPluginType.class)) {
        ISearchableAnalyser searchableAnalyser =
            (ISearchableAnalyser) registry.loadClass(analyserPlugin);
        searchableAnalyserMap.put(searchableAnalyser.getSearchableClass(), searchableAnalyser);
      }

      // Search!
      //
      for (ISearchablesLocation searchablesLocation : searchablesLocations) {
        System.out.println(
            "Searching in location : " + searchablesLocation.getLocationDescription());
        System.out.println(
            "-----------------------------------------------------------------------------------");

        Iterator<ISearchable> iterator =
            searchablesLocation.getSearchables(metadataProvider, variables);
        while (iterator.hasNext()) {
          // Load the next object
          //
          ISearchable searchable = iterator.next();

          Object object = searchable.getSearchableObject();
          if (object != null) {
            if (printLocations != null && printLocations) {
              System.out.println("Checking searchable: " + searchable.getName());
            }
            // Find an analyser...
            //
            ISearchableAnalyser searchableAnalyser = searchableAnalyserMap.get(object.getClass());
            if (searchableAnalyser != null) {
              List<ISearchResult> searchResults =
                  searchableAnalyser.search(searchable, searchQuery);

              // Print the results...
              //
              for (ISearchResult searchResult : searchResults) {
                String filename =
                    variables.resolve(searchResult.getMatchingSearchable().getFilename());
                if (StringUtils.isNotEmpty(filename)) {
                  System.out.print(filename + " : ");
                }
                System.out.print(
                    searchResult.getComponent() + " : " + searchResult.getDescription());
                System.out.println();
              }
            }
          }
        }
      }

    } catch (Exception e) {
      throw new ExecutionException(cmd, "There was an error handling options", e);
    }
  }

  private void buildMetadataProvider() throws HopException {
    List<IHopMetadataProvider> providers = new ArrayList<>();

    String folder = variables.getVariable(Const.HOP_METADATA_FOLDER);
    if (StringUtils.isEmpty(folder)) {
      providers.add(new JsonMetadataProvider());
    } else {
      ITwoWayPasswordEncoder passwordEncoder = Encr.getEncoder();
      if (passwordEncoder == null) {
        passwordEncoder = new HopTwoWayPasswordEncoder();
      }
      providers.add(new JsonMetadataProvider(passwordEncoder, folder, variables));
    }

    metadataProvider = new MultiMetadataProvider(Encr.getEncoder(), providers, variables);
  }

  /**
   * Gets cmd
   *
   * @return value of cmd
   */
  public CommandLine getCmd() {
    return cmd;
  }

  /** @param cmd The cmd to set */
  public void setCmd(CommandLine cmd) {
    this.cmd = cmd;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override
  public MultiMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** @param metadataProvider The metadataProvider to set */
  @Override
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Gets helpRequested
   *
   * @return value of helpRequested
   */
  public boolean isHelpRequested() {
    return helpRequested;
  }

  /** @param helpRequested The helpRequested to set */
  public void setHelpRequested(boolean helpRequested) {
    this.helpRequested = helpRequested;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /** @param variables The variables to set */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets searchablesLocations
   *
   * @return value of searchablesLocations
   */
  public List<ISearchablesLocation> getSearchablesLocations() {
    return searchablesLocations;
  }

  /** @param searchablesLocations The searchablesLocations to set */
  public void setSearchablesLocations(List<ISearchablesLocation> searchablesLocations) {
    this.searchablesLocations = searchablesLocations;
  }

  /**
   * Gets searchString
   *
   * @return value of searchString
   */
  public String getSearchString() {
    return searchString;
  }

  /** @param searchString The searchString to set */
  public void setSearchString(String searchString) {
    this.searchString = searchString;
  }

  /**
   * Gets caseInsensitive
   *
   * @return value of caseInsensitive
   */
  public Boolean getCaseInsensitive() {
    return caseInsensitive;
  }

  /** @param caseInsensitive The caseInsensitive to set */
  public void setCaseInsensitive(Boolean caseInsensitive) {
    this.caseInsensitive = caseInsensitive;
  }

  /**
   * Gets regularExpression
   *
   * @return value of regularExpression
   */
  public Boolean getRegularExpression() {
    return regularExpression;
  }

  /** @param regularExpression The regularExpression to set */
  public void setRegularExpression(Boolean regularExpression) {
    this.regularExpression = regularExpression;
  }

  public static void main(String[] args) {

    HopSearch hopSearch = new HopSearch();

    try {
      HopEnvironment.init();

      // Also register the search plugin type (usually only done for the GUI)
      // We don't want to load these in HopEnvironmnent.init() because for now it would
      // only be useful in Hop GUI and Hop Search.  There is no need to slow down
      // Hop Run or Hop Server with this.
      //
      PluginRegistry registry = PluginRegistry.getInstance();
      SearchableAnalyserPluginType searchableAnalyserPluginType =
          SearchableAnalyserPluginType.getInstance();
      registry.addPluginType(searchableAnalyserPluginType);
      searchableAnalyserPluginType.searchPlugins();

      CommandLine cmd = new CommandLine(hopSearch);
      List<IPlugin> configPlugins = registry.getPlugins(ConfigPluginType.class);
      for (IPlugin configPlugin : configPlugins) {
        // Load only the plugins of the "search" category
        if (ConfigPlugin.CATEGORY_SEARCH.equals(configPlugin.getCategory())) {
          IConfigOptions configOptions = registry.loadClass(configPlugin, IConfigOptions.class);
          cmd.addMixin(configPlugin.getIds()[0], configOptions);
        }
      }

      hopSearch.setCmd(cmd);
      CommandLine.ParseResult parseResult = cmd.parseArgs(args);
      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        hopSearch.run();
        System.exit(0);
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      hopSearch.cmd.usage(System.err);
      System.exit(9);
    } catch (ExecutionException e) {
      System.err.println("Error found during execution!");
      System.err.println(Const.getStackTracker(e));

      System.exit(1);
    } catch (Exception e) {
      System.err.println("General error found, something went horribly wrong!");
      System.err.println(Const.getStackTracker(e));

      System.exit(2);
    }
  }
}

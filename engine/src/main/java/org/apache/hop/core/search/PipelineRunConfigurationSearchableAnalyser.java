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

package org.apache.hop.core.search;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

@SearchableAnalyserPlugin(
    id = "PipelineRunConfigurationSearchableAnalyser",
    name = "Search in pipeline run configuration metadata")
public class PipelineRunConfigurationSearchableAnalyser
    extends BaseMetadataSearchableAnalyser<PipelineRunConfiguration>
    implements ISearchableAnalyser<PipelineRunConfiguration> {

  @Override
  public Class<PipelineRunConfiguration> getSearchableClass() {
    return PipelineRunConfiguration.class;
  }

  @Override
  public List<ISearchResult> search(
      ISearchable<PipelineRunConfiguration> searchable, ISearchQuery searchQuery) {
    PipelineRunConfiguration runConfig = searchable.getSearchableObject();
    String component = getMetadataComponent();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty(
        searchable,
        results,
        searchQuery,
        "Pipeline run configuration name",
        runConfig.getName(),
        component);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "Pipeline run configuration description",
        runConfig.getDescription(),
        component);

    // Analyze the variables
    //
    for (DescribedVariable configurationVariable : runConfig.getConfigurationVariables()) {
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Pipeline run configuration variable name",
          configurationVariable.getName(),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Pipeline run configuration variable value",
          configurationVariable.getValue(),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Pipeline run configuration variable description",
          configurationVariable.getDescription(),
          null);
    }

    // Analyze the configuration plugin fields
    //
    matchObjectFields(
        searchable,
        results,
        searchQuery,
        runConfig.getEngineRunConfiguration(),
        "Pipeline run configuration property",
        null);

    return results;
  }
}

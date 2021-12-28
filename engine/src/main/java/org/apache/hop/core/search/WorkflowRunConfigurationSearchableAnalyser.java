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

import org.apache.hop.workflow.config.WorkflowRunConfiguration;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
    id = "WorkflowRunConfigurationSearchableAnalyser",
    name = "Search in workflow run configuration metadata")
public class WorkflowRunConfigurationSearchableAnalyser
    extends BaseMetadataSearchableAnalyser<WorkflowRunConfiguration>
    implements ISearchableAnalyser<WorkflowRunConfiguration> {

  @Override
  public Class<WorkflowRunConfiguration> getSearchableClass() {
    return WorkflowRunConfiguration.class;
  }

  @Override
  public List<ISearchResult> search(
      ISearchable<WorkflowRunConfiguration> searchable, ISearchQuery searchQuery) {
    WorkflowRunConfiguration runConfig = searchable.getSearchableObject();
    String component = getMetadataComponent();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty(
        searchable,
        results,
        searchQuery,
        "Workflow run configuration name",
        runConfig.getName(),
        component);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "Workflow run configuration description",
        runConfig.getDescription(),
        component);

    // Analyze the configuration plugin fields
    //
    matchObjectFields(
        searchable,
        results,
        searchQuery,
        runConfig.getEngineRunConfiguration(),
        "Workflow run configuration property",
        null);

    return results;
  }
}

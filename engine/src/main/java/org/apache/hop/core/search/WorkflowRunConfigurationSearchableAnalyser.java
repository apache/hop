package org.apache.hop.core.search;

import org.apache.hop.workflow.config.WorkflowRunConfiguration;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "WorkflowRunConfigurationSearchableAnalyser",
  name = "Search in workflow run configuration metadata"
)
public class WorkflowRunConfigurationSearchableAnalyser extends BaseSearchableAnalyser<WorkflowRunConfiguration> implements ISearchableAnalyser<WorkflowRunConfiguration> {

  @Override public Class<WorkflowRunConfiguration> getSearchableClass() {
    return WorkflowRunConfiguration.class;
  }

  @Override public List<ISearchResult> search( ISearchable<WorkflowRunConfiguration> searchable, ISearchQuery searchQuery ) {
    WorkflowRunConfiguration runConfig = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "Workflow run configuration name", runConfig.getName(), null );
    matchProperty( searchable, results, searchQuery, "Workflow run configuration description", runConfig.getDescription(), null );

    /* Analyze the variables
    //
    for ( VariableValueDescription configurationVariable : runConfig.getConfigurationVariables()) {
      matchProperty( searchable, results, searchQuery, "Pipeline run configuration variable name", configurationVariable.getName() );
      matchProperty( searchable, results, searchQuery, "Pipeline run configuration variable value", configurationVariable.getValue() );
      matchProperty( searchable, results, searchQuery, "Pipeline run configuration variable description", configurationVariable.getDescription() );
    }
    */

    // Analyze the configuration plugin fields
    //
    matchObjectFields( searchable, results, searchQuery, runConfig.getEngineRunConfiguration(), "Workflow run configuration property", null );

    return results;
  }
}

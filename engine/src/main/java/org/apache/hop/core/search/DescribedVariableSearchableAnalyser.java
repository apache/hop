package org.apache.hop.core.search;

import org.apache.hop.core.config.DescribedVariable;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "DescribedVariableSearchableAnalyser",
  name = "Search in a described variable"
)
public class DescribedVariableSearchableAnalyser extends BaseSearchableAnalyser<DescribedVariable> implements ISearchableAnalyser<DescribedVariable> {

  @Override public Class<DescribedVariable> getSearchableClass() {
    return DescribedVariable.class;
  }

  @Override public List<ISearchResult> search( ISearchable<DescribedVariable> searchable, ISearchQuery searchQuery ) {
    DescribedVariable describedVariable = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "variable name", describedVariable.getName() );
    matchProperty( searchable, results, searchQuery, "variable value", describedVariable.getValue() );
    matchProperty( searchable, results, searchQuery, "variable description", describedVariable.getDescription() );

    return results;
  }
}

package org.apache.hop.core.search;

import org.apache.hop.core.database.DatabaseMeta;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "DatabaseMetaSearchableAnalyser",
  name = "Search in relational database metadata"
)
public class DatabaseMetaSearchableAnalyser extends BaseSearchableAnalyser<DatabaseMeta> implements ISearchableAnalyser<DatabaseMeta> {

  @Override public Class<DatabaseMeta> getSearchableClass() {
    return DatabaseMeta.class;
  }

  @Override public List<ISearchResult> search( ISearchable<DatabaseMeta> searchable, ISearchQuery searchQuery ) {
    DatabaseMeta databaseMeta = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "database name", databaseMeta.getName(), null );

    matchObjectFields( searchable, results, searchQuery, databaseMeta.getIDatabase(), "database '"+databaseMeta.getName()+"' property", null );

    return results;
  }
}

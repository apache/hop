package org.apache.hop.core.search;

import org.apache.hop.server.HopServer;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
  id = "HopServerSearchableAnalyser",
  name = "Search in hop server metadata"
)
public class HopServerSearchableAnalyser extends BaseSearchableAnalyser<HopServer> implements ISearchableAnalyser<HopServer> {

  @Override public Class<HopServer> getSearchableClass() {
    return HopServer.class;
  }

  @Override public List<ISearchResult> search( ISearchable<HopServer> searchable, ISearchQuery searchQuery ) {
    HopServer hopServer = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty( searchable, results, searchQuery, "Hop server name", hopServer.getName() );
    matchProperty( searchable, results, searchQuery, "Hop server hostname", hopServer.getHostname() );
    matchProperty( searchable, results, searchQuery, "Hop server port", hopServer.getPort() );
    matchProperty( searchable, results, searchQuery, "Hop server username", hopServer.getUsername() );
    matchProperty( searchable, results, searchQuery, "Hop server webapp", hopServer.getWebAppName() );
    return results;
  }
}

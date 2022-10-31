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

package org.apache.hop.neo4j.shared;

import org.apache.hop.core.search.BaseMetadataSearchableAnalyser;
import org.apache.hop.core.search.ISearchQuery;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchableAnalyserPlugin;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
    id = "NeoConnectionSearchableAnalyser",
    name = "Search in Neo4j Connection metadata")
public class NeoConnectionSearchableAnalyser extends BaseMetadataSearchableAnalyser<NeoConnection>
    implements ISearchableAnalyser<NeoConnection> {

  @Override
  public Class<NeoConnection> getSearchableClass() {
    return NeoConnection.class;
  }

  @Override
  public List<ISearchResult> search(
      ISearchable<NeoConnection> searchable, ISearchQuery searchQuery) {
    NeoConnection neoConnection = searchable.getSearchableObject();
    String component = getMetadataComponent();
    List<ISearchResult> results = new ArrayList<>();

    matchProperty(searchable, results, searchQuery, "Name", neoConnection.getName(), component);
    matchProperty(searchable, results, searchQuery, "Server", neoConnection.getServer(), component);
    matchProperty(
        searchable, results, searchQuery, "Bolt port", neoConnection.getBoltPort(), component);
    matchProperty(
        searchable, results, searchQuery, "Username", neoConnection.getUsername(), component);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "Database name",
        neoConnection.getDatabaseName(),
        component);
    return results;
  }
}

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
import org.apache.hop.server.HopServerMeta;

@SearchableAnalyserPlugin(
    id = "HopServerSearchableAnalyser",
    name = "Search in hop server metadata")
public class HopServerSearchableAnalyser extends BaseMetadataSearchableAnalyser<HopServerMeta>
    implements ISearchableAnalyser<HopServerMeta> {

  @Override
  public Class<HopServerMeta> getSearchableClass() {
    return HopServerMeta.class;
  }

  @Override
  public List<ISearchResult> search(
      ISearchable<HopServerMeta> searchable, ISearchQuery searchQuery) {
    HopServerMeta hopServer = searchable.getSearchableObject();
    String component = getMetadataComponent();
    List<ISearchResult> results = new ArrayList<>();

    matchProperty(
        searchable, results, searchQuery, "Hop server name", hopServer.getName(), component);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "Hop server hostname",
        hopServer.getHostname(),
        component);
    matchProperty(
        searchable, results, searchQuery, "Hop server port", hopServer.getPort(), component);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "Hop server username",
        hopServer.getUsername(),
        component);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "Hop server webapp",
        hopServer.getWebAppName(),
        component);
    return results;
  }
}

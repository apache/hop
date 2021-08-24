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

package org.apache.hop.neo4j.model;

import org.apache.hop.core.search.*;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
    id = "GraphModelSearchableAnalyser",
    name = "Search in Neo4j Graph Model metadata")
public class GraphModelSearchableAnalyser extends BaseMetadataSearchableAnalyser<GraphModel>
    implements ISearchableAnalyser<GraphModel> {

  @Override
  public Class<GraphModel> getSearchableClass() {
    return GraphModel.class;
  }

  @Override
  public List<ISearchResult> search(ISearchable<GraphModel> searchable, ISearchQuery searchQuery) {
    GraphModel graphModel = searchable.getSearchableObject();
    String component = getMetadataComponent();
    List<ISearchResult> results = new ArrayList<>();

    matchProperty(searchable, results, searchQuery, "Name", graphModel.getName(), component);
    matchProperty(
        searchable, results, searchQuery, "Description", graphModel.getDescription(), component);

    // Look in nodes
    //
    for (GraphNode graphNode : graphModel.getNodes()) {
      matchProperty(searchable, results, searchQuery, "Node name", graphNode.getName(), component);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Node description",
          graphNode.getDescription(),
          component);

      // Match labels...
      for (String nodeLabel : graphNode.getLabels()) {
        matchProperty(searchable, results, searchQuery, "Node label", nodeLabel, component);
      }

      // Match properties...
      for (GraphProperty property : graphNode.getProperties()) {
        matchProperty(
            searchable, results, searchQuery, "Node property name", property.getName(), component);
        matchProperty(
            searchable,
            results,
            searchQuery,
            "Node property type",
            property.getType().name(),
            component);
        matchProperty(
            searchable,
            results,
            searchQuery,
            "Node property description",
            property.getDescription(),
            component);
      }
    }

    // Look in relationships...
    //
    for (GraphRelationship relationship : graphModel.getRelationships()) {
      matchProperty(
          searchable, results, searchQuery, "Relationship name", relationship.getName(), component);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Relationship description",
          relationship.getDescription(),
          component);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Relationship source node",
          relationship.getNodeSource(),
          component);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Relationship target node",
          relationship.getNodeTarget(),
          component);
    }

    return results;
  }
}

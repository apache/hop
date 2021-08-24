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

package org.apache.hop.testing;

import org.apache.hop.core.search.*;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(
    id = "PipelineUnitTestSearchableAnalyser",
    name = "Search in pipeline unit test metadata")
public class PipelineUnitTestSearchableAnalyser
    extends BaseMetadataSearchableAnalyser<PipelineUnitTest>
    implements ISearchableAnalyser<PipelineUnitTest> {

  @Override
  public Class<PipelineUnitTest> getSearchableClass() {
    return PipelineUnitTest.class;
  }

  @Override
  public List<ISearchResult> search(
      ISearchable<PipelineUnitTest> searchable, ISearchQuery searchQuery) {
    PipelineUnitTest unitTest = searchable.getSearchableObject();

    List<ISearchResult> results = new ArrayList<>();

    matchProperty(searchable, results, searchQuery, "name", unitTest.getName(), null);
    matchProperty(searchable, results, searchQuery, "description", unitTest.getDescription(), null);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "type",
        unitTest.getType() != null ? unitTest.getType().name() : null,
        null);
    matchProperty(
        searchable,
        results,
        searchQuery,
        "pipeline-filename",
        unitTest.getPipelineFilename(),
        null);
    matchProperty(searchable, results, searchQuery, "test-filename", unitTest.getFilename(), null);
    matchProperty(searchable, results, searchQuery, "base-path", unitTest.getBasePath(), null);
    matchProperty(
        searchable, results, searchQuery, "auto-open", unitTest.isAutoOpening() ? "Y" : "N", null);

    // Analyze the variables
    //
    for (VariableValue configurationVariable : unitTest.variableValues) {
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Unit test variable name",
          configurationVariable.getKey(),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Unit test variable value",
          configurationVariable.getValue(),
          null);
    }

    // The database replacements...
    //
    for (PipelineUnitTestDatabaseReplacement replacement : unitTest.getDatabaseReplacements()) {
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Unit test database replacement: original",
          replacement.getOriginalDatabaseName(),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "Unit test database replacement: replacement",
          replacement.getReplacementDatabaseName(),
          null);
    }

    return results;
  }
}

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

import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.search.*;

import java.util.ArrayList;
import java.util.List;

@SearchableAnalyserPlugin(id = "DataSetSearchableAnalyser", name = "Search in data set metadata")
public class DataSetSearchableAnalyser extends BaseMetadataSearchableAnalyser<DataSet>
    implements ISearchableAnalyser<DataSet> {
  @Override
  public List<ISearchResult> search(ISearchable<DataSet> searchable, ISearchQuery searchQuery) {
    DataSet set = searchable.getSearchableObject();
    List<ISearchResult> results = new ArrayList<>();

    matchProperty(searchable, results, searchQuery, "name", set.getName(), null);
    matchProperty(searchable, results, searchQuery, "description", set.getDescription(), null);
    matchProperty(searchable, results, searchQuery, "folder-name", set.getFolderName(), null);
    matchProperty(searchable, results, searchQuery, "base-filename", set.getBaseFilename(), null);

    for (DataSetField field : set.getFields()) {
      matchProperty(
          searchable,
          results,
          searchQuery,
          "name of data set field: " + field.getFieldName(),
          field.getFieldName(),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "type of data set field: " + field.getFieldName(),
          ValueMetaFactory.getValueMetaName(field.getType()),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "format of data set field: " + field.getFieldName(),
          field.getFormat(),
          null);
      matchProperty(
          searchable,
          results,
          searchQuery,
          "comment of data set field: " + field.getFieldName(),
          field.getComment(),
          null);
    }

    return results;
  }

  @Override
  public Class<DataSet> getSearchableClass() {
    return DataSet.class;
  }
}

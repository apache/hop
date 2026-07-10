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

package org.apache.hop.ui.hopgui.search;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.search.BaseSearchableAnalyser;
import org.apache.hop.core.search.ISearchQuery;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableAnalyser;
import org.apache.hop.core.search.SearchableAnalyserPlugin;

@SearchableAnalyserPlugin(
    id = "TextFileContentSearchableAnalyser",
    name = "Search in a text file",
    description = "Search content of text-based explorer files")
public class TextFileContentSearchableAnalyser extends BaseSearchableAnalyser<TextFileContent>
    implements ISearchableAnalyser<TextFileContent> {

  @Override
  public Class<TextFileContent> getSearchableClass() {
    return TextFileContent.class;
  }

  @Override
  public List<ISearchResult> search(
      ISearchable<TextFileContent> searchable, ISearchQuery searchQuery) {
    TextFileContent textFileContent = searchable.getSearchableObject();
    List<ISearchResult> results = new ArrayList<>();

    String content = textFileContent.getContent();
    if (content == null || content.isEmpty()) {
      return results;
    }

    String[] lines = content.split("\\R", -1);
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      String lineNumber = String.valueOf(i + 1);
      matchProperty(searchable, results, searchQuery, "line " + lineNumber, line, lineNumber);
    }

    return results;
  }
}

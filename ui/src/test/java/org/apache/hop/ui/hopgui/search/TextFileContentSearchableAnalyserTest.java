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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.core.search.SearchQuery;
import org.junit.jupiter.api.Test;

class TextFileContentSearchableAnalyserTest {

  @Test
  void matchesLinesAndReportsLineNumbers() {
    TextFileContent content =
        new TextFileContent("/project/query.sql", "select 1;\nselect foo from bar;\nselect 2;");
    ISearchable<TextFileContent> searchable =
        new HopGuiTextFileSearchable("test", "SQL File", content);

    TextFileContentSearchableAnalyser analyser = new TextFileContentSearchableAnalyser();
    List<ISearchResult> results = analyser.search(searchable, new SearchQuery("foo", true, false));

    assertEquals(1, results.size());
    assertEquals("2", results.get(0).getComponent());
    assertTrue(results.get(0).getMatchingString().contains("foo"));
  }

  @Test
  void emptyContentReturnsNoResults() {
    TextFileContent content = new TextFileContent("/project/empty.txt", "");
    ISearchable<TextFileContent> searchable =
        new ISearchable<>() {
          @Override
          public String getLocation() {
            return "test";
          }

          @Override
          public String getName() {
            return "empty.txt";
          }

          @Override
          public String getType() {
            return "TXT File";
          }

          @Override
          public String getFilename() {
            return content.getFilename();
          }

          @Override
          public TextFileContent getSearchableObject() {
            return content;
          }

          @Override
          public ISearchableCallback getSearchCallback() {
            return null;
          }
        };

    TextFileContentSearchableAnalyser analyser = new TextFileContentSearchableAnalyser();
    assertTrue(analyser.search(searchable, new SearchQuery("anything", true, false)).isEmpty());
  }
}

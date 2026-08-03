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

package org.apache.hop.ui.hopgui.search.config;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.search.ISearchResult;

/** Outcome of a limited GUI search: results plus whether caps/filters applied. */
@Getter
public class SearchAnalysisResult {

  private final List<ISearchResult> results;
  private final boolean truncated;
  private final boolean contentSearchSkipped;
  private final boolean projectTextFilesExcluded;

  public SearchAnalysisResult(
      List<ISearchResult> results,
      boolean truncated,
      boolean contentSearchSkipped,
      boolean projectTextFilesExcluded) {
    this.results = results == null ? new ArrayList<>() : results;
    this.truncated = truncated;
    this.contentSearchSkipped = contentSearchSkipped;
    this.projectTextFilesExcluded = projectTextFilesExcluded;
  }

  public static SearchAnalysisResult empty() {
    return new SearchAnalysisResult(List.of(), false, false, false);
  }
}

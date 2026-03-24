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

package org.apache.hop.pipeline.transforms.fileinput.text;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/** Processor of Filters. Kind of inversion principle, and to make unit testing easier. */
@Getter
@Setter
public class TextFileFilterProcessor {
  /** The filters to process */
  private List<TextFileFilter> filters;

  private List<String> filtersStrings;
  private boolean stopProcessing;

  private TextFileFilterProcessor() {
    this.filters = new ArrayList<>();
    this.filtersStrings = new ArrayList<>();
    this.stopProcessing = false;
  }

  /**
   * @param filters The filters to process
   */
  public TextFileFilterProcessor(List<TextFileFilter> filters, IVariables variables) {
    this();
    this.filters = filters;

    if (!filters.isEmpty()) {
      for (TextFileFilter filter : filters) {
        filtersStrings.add(variables.resolve(filter.getFilterString()));
      }
    }
  }

  public boolean doFilters(String line) {
    if (filters == null) {
      return true;
    }

    boolean filterOK = true; // if false: skip this row
    boolean positiveMode = false;
    boolean positiveMatchFound = false;

    // If we have at least one positive filter, we enter positiveMode
    // Negative filters will always take precedence, meaning that the line
    // is skipped if one of them is found

    for (int i = 0; i < filters.size(); i++) {
      TextFileFilter filter = filters.get(i);
      String filterString = filtersStrings.get(i);
      if (filter.isFilterPositive()) {
        positiveMode = true;
      }

      if (!Utils.isEmpty(filterString)) {
        int from = filter.getFilterPosition();
        if (from >= 0) {
          int to = from + filterString.length();
          if (line.length() >= from && line.length() >= to) {
            String sub = line.substring(filter.getFilterPosition(), to);
            if (sub.equalsIgnoreCase(filterString)) {
              if (filter.isFilterPositive()) {
                positiveMatchFound = true;
              } else {
                filterOK = false; // skip this one!
              }
            }
          }
        } else { // anywhere on the line
          int idx = line.indexOf(filterString);
          if (idx >= 0) {
            if (filter.isFilterPositive()) {
              positiveMatchFound = true;
            } else {
              filterOK = false; // skip this one!
            }
          }
        }

        if (!filterOK) {
          boolean isFilterLastLine = filter.isFilterLastLine();
          if (isFilterLastLine) {
            stopProcessing = true;
          }
        }
      }
    }

    // Positive mode and no match found? Discard the line
    if (filterOK && positiveMode && !positiveMatchFound) {
      filterOK = false;
    }

    return filterOK;
  }
}

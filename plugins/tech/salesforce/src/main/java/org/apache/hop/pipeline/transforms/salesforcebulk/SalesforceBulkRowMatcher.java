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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;

/**
 * Pairs Bulk API result rows with the rows that were uploaded. Result files are not in input order.
 * Rows that share a key are paired in input order.
 */
public final class SalesforceBulkRowMatcher {
  private SalesforceBulkRowMatcher() {}

  /** One uploaded row and the result Salesforce returned for it. */
  @Getter
  public static final class Pending {
    private final Object[] inputRow;
    private final List<String> values;
    @Setter private boolean matched;
    @Setter private String salesforceId;
    @Setter private String error;

    public Pending(Object[] inputRow, List<String> values) {
      this.inputRow = inputRow;
      this.values = values;
    }
  }

  public static String key(List<String> values, int[] keyIndexes) {
    StringBuilder key = new StringBuilder();
    for (int i = 0; i < keyIndexes.length; i++) {
      if (i > 0) {
        key.append('\u0001');
      }
      String value = values.get(keyIndexes[i]);
      key.append(value == null ? "" : value);
    }
    return key.toString();
  }

  public static void match(
      List<Pending> pending,
      List<String> sentHeaders,
      int[] keyIndexes,
      String resultsCsv,
      SalesforceBulkDelimiter delimiter,
      boolean failed)
      throws HopException {
    if (resultsCsv == null || resultsCsv.isBlank() || pending.isEmpty()) {
      return;
    }
    List<List<String>> table = SalesforceBulkCsv.parse(resultsCsv, delimiter);
    if (table.size() < 2) {
      return;
    }
    List<String> resultHeaders = table.get(0);
    Map<String, Integer> index = new HashMap<>();
    for (int i = 0; i < resultHeaders.size(); i++) {
      index.put(resultHeaders.get(i), i);
    }
    int idIndex = index.getOrDefault(SalesforceBulkCsv.SUCCESS_ID, -1);
    int errorIndex = index.getOrDefault(SalesforceBulkCsv.FAILURE_ERROR, -1);
    for (int column : keyIndexes) {
      if (!index.containsKey(sentHeaders.get(column))) {
        throw new HopException("Bulk API results have no column " + sentHeaders.get(column));
      }
    }
    Map<String, ArrayDeque<Pending>> waiting = new HashMap<>();
    for (Pending row : pending) {
      if (row.isMatched()) {
        continue;
      }
      waiting
          .computeIfAbsent(key(row.getValues(), keyIndexes), ignored -> new ArrayDeque<>())
          .add(row);
    }
    for (int r = 1; r < table.size(); r++) {
      List<String> result = table.get(r);
      List<String> aligned = new ArrayList<>(sentHeaders.size());
      for (int c = 0; c < sentHeaders.size(); c++) {
        int at = index.getOrDefault(sentHeaders.get(c), -1);
        aligned.add(at >= 0 && at < result.size() ? result.get(at) : "");
      }
      ArrayDeque<Pending> queue = waiting.get(key(aligned, keyIndexes));
      if (queue == null || queue.isEmpty()) {
        continue;
      }
      Pending row = queue.removeFirst();
      row.setMatched(true);
      if (idIndex >= 0 && idIndex < result.size()) {
        row.setSalesforceId(result.get(idIndex));
      }
      if (failed && errorIndex >= 0 && errorIndex < result.size()) {
        row.setError(result.get(errorIndex));
      }
    }
  }
}

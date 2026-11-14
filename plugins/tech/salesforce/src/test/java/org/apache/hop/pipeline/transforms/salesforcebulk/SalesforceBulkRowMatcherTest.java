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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SalesforceBulkRowMatcherTest {

  @Test
  void matchesResultsThatAreNotInInputOrder() throws Exception {
    List<SalesforceBulkRowMatcher.Pending> pending = new ArrayList<>();
    pending.add(row("Alice"));
    pending.add(row("Bob"));
    pending.add(row("FAIL"));
    String success =
        "Name,sf__Id,sf__Created\nBob,001000000000002AAA,true\nAlice,001000000000001AAA,true\n";
    String failed = "Name,sf__Id,sf__Error\nFAIL,,simulated failure\n";
    SalesforceBulkRowMatcher.match(
        pending, List.of("Name"), new int[] {0}, success, SalesforceBulkDelimiter.COMMA, false);
    SalesforceBulkRowMatcher.match(
        pending, List.of("Name"), new int[] {0}, failed, SalesforceBulkDelimiter.COMMA, true);

    assertTrue(pending.get(0).isMatched());
    assertEquals("001000000000001AAA", pending.get(0).getSalesforceId());
    assertEquals("001000000000002AAA", pending.get(1).getSalesforceId());
    assertEquals("simulated failure", pending.get(2).getError());
    assertTrue(pending.get(2).isMatched());
  }

  @Test
  void pairsDuplicateKeysInInputOrder() throws Exception {
    List<SalesforceBulkRowMatcher.Pending> pending = new ArrayList<>();
    pending.add(row("Ada"));
    pending.add(row("Ada"));
    String success = "Name,sf__Id\nAda,second\nAda,first\n";
    SalesforceBulkRowMatcher.match(
        pending, List.of("Name"), new int[] {0}, success, SalesforceBulkDelimiter.COMMA, false);
    assertEquals("second", pending.get(0).getSalesforceId());
    assertEquals("first", pending.get(1).getSalesforceId());
    assertFalse(pending.stream().anyMatch(row -> !row.isMatched()));
  }

  private static SalesforceBulkRowMatcher.Pending row(String name) {
    return new SalesforceBulkRowMatcher.Pending(new Object[] {name}, List.of(name));
  }
}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class SalesforceBulkCsvTest {

  @Test
  void quotesCommasQuotesAndSpaces() {
    assertEquals("Alice", SalesforceBulkCsv.encode("Alice", ","));
    assertEquals("\"Director, Marketing\"", SalesforceBulkCsv.encode("Director, Marketing", ","));
    assertEquals("\"Say \"\"hi\"\"\"", SalesforceBulkCsv.encode("Say \"hi\"", ","));
    assertEquals("\" Alice\"", SalesforceBulkCsv.encode(" Alice", ","));
    assertEquals("#N/A", SalesforceBulkCsv.encode("#N/A", ","));
  }

  @Test
  void roundTripsQuotedFieldsAndSkipsTheTrailingNewline() throws Exception {
    SalesforceBulkCsv.Builder builder =
        new SalesforceBulkCsv.Builder(
            List.of("Name", "City"), SalesforceBulkDelimiter.COMMA, SalesforceBulkLineEnding.LF);
    builder.add(List.of("Alice", "Director, Marketing"));
    builder.add(List.of("Bob", ""));
    String csv = new String(builder.toByteArray(), StandardCharsets.UTF_8);
    assertTrue(csv.endsWith("\n"));
    List<List<String>> rows = SalesforceBulkCsv.parse(csv);
    assertEquals(List.of("Name", "City"), rows.get(0));
    assertEquals(List.of("Alice", "Director, Marketing"), rows.get(1));
    assertEquals(List.of("Bob", ""), rows.get(2));
    assertEquals(3, rows.size());
  }

  @Test
  void parsesASemicolonFile() throws Exception {
    SalesforceBulkCsv.Builder builder =
        new SalesforceBulkCsv.Builder(
            List.of("Name"), SalesforceBulkDelimiter.SEMICOLON, SalesforceBulkLineEnding.CRLF);
    builder.add(List.of("A;B"));
    List<List<String>> rows =
        SalesforceBulkCsv.parse(
            new String(builder.toByteArray(), StandardCharsets.UTF_8),
            SalesforceBulkDelimiter.SEMICOLON);
    assertEquals("A;B", rows.get(1).get(0));
  }
}

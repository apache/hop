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

package org.apache.hop.marketplace.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class CliTableTest {

  @Test
  void printTableBordersAlign() {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8);
    CliTable.printTable(out, List.of("A", "BB"), List.of(List.of("1", "22"), List.of("xx", "y")));
    String text = buf.toString(StandardCharsets.UTF_8);
    String[] lines = text.split("\\R");
    assertTrue(lines.length >= 5);
    assertTrue(lines[0].startsWith("+"));
    assertTrue(lines[0].endsWith("+"));
    assertEquals(lines[0], lines[2]);
    assertEquals(lines[0], lines[lines.length - 1]);
    assertTrue(lines[1].startsWith("|"));
    assertTrue(lines[1].contains("A"));
    assertTrue(lines[1].contains("BB"));
  }

  @Test
  void csvEscapesCommaAndQuotes() {
    assertEquals("plain", CliTable.escapeCsv("plain"));
    assertEquals("\"a,b\"", CliTable.escapeCsv("a,b"));
    assertEquals("\"say \"\"hi\"\"\"", CliTable.escapeCsv("say \"hi\""));
    assertEquals("\"a\nb\"", CliTable.escapeCsv("a\nb"));
  }

  @Test
  void printCsvHeaderOnlyWhenEmpty() {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8);
    CliTable.printCsv(out, List.of("artifact", "version"), List.of());
    String text = buf.toString(StandardCharsets.UTF_8).trim();
    assertEquals("artifact,version", text);
  }

  @Test
  void printCsvRows() {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf, true, StandardCharsets.UTF_8);
    CliTable.printCsv(out, List.of("artifact", "desc"), List.of(List.of("hop-x", "hello, world")));
    String text = buf.toString(StandardCharsets.UTF_8).trim();
    String[] lines = text.split("\\R");
    assertEquals(2, lines.length);
    assertEquals("artifact,desc", lines[0]);
    assertEquals("hop-x,\"hello, world\"", lines[1]);
  }

  @Test
  void truncateDescription() {
    assertEquals("short", CliTable.truncate("short", 56));
    assertEquals("abcdefghij...", CliTable.truncate("abcdefghijklmnopqrstuvwxyz", 13));
    assertFalse(CliTable.truncate("line\nbreak", 20).contains("\n"));
  }

  @Test
  void queryHeadersOptionalGav() {
    List<String> without = MarketplaceCommand.QueryCommand.queryHeaders(false);
    assertFalse(without.contains("GAV"));
    assertTrue(without.contains("ARTIFACT"));
    List<String> with = MarketplaceCommand.QueryCommand.queryHeaders(true);
    assertTrue(with.contains("GAV"));
    assertEquals(without.size() + 1, with.size());
  }
}

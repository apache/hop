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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BinDatabaseTest {

  @Test
  void testLoadWithHeader(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(
          ("BIN,CARDTYPE,ISSUERS,COUNTRY\n"
                  + "512003,MASTERCARD,PT. Bank Central Asia,INDONESIA\n"
                  + "51,MASTERCARD,Generic,\n")
              .getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(),
        file.toUri().toString(),
        "BIN",
        new ArrayList<>(),
        ",",
        "\"",
        "UTF-8",
        true);

    assertEquals(2, db.size());
    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("MASTERCARD", record.getCardType());
  }

  @Test
  void testLongestPrefixMatch(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(
          ("BIN,CARDTYPE,ISSUERS,COUNTRY\n"
                  + "51,MASTERCARD,Generic,\n"
                  + "512003,MASTERCARD,BCA,INDONESIA\n")
              .getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", true);

    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("MASTERCARD", record.getCardType());
  }

  @Test
  void testLoadWithoutHeaderUsesPositionalOrder(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(("512003,MASTERCARD,BCA,INDONESIA\n").getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", false);

    assertEquals(1, db.size());
    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("MASTERCARD", record.getCardType());
  }

  @Test
  void testExtraOutputField(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(
          ("BIN,CARDTYPE,ISSUERS,COUNTRY\n" + "512003,MASTERCARD,BCA,INDONESIA\n")
              .getBytes(StandardCharsets.UTF_8));
    }

    List<BinOutputField> outputFields = new ArrayList<>();
    outputFields.add(new BinOutputField("bank_name", "ISSUERS"));

    BinDatabase db = new BinDatabase();
    db.load(new Variables(), file.toUri().toString(), "", outputFields, ",", "\"", "UTF-8", true);

    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("BCA", record.getValue("bank_name"));
  }

  @Test
  void testExtraOutputFieldWithoutHeaderMapsPositionally(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(("512003,MASTERCARD,BCA,INDONESIA,BANKVALUE\n").getBytes(StandardCharsets.UTF_8));
    }

    List<BinOutputField> outputFields = new ArrayList<>();
    outputFields.add(new BinOutputField("bank_name", ""));

    BinDatabase db = new BinDatabase();
    db.load(new Variables(), file.toUri().toString(), "", outputFields, ",", "\"", "UTF-8", false);

    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("BANKVALUE", record.getValue("bank_name"));
  }

  @Test
  void testBrandDetectedAsCardType(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(("BIN,BRAND\n" + "512003,MASTERCARD\n").getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", true);

    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("MASTERCARD", record.getCardType());
  }

  @Test
  void testUnmatchedCardTypeIsEmpty(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(("BIN,FOO\n" + "512003,MASTERCARD\n").getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", true);

    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertEquals("", record.getCardType());
  }

  @Test
  void testSkippedRowsCounted(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(
          ("BIN,CARDTYPE\n"
                  + "512003,MASTERCARD\n"
                  + "123456789012,TOOLONG\n"
                  + "ABCDEF,NOTNUMERIC\n")
              .getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", true);

    assertEquals(1, db.size());
    assertEquals(2, db.getSkippedRows());
  }

  @Test
  void testNoExtraFieldsUsesEmptyValues(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(("BIN,CARDTYPE\n" + "512003,MASTERCARD\n").getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", true);

    BinDatabase.BinRecord record = db.lookup("5120031234567890");
    assertNotNull(record);
    assertTrue(record.getExtraValues().isEmpty());
    assertNull(record.getValue("anything"));
  }
}

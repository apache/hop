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

package org.apache.hop.naming.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.naming.metadata.NamingCaseStyle;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingWordSeparator;
import org.junit.jupiter.api.Test;

class NamingEngineTest {

  @Test
  void nullAndEmpty() {
    NamingScheme scheme = lowerUnderscore();
    assertNull(NamingEngine.apply(scheme, null));
    assertEquals("", NamingEngine.apply(scheme, ""));
  }

  @Test
  void orderIdLowerUnderscore() {
    assertEquals("order_id", NamingEngine.apply(lowerUnderscore(), "Order ID"));
  }

  @Test
  void orderIdAsIsUnderscore() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.AS_IS.getCode());
    scheme.setWordSeparator(NamingWordSeparator.UNDERSCORE.getCode());
    scheme.setRemoveSpecialCharacters(false);
    assertEquals("Order_ID", NamingEngine.apply(scheme, "Order ID"));
  }

  @Test
  void orderIdCamel() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.CAMEL.getCode());
    scheme.setRemoveSpecialCharacters(false);
    assertEquals("orderId", NamingEngine.apply(scheme, "Order ID"));
  }

  @Test
  void orderIdPascal() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.PASCAL.getCode());
    scheme.setRemoveSpecialCharacters(false);
    assertEquals("OrderId", NamingEngine.apply(scheme, "Order ID"));
  }

  @Test
  void snakeToCamel() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.CAMEL.getCode());
    assertEquals("orderId", NamingEngine.apply(scheme, "order_id"));
  }

  @Test
  void kebabToUpperSnake() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.UPPER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.UNDERSCORE.getCode());
    assertEquals("ORDER_ID", NamingEngine.apply(scheme, "order-id"));
  }

  @Test
  void collapseAndTrimWhitespace() {
    assertEquals("foo_bar", NamingEngine.apply(lowerUnderscore(), "  foo   bar  "));
  }

  @Test
  void removeSpecialCharacters() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setRemoveSpecialCharacters(true);
    scheme.setExtraDelimiters("#");
    assertEquals("field_name", NamingEngine.apply(scheme, "field#name!"));
  }

  @Test
  void dashSeparator() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.LOWER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.DASH.getCode());
    assertEquals("order-id", NamingEngine.apply(scheme, "Order ID"));
  }

  @Test
  void spaceSeparator() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.LOWER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.SPACE.getCode());
    assertEquals("order id", NamingEngine.apply(scheme, "Order ID"));
    assertEquals("read customers", NamingEngine.apply(scheme, "read_customers"));
  }

  @Test
  void capitalizeFirstWordWithSpace() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.LOWER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.SPACE.getCode());
    scheme.setCapitalizeFirstWord(true);
    assertEquals("Table input", NamingEngine.apply(scheme, "table_input"));
    assertEquals("Read customers", NamingEngine.apply(scheme, "read_customers"));
    assertEquals("Order id", NamingEngine.apply(scheme, "Order ID"));
  }

  @Test
  void capitalizeFirstWordLeavesAlreadyCapital() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.AS_IS.getCode());
    scheme.setWordSeparator(NamingWordSeparator.SPACE.getCode());
    scheme.setRemoveSpecialCharacters(false);
    scheme.setCapitalizeFirstWord(true);
    assertEquals("Table Input", NamingEngine.apply(scheme, "Table Input"));
  }

  @Test
  void prefixAndSuffix() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setPrefix("fld_");
    scheme.setSuffix("_x");
    assertEquals("fld_order_id_x", NamingEngine.apply(scheme, "Order ID"));
  }

  @Test
  void camelCaseInputSplit() {
    NamingScheme scheme = lowerUnderscore();
    assertEquals("order_id", NamingEngine.apply(scheme, "orderId"));
  }

  @Test
  void nullSchemeUsesDefaults() {
    assertEquals("order_id", NamingEngine.apply(null, "Order ID"));
  }

  @Test
  void fileKeepsParentAndExtension() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setType("file");
    assertEquals("/data/order_id.csv", NamingEngine.apply(scheme, "/data/Order ID.csv", "file"));
    assertEquals(
        "s3://bucket/dir/my_file.csv",
        NamingEngine.apply(scheme, "s3://bucket/dir/My File.csv", "file"));
    assertEquals(
        "C:\\data\\order_id.csv", NamingEngine.apply(scheme, "C:\\data\\Order ID.csv", "file"));
    assertEquals("order_id.csv", NamingEngine.apply(scheme, "Order ID.csv", "file"));
  }

  @Test
  void fileGeneralSchemeStillUsesKind() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setType("general");
    assertEquals("/data/order_id.csv", NamingEngine.apply(scheme, "/data/Order ID.csv", "file"));
  }

  @Test
  void fileDoesNotSplitLeadingDot() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setRemoveSpecialCharacters(false);
    assertEquals("/data/.htaccess", NamingEngine.apply(scheme, "/data/.htaccess", "file"));
  }

  @Test
  void folderKeepsParentAndTrailingSlash() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setType("folder");
    assertEquals("/data/my_folder", NamingEngine.apply(scheme, "/data/My Folder", "folder"));
    assertEquals("/data/my_folder/", NamingEngine.apply(scheme, "/data/My Folder/", "folder"));
    assertEquals("my_folder", NamingEngine.apply(scheme, "My Folder", "folder"));
  }

  @Test
  void folderDoesNotSplitExtension() {
    NamingScheme scheme = lowerUnderscore();
    scheme.setRemoveSpecialCharacters(false);
    assertEquals("/data/my.folder", NamingEngine.apply(scheme, "/data/My.Folder", "folder"));
  }

  @Test
  void shouldSkipVariables() {
    assertTrue(NamingEngine.shouldSkip("${PROJECT_HOME}/Order ID.csv"));
    assertFalse(NamingEngine.shouldSkip("/data/Order ID.csv"));
  }

  private static NamingScheme lowerUnderscore() {
    NamingScheme scheme = new NamingScheme();
    scheme.setCaseStyle(NamingCaseStyle.LOWER.getCode());
    scheme.setWordSeparator(NamingWordSeparator.UNDERSCORE.getCode());
    scheme.setRemoveSpecialCharacters(true);
    scheme.setCollapseRepeatedSeparators(true);
    scheme.setTrimEdgeSeparators(true);
    return scheme;
  }
}

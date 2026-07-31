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
import static org.junit.jupiter.api.Assertions.assertNull;

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

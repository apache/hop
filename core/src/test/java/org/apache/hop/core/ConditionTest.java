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

package org.apache.hop.core;

import static org.apache.hop.core.Condition.Function;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ConditionTest {

  @Test
  void testNegatedTrueFuncEvaluatesAsFalse() throws Exception {
    String left = "test_filed";
    String right = "test_value";
    Function func = Function.TRUE;
    boolean negate = true;

    Condition condition = new Condition(negate, left, func, right, null);
    assertFalse(condition.evaluate(new RowMeta(), new Object[] {"test"}));
  }

  @Test
  void testCacheInvalidationTest() throws Exception {
    IRowMeta rowMeta1 = new RowMeta();
    rowMeta1.addValueMeta(new ValueMetaNumber("name1"));
    rowMeta1.addValueMeta(new ValueMetaNumber("name2"));
    rowMeta1.addValueMeta(new ValueMetaNumber("name3"));

    IRowMeta rowMeta2 = new RowMeta();
    rowMeta2.addValueMeta(new ValueMetaNumber("name2"));
    rowMeta2.addValueMeta(new ValueMetaNumber("name1"));
    rowMeta2.addValueMeta(new ValueMetaNumber("name3"));

    String left = "name1";
    String right = "name3";
    Condition condition = new Condition(left, Function.EQUAL, right, null);

    assertTrue(condition.evaluate(rowMeta1, new Object[] {1.0, 2.0, 1.0}));
    assertTrue(condition.evaluate(rowMeta2, new Object[] {2.0, 1.0, 1.0}));
  }

  @Test
  void testNullLessThanNumberEvaluatesAsFalse() throws Exception {
    IRowMeta rowMeta1 = new RowMeta();
    rowMeta1.addValueMeta(new ValueMetaInteger("name1"));

    String left = "name1";
    ValueMetaAndData rightExact = new ValueMetaAndData(new ValueMetaInteger("name1"), -10L);

    Condition condition = new Condition(left, Function.SMALLER, null, rightExact);
    assertFalse(condition.evaluate(rowMeta1, new Object[] {null, "test"}));

    condition = new Condition(left, Function.SMALLER_EQUAL, null, rightExact);
    assertFalse(condition.evaluate(rowMeta1, new Object[] {null, "test"}));
  }

  @Test
  void testSerialization() throws Exception {
    Document document = XmlHandler.loadXmlFile(getClass().getResourceAsStream("/condition.xml"));
    Node node = XmlHandler.getSubNode(document, Condition.XML_TAG);

    Condition condition = new Condition(node);

    assertNotNull(condition);
    assertEquals(2, condition.getChildren().size());
    Condition c1 = condition.getChildren().get(0);
    assertEquals("stateCode", c1.getLeftValueName());
    assertEquals("FL", c1.getRightValueString());

    Condition c2 = condition.getChildren().get(1);
    assertEquals("housenr", c2.getLeftValueName());
    assertEquals("100", c2.getRightValueString());
  }

  @Test
  void testSerialization2() throws Exception {
    Document document = XmlHandler.loadXmlFile(getClass().getResourceAsStream("/condition2.xml"));
    Node node = XmlHandler.getSubNode(document, Condition.XML_TAG);

    Condition condition = new Condition(node);

    assertNotNull(condition);
    assertEquals(0, condition.getChildren().size());

    assertEquals("id1", condition.getLeftValueName());
    assertEquals("rangeStart", condition.getRightValueName());
    assertNull(condition.getRightValue());
    assertEquals(Function.LARGER_EQUAL, condition.getFunction());
  }
}

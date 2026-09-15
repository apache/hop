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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.util.TestUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ConditionTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
    TestUtil.registerTestPluginTypes();
  }

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

  @Test
  void dateConstantWithMatchingMaskEvaluates() throws Exception {
    Condition condition = dateLessThanConstant("2022-01-01", "yyyy-MM-dd");
    assertDateLessThan(condition);
  }

  @Test
  void dateConstantWithLegacyCompatibleTextEvaluates() throws Exception {
    // Issue #3051: text stored in the canonical Hop date format, mask is the user format.
    Condition condition = dateLessThanConstant("2022/01/01 00:00:00.000", "yyyy-MM-dd");
    assertDateLessThan(condition);
  }

  @Test
  void dateConstantXmlRoundTripKeepsCustomMask() throws Exception {
    SimpleDateFormat iso = isoDate();
    ValueMetaDate dateMeta = new ValueMetaDate("constant");
    dateMeta.setConversionMask("yyyy-MM-dd");
    Condition original =
        new Condition(
            "date",
            Function.SMALLER,
            null,
            new ValueMetaAndData(dateMeta, iso.parse("2022-01-01")));

    Condition copy = new Condition(original.getXml());
    assertEquals("yyyy-MM-dd", copy.getRightValue().getMask());
    assertDateLessThan(copy);
  }

  @Test
  void dateConstantConstructorTextIsParseable() throws Exception {
    SimpleDateFormat iso = isoDate();
    Date date = iso.parse("2022-01-01");
    ValueMetaDate dateMeta = new ValueMetaDate("constant");
    dateMeta.setConversionMask("yyyy-MM-dd");

    Condition.CValue value = new Condition.CValue(new ValueMetaAndData(dateMeta, date));
    assertEquals("yyyy-MM-dd", value.getMask());
    assertInstanceOf(Date.class, value.createValueData());
  }

  @Test
  void dateConstantPersistsDefaultMaskWhenMissing() throws Exception {
    Date date = isoDate().parse("2022-01-01");
    Condition.CValue value =
        new Condition.CValue(new ValueMetaAndData(new ValueMetaDate("constant"), date));
    assertEquals(ValueMetaBase.DEFAULT_DATE_FORMAT_MASK, value.getMask());
    assertInstanceOf(Date.class, value.createValueData());
  }

  @Test
  void timestampConstantWithLegacyCompatibleTextConverts() throws Exception {
    Condition.CValue value = new Condition.CValue();
    value.setName("constant");
    value.setType("Timestamp");
    value.setText("2022/01/01 12:34:56.789");
    value.setMask("yyyy-MM-dd HH:mm:ss.SSS");
    value.setNullValue(false);
    value.setLength(-1);
    value.setPrecision(-1);

    assertNotNull(value.createValueData());
  }

  @Test
  void timestampConstantConstructorTextIsParseable() throws Exception {
    ValueMetaTimestamp timestampMeta = new ValueMetaTimestamp("constant");
    timestampMeta.setConversionMask("yyyy-MM-dd HH:mm:ss.SSS");
    Timestamp timestamp = Timestamp.valueOf("2022-01-01 12:34:56.789");

    Condition.CValue value = new Condition.CValue(new ValueMetaAndData(timestampMeta, timestamp));
    assertEquals("yyyy-MM-dd HH:mm:ss.SSS", value.getMask());
    assertNotNull(value.createValueData());
  }

  @Test
  void nullDateConstantStaysNull() throws Exception {
    Condition.CValue value =
        new Condition.CValue(new ValueMetaAndData(new ValueMetaDate("constant"), null));
    assertTrue(value.isNullValue());
    assertNull(value.createValueData());
  }

  @Test
  void unparseableDateConstantStillFails() {
    Condition.CValue value = new Condition.CValue();
    value.setName("constant");
    value.setType("Date");
    value.setText("not-a-date");
    value.setMask("yyyy-MM-dd");
    value.setNullValue(false);
    value.setLength(-1);
    value.setPrecision(-1);

    assertThrows(HopException.class, value::createValueData);
  }

  @Test
  void integerAndBigNumberConstantsStillConvert() throws Exception {
    Condition.CValue integer =
        new Condition.CValue(new ValueMetaAndData(new ValueMetaInteger("constant"), 100L));
    assertEquals(100L, integer.createValueData());

    Condition.CValue bigNumber =
        new Condition.CValue(
            new ValueMetaAndData(new ValueMetaBigNumber("constant"), new BigDecimal("123.45")));
    assertEquals(0, new BigDecimal("123.45").compareTo((BigDecimal) bigNumber.createValueData()));
  }

  private static Condition dateLessThanConstant(String text, String mask) {
    Condition.CValue constant = new Condition.CValue();
    constant.setName("constant");
    constant.setType("Date");
    constant.setText(text);
    constant.setMask(mask);
    constant.setNullValue(false);
    constant.setLength(-1);
    constant.setPrecision(-1);

    Condition condition = new Condition();
    condition.setLeftValueName("date");
    condition.setFunction(Function.SMALLER);
    condition.setRightValue(constant);
    return condition;
  }

  private static void assertDateLessThan(Condition condition) throws Exception {
    SimpleDateFormat iso = isoDate();
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("date"));

    assertTrue(condition.evaluate(rowMeta, new Object[] {iso.parse("2021-12-31")}));
    assertFalse(condition.evaluate(rowMeta, new Object[] {iso.parse("2022-01-01")}));
    assertFalse(condition.evaluate(rowMeta, new Object[] {iso.parse("2022-01-02")}));
  }

  private static SimpleDateFormat isoDate() {
    SimpleDateFormat iso = new SimpleDateFormat("yyyy-MM-dd");
    iso.setLenient(false);
    return iso;
  }
}

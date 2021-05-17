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

package org.apache.hop.core.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.jdbc.ThinUtil;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThinUtilTest {

  @Before
  public void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testFindClauseNullOrEmptyString() throws HopException {
    assertNull(ThinUtil.findClause(null, null));
    assertNull(ThinUtil.findClause("", null));
  }

  @Test
  public void testFindSelectFromNotFound() throws HopException {
    assertNull(ThinUtil.findClause("Select * From Test", "WHERE"));
  }

  @Test
  public void testFindSelectFromFound() throws HopException {
    assertEquals("*", ThinUtil.findClause("Select * From Test", "SELECT", "FROM"));
  }

  @Test
  public void testFindClauseSkipsChars() throws HopException {
    assertNull(ThinUtil.findClause("'Select' * From Test", "SELECT", "FROM"));
  }

  @Test
  public void testAttemptDateValueExtraction() throws HopException {
    ValueMetaAndData timestamp =
        ThinUtil.attemptDateValueExtraction("TIMESTAMP '2014-01-01 00:00:00'");
    ValueMetaAndData date = ThinUtil.attemptDateValueExtraction("DATE '2014-01-01'");

    assertNotNull(timestamp);
    assertEquals("2014/01/01 00:00:00.000", timestamp.toString());

    assertNotNull(date);
    assertEquals("2014/01/01 00:00:00.000", date.toString());
  }

  @Test
  public void testLikePatternMatching() {
    try {
      ThinUtil.like("foo", null);
      fail("Null pattern should not be allowed");
    } catch (IllegalArgumentException e) {
      assertNotNull(e);
    }

    assertTrue("Exact Matching", ThinUtil.like("foobar", "foobar"));

    assertTrue("_ Matching", ThinUtil.like("foobar", "f__b_r"));
    assertTrue("* Matching", ThinUtil.like("foobar", "foo%"));

    assertTrue("Regex Escaping", ThinUtil.like("foo\\*?[]()bar", "%\\*?[]()%"));

    assertFalse("False Match", ThinUtil.like("foo", "bar"));
  }

  @Test
  public void testGetValueMeta() throws SQLException {
    IValueMeta testValue;
    String expectedName = "testName";
    testValue = ThinUtil.getValueMeta(expectedName, Types.BIGINT);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.INTEGER);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.SMALLINT);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.CHAR);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_STRING, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.VARCHAR);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_STRING, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.CLOB);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_STRING, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.DATE);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_DATE, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.TIMESTAMP);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_DATE, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.TIME);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_DATE, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.DECIMAL);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_BIGNUMBER, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.DOUBLE);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.FLOAT);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.BOOLEAN);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_BOOLEAN, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.BIT);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_BOOLEAN, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.BINARY);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_BINARY, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.BLOB);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_BINARY, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.OTHER);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_NONE, testValue.getType());

    testValue = ThinUtil.getValueMeta(expectedName, Types.NULL);
    assertEquals(expectedName, testValue.getName());
    assertEquals(IValueMeta.TYPE_NONE, testValue.getType());

    try {
      ThinUtil.getValueMeta(expectedName, Integer.MIN_VALUE);
      fail();
    } catch (SQLException expected) {
      // Do nothing, there is no Sql Type for Integer.MIN_VALUE, an exception was thrown as
      // expected.
    }
  }

  @Test
  public void checkValueMetaTypeToSqlTypeConsistency() throws SQLException {
    class TypeMap {
      IValueMeta valueMeta;
      int sqlType;
      String sqlDesc;

      TypeMap(int valueMetaType, int sqlType, String desc) {
        this.valueMeta = mock(IValueMeta.class);
        when(valueMeta.getType()).thenReturn(valueMetaType);
        this.sqlType = sqlType;
        this.sqlDesc = desc;
      }
    }
    TypeMap[] typeMaps =
        new TypeMap[] {
          new TypeMap(IValueMeta.TYPE_STRING, Types.VARCHAR, "VARCHAR"),
          new TypeMap(IValueMeta.TYPE_DATE, Types.TIMESTAMP, "TIMESTAMP"),
          new TypeMap(IValueMeta.TYPE_INTEGER, Types.BIGINT, "BIGINT"),
          new TypeMap(IValueMeta.TYPE_NUMBER, Types.DOUBLE, "DOUBLE"),
          new TypeMap(IValueMeta.TYPE_BIGNUMBER, Types.DECIMAL, "DECIMAL"),
          new TypeMap(IValueMeta.TYPE_BOOLEAN, Types.BOOLEAN, "BOOLEAN"),
          new TypeMap(IValueMeta.TYPE_BINARY, Types.BLOB, "BLOB"),
          new TypeMap(IValueMeta.TYPE_NONE, Types.OTHER, "OTHER"),
        };
    for (TypeMap map : typeMaps) {
      assertEquals(map.sqlDesc, ThinUtil.getSqlTypeDesc(map.valueMeta));
      assertEquals(map.sqlType, ThinUtil.getSqlType(map.valueMeta));
      assertEquals(map.valueMeta.getType(), ThinUtil.getValueMeta("test", map.sqlType).getType());
    }
  }

  @Test
  public void testAttemptDateValueExtraction2() throws HopException {
    ValueMetaAndData result = ThinUtil.attemptDateValueExtraction("[2015/01/02 03:04:56.789]");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_DATE, result.getValueMeta().getType());
    assertEquals("2015/01/02 03:04:56.789", result.getValueMeta().getString(result.getValueData()));

    // assertNull( ThinUtil.attemptDateValueExtraction( null ) );
    assertNull(ThinUtil.attemptDateValueExtraction(""));
    assertNull(ThinUtil.attemptDateValueExtraction("[]"));
    assertNull(ThinUtil.attemptDateValueExtraction("[notadate]"));
    assertNull(ThinUtil.attemptDateValueExtraction("2015/01/02 03:04:56.789"));
  }

  @Test
  public void testAttemptIntegerValueExtraction() {
    ValueMetaAndData result = ThinUtil.attemptIntegerValueExtraction("12345");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_INTEGER, result.getValueMeta().getType());
    assertEquals(12345L, result.getValueData());

    // assertNull( ThinUtil.attemptIntegerValueExtraction( null ) );
    assertNull(ThinUtil.attemptIntegerValueExtraction(""));
    assertNull(ThinUtil.attemptIntegerValueExtraction("123.45"));
  }

  @Test
  public void testAttemptNumberValueExtraction() {
    ValueMetaAndData result = ThinUtil.attemptNumberValueExtraction("12345.678");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_NUMBER, result.getValueMeta().getType());
    assertEquals(12345.678, result.getValueData());

    // assertNull( ThinUtil.attemptNumberValueExtraction( null ) );
    assertNull(ThinUtil.attemptNumberValueExtraction(""));
    assertNull(ThinUtil.attemptNumberValueExtraction("abcde"));
  }

  @Test
  public void testAttemptBigNumberValueExtraction() {
    ValueMetaAndData result =
        ThinUtil.attemptBigNumberValueExtraction("1234567890123456789.0987654321");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_BIGNUMBER, result.getValueMeta().getType());
    assertEquals(new BigDecimal("1234567890123456789.0987654321"), result.getValueData());

    // assertNull( ThinUtil.attemptBigNumberValueExtraction( null ) );
    assertNull(ThinUtil.attemptBigNumberValueExtraction(""));
    assertNull(ThinUtil.attemptBigNumberValueExtraction("abcde"));
  }

  @Test
  public void testAttemptStringValueExtraction() throws HopException {
    ValueMetaAndData result = ThinUtil.attemptStringValueExtraction("'testValue'");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta().getType());
    assertEquals("testValue", result.getValueData());

    result = ThinUtil.attemptStringValueExtraction("'test\'\'Value'");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_STRING, result.getValueMeta().getType());
    assertEquals("test\'Value", result.getValueData());

    // assertNull( ThinUtil.attemptStringValueExtraction( null ) );
    assertNull(ThinUtil.attemptStringValueExtraction(""));
    assertNull(ThinUtil.attemptStringValueExtraction("abcde"));
  }

  @Test
  public void testAttemptBooleanValueExtraction() throws HopException {
    ValueMetaAndData result = ThinUtil.attemptBooleanValueExtraction("TrUe");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_BOOLEAN, result.getValueMeta().getType());
    assertEquals(Boolean.TRUE, result.getValueData());

    result = ThinUtil.attemptBooleanValueExtraction("fAlSe");
    assertNotNull(result);
    assertEquals(IValueMeta.TYPE_BOOLEAN, result.getValueMeta().getType());
    assertEquals(Boolean.FALSE, result.getValueData());

    assertNull(ThinUtil.attemptBooleanValueExtraction(null));
    assertNull(ThinUtil.attemptBooleanValueExtraction(""));
    assertNull(ThinUtil.attemptBooleanValueExtraction("abcde"));
  }

  @Test
  public void testStripQuotesIfNoWhitespace() {
    // map of tests to expected result
    Map<String, String> testStrings =
        new ImmutableMap.Builder<String, String>()
            .put("\"quotedNospace\"", "quotedNospace")
            .put("unquoted", "unquoted")
            .put("\"quoted space\"", "\"quoted space\"")
            .put("\"field\" \"fieldAlias\"", "\"field\" \"fieldAlias\"")
            .put("\"field with space\" \"fieldAlias\"", "\"field with space\" \"fieldAlias\"")
            .put("\"field\" AS \"field Alias\"", "\"field\" AS \"field Alias\"")
            .build();

    for (String test : testStrings.keySet()) {
      for (char quote : new char[] {'\'', '"', '`'}) {
        assertEquals(
            testStrings.get(test).replace('"', quote),
            ThinUtil.stripQuotesIfNoWhitespace(test.replace('"', quote), quote));
      }
    }
  }

  @Test
  public void testSkipChars() {
    List<String> checkList = new ArrayList<String>();
    checkList.add("SELECT functionN \"transaction:a\" LIMIT ? OFFSET ?");
    checkList.add("SELECT \"functionN\" \"transaction:a\" LIMIT ? OFFSET ?");
    checkList.add("SELECT \"function'N'\" \"transaction:a\" LIMIT ? OFFSET ?");
    checkList.add("SELECT \"function\"\"N\"\"\" \"transaction:a\" LIMIT ? OFFSET ?");

    for (String sql : checkList) {
      checkSkipChars(sql);
    }
  }

  private void checkSkipChars(String _sql) {
    List<Integer> placeholderIndexes = new ArrayList<Integer>();
    try {
      int index = 0;
      while (index < _sql.length()) {
        index = ThinUtil.skipChars(_sql, index, '\'', '"');
        if (index < _sql.length()) {
          if (_sql.charAt(index) == '?') {
            // placeholder found.
            placeholderIndexes.add(index);
          }
        }

        index++;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    Assert.assertEquals(placeholderIndexes.size(), 2);
  }

  @Test
  public void testUnQuote() {
    Assert.assertEquals("string", ThinUtil.unQuote("\"string\""));
    Assert.assertEquals("\"string\".\"string2\"", ThinUtil.unQuote("\"string\".\"string2\""));
    Assert.assertEquals("string\"N", ThinUtil.unQuote("\"string\"N\""));
    Assert.assertEquals("string\"\"N\"\"", ThinUtil.unQuote("\"string\"\"N\"\"\""));
  }
}

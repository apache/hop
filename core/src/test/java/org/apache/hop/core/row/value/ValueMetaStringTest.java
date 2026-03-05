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

package org.apache.hop.core.row.value;

import static junit.framework.TestCase.failNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class ValueMetaStringTest {
  private static final String BASE_VALUE = "Some text";
  private static final String TEST_VALUE = "Some text";

  private ConfigurableMeta meta;

  @BeforeEach
  void setUp() {
    meta = new ConfigurableMeta(BASE_VALUE);
  }

  @AfterEach
  void tearDown() {
    meta = null;
  }

  @Test
  void testGetNativeDataEmptyIsNotNull() throws Exception {
    meta.setNullsAndEmptyAreDifferent(true);

    assertEquals(BASE_VALUE, meta.getNativeDataType(BASE_VALUE));
    assertEquals(TEST_VALUE, meta.getNativeDataType(TEST_VALUE));
    assertNull(meta.getNativeDataType(null));
    assertEquals("1", meta.getNativeDataType(1));
    assertEquals("1.0", meta.getNativeDataType(1.0));

    Date d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")).parse("2012-11-10 09:08:07.654");
    assertEquals(d.toString(), meta.getNativeDataType(d));

    Timestamp ts = Timestamp.valueOf("2012-11-10 09:08:07.654321");
    assertEquals("2012-11-10 09:08:07.654321", meta.getNativeDataType(ts));

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    assertEquals("", meta.getNativeDataType(""));
    assertEquals("1", meta.getNativeDataType("1"));
    assertEquals("    ", meta.getNativeDataType("    "));
    assertEquals("  1  ", meta.getNativeDataType("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    assertEquals("", meta.getNativeDataType(""));
    assertEquals("1", meta.getNativeDataType("1"));
    assertEquals("", meta.getNativeDataType("    "));
    assertEquals("1  ", meta.getNativeDataType("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    assertEquals("", meta.getNativeDataType(""));
    assertEquals("1", meta.getNativeDataType("1"));
    assertEquals("", meta.getNativeDataType("    "));
    assertEquals("  1", meta.getNativeDataType("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    assertEquals("", meta.getNativeDataType(""));
    assertEquals("1", meta.getNativeDataType("1"));
    assertEquals("", meta.getNativeDataType("    "));
    assertEquals("1", meta.getNativeDataType("  1  "));
  }

  @Test
  void testGetNativeDataEmptyIsNull() throws Exception {
    meta.setNullsAndEmptyAreDifferent(false);

    assertEquals(BASE_VALUE, meta.getNativeDataType(BASE_VALUE));
    assertEquals(TEST_VALUE, meta.getNativeDataType(TEST_VALUE));
    assertNull(meta.getNativeDataType(null));
    assertEquals("1", meta.getNativeDataType(1));
    assertEquals("1.0", meta.getNativeDataType(1.0));

    Date d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")).parse("2012-11-10 09:08:07.654");
    assertEquals(d.toString(), meta.getNativeDataType(d));

    Timestamp ts = Timestamp.valueOf("2012-11-10 09:08:07.654321");
    assertEquals("2012-11-10 09:08:07.654321", meta.getNativeDataType(ts));

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    // assertEquals( null, meta.getNativeDataType( "" ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("")); //  is it correct?
    assertEquals("1", meta.getNativeDataType("1"));
    assertEquals("    ", meta.getNativeDataType("    "));
    assertEquals("  1  ", meta.getNativeDataType("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    // assertEquals( null, meta.getNativeDataType( "" ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("")); //  is it correct?
    assertEquals("1", meta.getNativeDataType("1"));
    // assertEquals( null, meta.getNativeDataType( "    " ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("    ")); //  is it correct?
    assertEquals("1  ", meta.getNativeDataType("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    // assertEquals( null, meta.getNativeDataType( "" ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("")); //  is it correct?
    assertEquals("1", meta.getNativeDataType("1"));
    // assertEquals( null, meta.getNativeDataType( "    " ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("    ")); //  is it correct?
    assertEquals("  1", meta.getNativeDataType("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    // assertEquals( null, meta.getNativeDataType( "" ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("")); //  is it correct?
    assertEquals("1", meta.getNativeDataType("1"));
    // assertEquals( null, meta.getNativeDataType( "    " ) ); // is it correct?
    assertEquals("", meta.getNativeDataType("    ")); //  is it correct?
    assertEquals("1", meta.getNativeDataType("  1  "));
  }

  @Test
  void testIsNullEmptyIsNotNull() throws HopValueException {
    meta.setNullsAndEmptyAreDifferent(true);

    assertTrue(meta.isNull(null));
    assertFalse(meta.isNull(""));

    assertFalse(meta.isNull("1"));

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    assertFalse(meta.isNull("    "));
    assertFalse(meta.isNull("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    assertFalse(meta.isNull("    "));
    assertFalse(meta.isNull("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    assertFalse(meta.isNull("    "));
    assertFalse(meta.isNull("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    assertFalse(meta.isNull("    "));
    assertFalse(meta.isNull("  1  "));
  }

  @Test
  void testIsNullEmptyIsNull() throws HopValueException {
    meta.setNullsAndEmptyAreDifferent(false);

    assertTrue(meta.isNull(null));
    assertTrue(meta.isNull(""));

    assertFalse(meta.isNull("1"));

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    assertFalse(meta.isNull("    "));
    assertFalse(meta.isNull(meta.getString("    ")));

    assertFalse(meta.isNull("  1  "));
    assertFalse(meta.isNull(meta.getString("  1  ")));

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    // assertEquals( true, meta.isNull( "    " ) ); // is it correct?
    assertFalse(meta.isNull("    ")); //  is it correct?
    assertTrue(meta.isNull(meta.getString("    ")));

    assertFalse(meta.isNull("  1  "));
    assertFalse(meta.isNull(meta.getString("  1  ")));

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    // assertEquals( true, meta.isNull( "    " ) ); // is it correct?
    assertFalse(meta.isNull("    ")); //  is it correct?
    assertTrue(meta.isNull(meta.getString("    ")));

    assertFalse(meta.isNull("  1  "));
    assertFalse(meta.isNull(meta.getString("  1  ")));

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    // assertEquals( true, meta.isNull( "    " ) ); // is it correct?
    assertFalse(meta.isNull("    ")); //  is it correct?
    assertTrue(meta.isNull(meta.getString("    ")));

    assertFalse(meta.isNull("  1  "));
    assertFalse(meta.isNull(meta.getString("  1  ")));
  }

  @Test
  void testGetStringEmptyIsNotNull() throws HopValueException {
    meta.setNullsAndEmptyAreDifferent(true);

    assertNull(meta.getString(null));
    assertEquals("", meta.getString(""));

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    assertEquals("    ", meta.getString("    "));
    assertEquals("  1  ", meta.getString("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    assertEquals("", meta.getString("    "));
    assertEquals("1  ", meta.getString("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    assertEquals("", meta.getString("    "));
    assertEquals("  1", meta.getString("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    assertEquals("", meta.getString("    "));
    assertEquals("1", meta.getString("  1  "));
  }

  @Test
  void testGetStringEmptyIsNull() throws HopValueException {
    meta.setNullsAndEmptyAreDifferent(false);

    assertNull(meta.getString(null));
    // assertEquals( null, meta.getString( "" ) ); //  is it correct?
    assertEquals("", meta.getString("")); //  is it correct?

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    assertEquals("    ", meta.getString("    "));
    assertEquals("  1  ", meta.getString("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);
    // assertEquals( null, meta.getString( "    " ) ); //  is it correct?
    assertEquals("", meta.getString("    ")); //  is it correct?
    assertEquals("1  ", meta.getString("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);
    // assertEquals( null, meta.getString( "    " ) ); //  is it correct?
    assertEquals("", meta.getString("    ")); //  is it correct?
    assertEquals("  1", meta.getString("  1  "));

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    // assertEquals( null, meta.getString( "    " ) ); //  is it correct?
    assertEquals("", meta.getString("    ")); //  is it correct?
    assertEquals("1", meta.getString("  1  "));
  }

  @Test
  void testCompareEmptyIsNotNull() throws HopValueException {
    meta.setNullsAndEmptyAreDifferent(true);

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(-1, meta.compare(null, "")); // null < ""
    assertSignum(-1, meta.compare(null, " ")); // null < " "
    assertSignum(-1, meta.compare(null, " 1")); // null < " 1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < " 1 "
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1 "

    assertSignum(1, meta.compare("", null)); // "" > null
    assertSignum(0, meta.compare("", "")); // "" == ""
    assertSignum(-1, meta.compare("", " ")); // "" < " "
    assertSignum(-1, meta.compare("", " 1")); // "" < " 1"
    assertSignum(-1, meta.compare("", " 1 ")); // "" < " 1 "
    assertSignum(-1, meta.compare("", "1")); // "" < "1"
    assertSignum(-1, meta.compare("", "1 ")); // "" < "1 "

    assertSignum(1, meta.compare(" ", null)); // " " > null
    assertSignum(1, meta.compare(" ", "")); // " " > ""
    assertSignum(0, meta.compare(" ", " ")); // " " == " "
    assertSignum(-1, meta.compare(" ", " 1")); // " " < " 1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // " " < " 1 "
    assertSignum(-1, meta.compare(" ", "1")); // " " < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // " " < "1 "

    assertSignum(1, meta.compare(" 1", null)); // " 1" > null
    assertSignum(1, meta.compare(" 1", "")); // " 1" > ""
    assertSignum(1, meta.compare(" 1", " ")); // " 1" > " "
    assertSignum(0, meta.compare(" 1", " 1")); // " 1" == " 1"
    assertSignum(-1, meta.compare(" 1", " 1 ")); // " 1" < " 1 "
    assertSignum(-1, meta.compare(" 1", "1")); // " 1" < "1"
    assertSignum(-1, meta.compare(" 1", "1 ")); // " 1" < "1 "

    assertSignum(1, meta.compare(" 1 ", null)); // " 1 " > null
    assertSignum(1, meta.compare(" 1 ", "")); // " 1 " > ""
    assertSignum(1, meta.compare(" 1 ", " ")); // " 1 " > " "
    assertSignum(1, meta.compare(" 1 ", " 1")); // " 1 " > " 1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // " 1 " == " 1 "
    assertSignum(-1, meta.compare(" 1 ", "1")); // " 1 " < "1"
    assertSignum(-1, meta.compare(" 1 ", "1 ")); // " 1 " < "1 "

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > ""
    assertSignum(1, meta.compare("1", " ")); // "1" > " "
    assertSignum(1, meta.compare("1", " 1")); // "1" > " 1"
    assertSignum(1, meta.compare("1", " 1 ")); // "1" > " 1 "
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(-1, meta.compare("1", "1 ")); // "1" < "1 "

    assertSignum(1, meta.compare("1 ", null)); // "1 " > null
    assertSignum(1, meta.compare("1 ", "")); // "1 " > ""
    assertSignum(1, meta.compare("1 ", " ")); // "1 " > " "
    assertSignum(1, meta.compare("1 ", " 1")); // "1 " > " 1"
    assertSignum(1, meta.compare("1 ", " 1 ")); // "1 " > " 1 "
    assertSignum(1, meta.compare("1 ", "1")); // "1 " > "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1 " == "1 "

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(-1, meta.compare(null, "")); // null < ""
    assertSignum(-1, meta.compare(null, " ")); // null < ""
    assertSignum(-1, meta.compare(null, " 1")); // null < "1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < "1 "
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1 "

    assertSignum(1, meta.compare("", null)); // "" > null
    assertSignum(0, meta.compare("", "")); // "" == ""
    assertSignum(0, meta.compare("", " ")); // "" == ""
    assertSignum(-1, meta.compare("", " 1")); // "" < "1"
    assertSignum(-1, meta.compare("", " 1 ")); // "" < "1 "
    assertSignum(-1, meta.compare("", "1")); // "" < "1"
    assertSignum(-1, meta.compare("", "1 ")); // "" < "1 "

    assertSignum(1, meta.compare(" ", null)); // "" > null
    assertSignum(0, meta.compare(" ", "")); // "" == ""
    assertSignum(0, meta.compare(" ", " ")); // "" == ""
    assertSignum(-1, meta.compare(" ", " 1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // "" < "1 "
    assertSignum(-1, meta.compare(" ", "1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // "" < "1 "

    assertSignum(1, meta.compare(" 1", null)); // "1" > null
    assertSignum(1, meta.compare(" 1", "")); // "1" > ""
    assertSignum(1, meta.compare(" 1", " ")); // "1" > ""
    assertSignum(0, meta.compare(" 1", " 1")); // "1" == "1"
    assertSignum(-1, meta.compare(" 1", " 1 ")); // "1" < "1 "
    assertSignum(0, meta.compare(" 1", "1")); // "1" == "1"
    assertSignum(-1, meta.compare(" 1", "1 ")); // "1" < "1 "

    assertSignum(1, meta.compare(" 1 ", null)); // "1 " > null
    assertSignum(1, meta.compare(" 1 ", "")); // "1 " > ""
    assertSignum(1, meta.compare(" 1 ", " ")); // "1 " > ""
    assertSignum(1, meta.compare(" 1 ", " 1")); // "1 " > "1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // "1 " == "1 "
    assertSignum(1, meta.compare(" 1 ", "1")); // "1 " > "1"
    assertSignum(0, meta.compare(" 1 ", "1 ")); // "1 " == "1 "

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > ""
    assertSignum(1, meta.compare("1", " ")); // "1" > ""
    assertSignum(0, meta.compare("1", " 1")); // "1" == "1"
    assertSignum(-1, meta.compare("1", " 1 ")); // "1" < "1 "
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(-1, meta.compare("1", "1 ")); // "1" < "1 "

    assertSignum(1, meta.compare("1 ", null)); // "1 " > null
    assertSignum(1, meta.compare("1 ", "")); // "1 " > ""
    assertSignum(1, meta.compare("1 ", " ")); // "1 " > ""
    assertSignum(1, meta.compare("1 ", " 1")); // "1 " > "1"
    assertSignum(0, meta.compare("1 ", " 1 ")); // "1 " == "1 "
    assertSignum(1, meta.compare("1 ", "1")); // "1 " > "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1 " == "1 "

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(-1, meta.compare(null, "")); // null < ""
    assertSignum(-1, meta.compare(null, " ")); // null < ""
    assertSignum(-1, meta.compare(null, " 1")); // null < " 1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < " 1"
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1"

    assertSignum(1, meta.compare("", null)); // "" > null
    assertSignum(0, meta.compare("", "")); // "" == ""
    assertSignum(0, meta.compare("", " ")); // "" == ""
    assertSignum(-1, meta.compare("", " 1")); // "" < " 1"
    assertSignum(-1, meta.compare("", " 1 ")); // "" < " 1"
    assertSignum(-1, meta.compare("", "1")); // "" < "1"
    assertSignum(-1, meta.compare("", "1 ")); // "" < "1"

    assertSignum(1, meta.compare(" ", null)); // "" > null
    assertSignum(0, meta.compare(" ", "")); // "" == ""
    assertSignum(0, meta.compare(" ", " ")); // "" == ""
    assertSignum(-1, meta.compare(" ", " 1")); // "" < " 1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // "" < " 1"
    assertSignum(-1, meta.compare(" ", "1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // "" < "1"

    assertSignum(1, meta.compare(" 1", null)); // " 1" > null
    assertSignum(1, meta.compare(" 1", "")); // " 1" > ""
    assertSignum(1, meta.compare(" 1", " ")); // " 1" > ""
    assertSignum(0, meta.compare(" 1", " 1")); // " 1" == " 1"
    assertSignum(0, meta.compare(" 1", " 1 ")); // " 1" == " 1"
    assertSignum(-1, meta.compare(" 1", "1")); // " 1" < "1"
    assertSignum(-1, meta.compare(" 1", "1 ")); // " 1" < "1"

    assertSignum(1, meta.compare(" 1 ", null)); // " 1" > null
    assertSignum(1, meta.compare(" 1 ", "")); // " 1" > ""
    assertSignum(1, meta.compare(" 1 ", " ")); // " 1" > ""
    assertSignum(0, meta.compare(" 1 ", " 1")); // " 1" == " 1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // " 1" == " 1"
    assertSignum(-1, meta.compare(" 1 ", "1")); // " 1" < "1"
    assertSignum(-1, meta.compare(" 1 ", "1 ")); // " 1" < "1"

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > ""
    assertSignum(1, meta.compare("1", " ")); // "1" > ""
    assertSignum(1, meta.compare("1", " 1")); // "1" > " 1"
    assertSignum(1, meta.compare("1", " 1 ")); // "1" > " 1"
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1 ", null)); // "1" > null
    assertSignum(1, meta.compare("1 ", "")); // "1" > ""
    assertSignum(1, meta.compare("1 ", " ")); // "1" > ""
    assertSignum(1, meta.compare("1 ", " 1")); // "1" > " 1"
    assertSignum(1, meta.compare("1 ", " 1 ")); // "1" > " 1"
    assertSignum(0, meta.compare("1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1" == "1"

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(-1, meta.compare(null, "")); // null < ""
    assertSignum(-1, meta.compare(null, " ")); // null < ""
    assertSignum(-1, meta.compare(null, " 1")); // null < "1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < "1"
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1"

    assertSignum(1, meta.compare("", null)); // "" > null
    assertSignum(0, meta.compare("", "")); // "" == ""
    assertSignum(0, meta.compare("", " ")); // "" == ""
    assertSignum(-1, meta.compare("", " 1")); // "" < "1"
    assertSignum(-1, meta.compare("", " 1 ")); // "" < "1"
    assertSignum(-1, meta.compare("", "1")); // "" < "1"
    assertSignum(-1, meta.compare("", "1 ")); // "" < "1"

    assertSignum(1, meta.compare(" ", null)); // "" > null
    assertSignum(0, meta.compare(" ", "")); // "" == ""
    assertSignum(0, meta.compare(" ", " ")); // "" == ""
    assertSignum(-1, meta.compare(" ", " 1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // "" < "1"
    assertSignum(-1, meta.compare(" ", "1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // "" < "1"

    assertSignum(1, meta.compare(" 1", null)); // "1" > null
    assertSignum(1, meta.compare(" 1", "")); // "1" > ""
    assertSignum(1, meta.compare(" 1", " ")); // "1" > ""
    assertSignum(0, meta.compare(" 1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare(" 1 ", null)); // "1" > null
    assertSignum(1, meta.compare(" 1 ", "")); // "1" > ""
    assertSignum(1, meta.compare(" 1 ", " ")); // "1" > ""
    assertSignum(0, meta.compare(" 1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > ""
    assertSignum(1, meta.compare("1", " ")); // "1" > ""
    assertSignum(0, meta.compare("1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1 ", null)); // "1" > null
    assertSignum(1, meta.compare("1 ", "")); // "1" > ""
    assertSignum(1, meta.compare("1 ", " ")); // "1" > ""
    assertSignum(0, meta.compare("1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1" == "1"

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    meta.setIgnoreWhitespace(true);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(-1, meta.compare(null, "")); // null < ""
    assertSignum(-1, meta.compare(null, " ")); // null < ""
    assertSignum(-1, meta.compare(null, " 1")); // null < "1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < "1"
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1"

    assertSignum(1, meta.compare("", null)); // "" > null
    assertSignum(0, meta.compare("", "")); // "" == ""
    assertSignum(0, meta.compare("", " ")); // "" == ""
    assertSignum(-1, meta.compare("", " 1")); // "" < "1"
    assertSignum(-1, meta.compare("", " 1 ")); // "" < "1"
    assertSignum(-1, meta.compare("", "1")); // "" < "1"
    assertSignum(-1, meta.compare("", "1 ")); // "" < "1"

    assertSignum(1, meta.compare(" ", null)); // "" > null
    assertSignum(0, meta.compare(" ", "")); // "" == ""
    assertSignum(0, meta.compare(" ", " ")); // "" == ""
    assertSignum(-1, meta.compare(" ", " 1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // "" < "1"
    assertSignum(-1, meta.compare(" ", "1")); // "" < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // "" < "1"

    assertSignum(1, meta.compare(" 1", null)); // "1" > null
    assertSignum(1, meta.compare(" 1", "")); // "1" > ""
    assertSignum(1, meta.compare(" 1", " ")); // "1" > ""
    assertSignum(0, meta.compare(" 1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare(" 1 ", null)); // "1" > null
    assertSignum(1, meta.compare(" 1 ", "")); // "1" > ""
    assertSignum(1, meta.compare(" 1 ", " ")); // "1" > ""
    assertSignum(0, meta.compare(" 1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > ""
    assertSignum(1, meta.compare("1", " ")); // "1" > ""
    assertSignum(0, meta.compare("1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1 ", null)); // "1" > null
    assertSignum(1, meta.compare("1 ", "")); // "1" > ""
    assertSignum(1, meta.compare("1 ", " ")); // "1" > ""
    assertSignum(0, meta.compare("1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1" == "1"
  }

  @Test
  void testCompareEmptyIsNull() throws HopValueException {
    meta.setNullsAndEmptyAreDifferent(false);

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(0, meta.compare(null, "")); // null == null
    assertSignum(-1, meta.compare(null, " ")); // null < " "
    assertSignum(-1, meta.compare(null, " 1")); // null < " 1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < " 1 "
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1 "

    assertSignum(0, meta.compare("", null)); // null > null
    assertSignum(0, meta.compare("", "")); // null == null
    assertSignum(-1, meta.compare("", " ")); // null < " "
    assertSignum(-1, meta.compare("", " 1")); // null < " 1"
    assertSignum(-1, meta.compare("", " 1 ")); // null < " 1 "
    assertSignum(-1, meta.compare("", "1")); // null < "1"
    assertSignum(-1, meta.compare("", "1 ")); // null < "1 "

    assertSignum(1, meta.compare(" ", null)); // " " > null
    assertSignum(1, meta.compare(" ", "")); // " " > null
    assertSignum(0, meta.compare(" ", " ")); // " " == " "
    assertSignum(-1, meta.compare(" ", " 1")); // " " < " 1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // " " < " 1 "
    assertSignum(-1, meta.compare(" ", "1")); // " " < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // " " < "1 "

    assertSignum(1, meta.compare(" 1", null)); // " 1" > null
    assertSignum(1, meta.compare(" 1", "")); // " 1" > null
    assertSignum(1, meta.compare(" 1", " ")); // " 1" > " "
    assertSignum(0, meta.compare(" 1", " 1")); // " 1" == " 1"
    assertSignum(-1, meta.compare(" 1", " 1 ")); // " 1" < " 1 "
    assertSignum(-1, meta.compare(" 1", "1")); // " 1" < "1"
    assertSignum(-1, meta.compare(" 1", "1 ")); // " 1" < "1 "

    assertSignum(1, meta.compare(" 1 ", null)); // " 1 " > null
    assertSignum(1, meta.compare(" 1 ", "")); // " 1 " > null
    assertSignum(1, meta.compare(" 1 ", " ")); // " 1 " > " "
    assertSignum(1, meta.compare(" 1 ", " 1")); // " 1 " > " 1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // " 1 " == " 1 "
    assertSignum(-1, meta.compare(" 1 ", "1")); // " 1 " < "1"
    assertSignum(-1, meta.compare(" 1 ", "1 ")); // " 1 " < "1 "

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > null
    assertSignum(1, meta.compare("1", " ")); // "1" > " "
    assertSignum(1, meta.compare("1", " 1")); // "1" > " 1"
    assertSignum(1, meta.compare("1", " 1 ")); // "1" > " 1 "
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(-1, meta.compare("1", "1 ")); // "1" < "1 "

    assertSignum(1, meta.compare("1 ", null)); // "1 " > null
    assertSignum(1, meta.compare("1 ", "")); // "1 " > null
    assertSignum(1, meta.compare("1 ", " ")); // "1 " > " "
    assertSignum(1, meta.compare("1 ", " 1")); // "1 " > " 1"
    assertSignum(1, meta.compare("1 ", " 1 ")); // "1 " > " 1 "
    assertSignum(1, meta.compare("1 ", "1")); // "1 " > "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1 " == "1 "

    meta.setTrimType(IValueMeta.TRIM_TYPE_LEFT);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(0, meta.compare(null, "")); // null < null
    // assertSignum( 0, meta.compare( null, " " ) ); // null == null // Is it correct?
    assertSignum(-1, meta.compare(null, " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare(null, " 1")); // null < "1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < "1 "
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1 "

    assertSignum(0, meta.compare("", null)); // null == null
    assertSignum(0, meta.compare("", "")); // null == null
    // assertSignum( 0, meta.compare( "", " " ) ); // null == null // Is it correct?
    assertSignum(-1, meta.compare("", " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare("", " 1")); // null < "1"
    assertSignum(-1, meta.compare("", " 1 ")); // null < "1 "
    assertSignum(-1, meta.compare("", "1")); // null < "1"
    assertSignum(-1, meta.compare("", "1 ")); // null < "1 "

    assertSignum(1, meta.compare(" ", null)); // null > null
    // assertSignum( 0, meta.compare( " ", "" ) ); // null == null // Is it correct?
    assertSignum(1, meta.compare(" ", "")); // null > null // Is it correct?
    assertSignum(0, meta.compare(" ", " ")); // null == null
    assertSignum(-1, meta.compare(" ", " 1")); // null < "1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // null < "1 "
    assertSignum(-1, meta.compare(" ", "1")); // null < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // null < "1 "

    assertSignum(1, meta.compare(" 1", null)); // "1" > null
    assertSignum(1, meta.compare(" 1", "")); // "1" > null
    assertSignum(1, meta.compare(" 1", " ")); // "1" > null
    assertSignum(0, meta.compare(" 1", " 1")); // "1" == "1"
    assertSignum(-1, meta.compare(" 1", " 1 ")); // "1" < "1 "
    assertSignum(0, meta.compare(" 1", "1")); // "1" == "1"
    assertSignum(-1, meta.compare(" 1", "1 ")); // "1" < "1 "

    assertSignum(1, meta.compare(" 1 ", null)); // "1 " > null
    assertSignum(1, meta.compare(" 1 ", "")); // "1 " > null
    assertSignum(1, meta.compare(" 1 ", " ")); // "1 " > null
    assertSignum(1, meta.compare(" 1 ", " 1")); // "1 " > "1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // "1 " == "1 "
    assertSignum(1, meta.compare(" 1 ", "1")); // "1 " > "1"
    assertSignum(0, meta.compare(" 1 ", "1 ")); // "1 " == "1 "

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > null
    assertSignum(1, meta.compare("1", " ")); // "1" > null
    assertSignum(0, meta.compare("1", " 1")); // "1" == "1"
    assertSignum(-1, meta.compare("1", " 1 ")); // "1" < "1 "
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(-1, meta.compare("1", "1 ")); // "1" < "1 "

    assertSignum(1, meta.compare("1 ", null)); // "1 " > null
    assertSignum(1, meta.compare("1 ", "")); // "1 " > null
    assertSignum(1, meta.compare("1 ", " ")); // "1 " > null
    assertSignum(1, meta.compare("1 ", " 1")); // "1 " > "1"
    assertSignum(0, meta.compare("1 ", " 1 ")); // "1 " == "1 "
    assertSignum(1, meta.compare("1 ", "1")); // "1 " > "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1 " == "1 "

    meta.setTrimType(IValueMeta.TRIM_TYPE_RIGHT);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(0, meta.compare(null, "")); // null == null
    // assertSignum( 0, meta.compare( null, " " ) ); // null == null // Is it correct?
    assertSignum(-1, meta.compare(null, " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare(null, " 1")); // null < " 1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < " 1"
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1"

    assertSignum(0, meta.compare("", null)); // null == null
    assertSignum(0, meta.compare("", "")); // null == null
    // assertSignum( 0, meta.compare( "", " " ) ); // null == null // Is it correct?
    assertSignum(-1, meta.compare("", " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare("", " 1")); // null < " 1"
    assertSignum(-1, meta.compare("", " 1 ")); // null < " 1"
    assertSignum(-1, meta.compare("", "1")); // null < "1"
    assertSignum(-1, meta.compare("", "1 ")); // null < "1"

    // assertSignum( 0, meta.compare( " ", null ) ); // null == null // Is it correct?
    assertSignum(1, meta.compare(" ", null)); // null > null // Is it correct?
    // assertSignum( 0, meta.compare( " ", "" ) ); // null == null // Is it correct?
    assertSignum(1, meta.compare(" ", "")); // null > null // Is it correct?
    assertSignum(0, meta.compare(" ", " ")); // null == null
    assertSignum(-1, meta.compare(" ", " 1")); // null < " 1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // null < " 1"
    assertSignum(-1, meta.compare(" ", "1")); // null < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // null < "1"

    assertSignum(1, meta.compare(" 1", null)); // " 1" > null
    assertSignum(1, meta.compare(" 1", "")); // " 1" > null
    assertSignum(1, meta.compare(" 1", " ")); // " 1" > null
    assertSignum(0, meta.compare(" 1", " 1")); // " 1" == " 1"
    assertSignum(0, meta.compare(" 1", " 1 ")); // " 1" == " 1"
    assertSignum(-1, meta.compare(" 1", "1")); // " 1" < "1"
    assertSignum(-1, meta.compare(" 1", "1 ")); // " 1" < "1"

    assertSignum(1, meta.compare(" 1 ", null)); // " 1" > null
    assertSignum(1, meta.compare(" 1 ", "")); // " 1" > null
    assertSignum(1, meta.compare(" 1 ", " ")); // " 1" > null
    assertSignum(0, meta.compare(" 1 ", " 1")); // " 1" == " 1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // " 1" == " 1"
    assertSignum(-1, meta.compare(" 1 ", "1")); // " 1" < "1"
    assertSignum(-1, meta.compare(" 1 ", "1 ")); // " 1" < "1"

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > null
    assertSignum(1, meta.compare("1", " ")); // "1" > null
    assertSignum(1, meta.compare("1", " 1")); // "1" > " 1"
    assertSignum(1, meta.compare("1", " 1 ")); // "1" > " 1"
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1 ", null)); // "1" > null
    assertSignum(1, meta.compare("1 ", "")); // "1" > null
    assertSignum(1, meta.compare("1 ", " ")); // "1" > null
    assertSignum(1, meta.compare("1 ", " 1")); // "1" > " 1"
    assertSignum(1, meta.compare("1 ", " 1 ")); // "1" > " 1"
    assertSignum(0, meta.compare("1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1" == "1"

    meta.setTrimType(IValueMeta.TRIM_TYPE_BOTH);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(0, meta.compare(null, "")); // null == null
    // assertSignum( 0, meta.compare( null, " " ) ); // null == null // Is it correct?
    assertSignum(-1, meta.compare(null, " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare(null, " 1")); // null < "1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < "1"
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1"

    assertSignum(0, meta.compare("", null)); // null == null
    assertSignum(0, meta.compare("", "")); // null == null
    // assertSignum( 0, meta.compare( "", " " ) ); // null == null // Is it correct?
    assertSignum(-1, meta.compare("", " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare("", " 1")); // null < "1"
    assertSignum(-1, meta.compare("", " 1 ")); // null < "1"
    assertSignum(-1, meta.compare("", "1")); // null < "1"
    assertSignum(-1, meta.compare("", "1 ")); // null < "1"

    // assertSignum( 0, meta.compare( " ", null ) ); // null == null // Is it correct?
    assertSignum(1, meta.compare(" ", null)); // null > null // Is it correct?
    // assertSignum( 0, meta.compare( " ", "" ) ); // null == null // Is it correct?
    assertSignum(1, meta.compare(" ", "")); // null > null // Is it correct?
    assertSignum(0, meta.compare(" ", " ")); // null == null
    assertSignum(-1, meta.compare(" ", " 1")); // null < "1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // null < "1"
    assertSignum(-1, meta.compare(" ", "1")); // null < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // null < "1"

    assertSignum(1, meta.compare(" 1", null)); // "1" > null
    assertSignum(1, meta.compare(" 1", "")); // "1" > null
    assertSignum(1, meta.compare(" 1", " ")); // "1" > null
    assertSignum(0, meta.compare(" 1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare(" 1 ", null)); // "1" > null
    assertSignum(1, meta.compare(" 1 ", "")); // "1" > null
    assertSignum(1, meta.compare(" 1 ", " ")); // "1" > null
    assertSignum(0, meta.compare(" 1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > null
    assertSignum(1, meta.compare("1", " ")); // "1" > null
    assertSignum(0, meta.compare("1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1 ", null)); // "1" > null
    assertSignum(1, meta.compare("1 ", "")); // "1" > null
    assertSignum(1, meta.compare("1 ", " ")); // "1" > null
    assertSignum(0, meta.compare("1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1" == "1"

    meta.setTrimType(IValueMeta.TRIM_TYPE_NONE);
    meta.setIgnoreWhitespace(true);

    assertSignum(0, meta.compare(null, null)); // null == null
    assertSignum(0, meta.compare(null, "")); // null == null
    assertSignum(-1, meta.compare(null, " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare(null, " 1")); // null < "1"
    assertSignum(-1, meta.compare(null, " 1 ")); // null < "1"
    assertSignum(-1, meta.compare(null, "1")); // null < "1"
    assertSignum(-1, meta.compare(null, "1 ")); // null < "1"

    assertSignum(0, meta.compare("", null)); // null == null
    assertSignum(0, meta.compare("", "")); // null == null
    assertSignum(-1, meta.compare("", " ")); // null < null // Is it correct?
    assertSignum(-1, meta.compare("", " 1")); // null < "1"
    assertSignum(-1, meta.compare("", " 1 ")); // null < "1"
    assertSignum(-1, meta.compare("", "1")); // null < "1"
    assertSignum(-1, meta.compare("", "1 ")); // null < "1"

    assertSignum(1, meta.compare(" ", null)); // null > null // Is it correct?
    assertSignum(1, meta.compare(" ", "")); // null > null // Is it correct?
    assertSignum(0, meta.compare(" ", " ")); // null == null
    assertSignum(-1, meta.compare(" ", " 1")); // null < "1"
    assertSignum(-1, meta.compare(" ", " 1 ")); // null < "1"
    assertSignum(-1, meta.compare(" ", "1")); // null < "1"
    assertSignum(-1, meta.compare(" ", "1 ")); // null < "1"

    assertSignum(1, meta.compare(" 1", null)); // "1" > null
    assertSignum(1, meta.compare(" 1", "")); // "1" > null
    assertSignum(1, meta.compare(" 1", " ")); // "1" > null
    assertSignum(0, meta.compare(" 1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare(" 1 ", null)); // "1" > null
    assertSignum(1, meta.compare(" 1 ", "")); // "1" > null
    assertSignum(1, meta.compare(" 1 ", " ")); // "1" > null
    assertSignum(0, meta.compare(" 1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare(" 1 ", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1", null)); // "1" > null
    assertSignum(1, meta.compare("1", "")); // "1" > null
    assertSignum(1, meta.compare("1", " ")); // "1" > null
    assertSignum(0, meta.compare("1", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1", "1 ")); // "1" == "1"

    assertSignum(1, meta.compare("1 ", null)); // "1" > null
    assertSignum(1, meta.compare("1 ", "")); // "1" > null
    assertSignum(1, meta.compare("1 ", " ")); // "1" > null
    assertSignum(0, meta.compare("1 ", " 1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", " 1 ")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1")); // "1" == "1"
    assertSignum(0, meta.compare("1 ", "1 ")); // "1" == "1"
  }

  @Test
  void testCompareCollatorEnabled() throws HopValueException {
    ValueMetaString meta = new ValueMetaString(BASE_VALUE);
    meta.setCollatorDisabled(false);
    meta.setCollatorLocale(Locale.FRENCH);

    meta.setCollatorStrength(3);
    assertSignum(-1, meta.compare("E", "F"));
    assertSignum(-1, meta.compare("e", "\u00e9"));
    assertSignum(-1, meta.compare("e", "E"));
    assertSignum(-1, meta.compare("\u0001", "\u0002"));
    assertSignum(0, meta.compare("e", "e"));

    meta.setCollatorStrength(2);
    assertSignum(-1, meta.compare("E", "F"));
    assertSignum(-1, meta.compare("e", "\u00e9"));
    assertSignum(-1, meta.compare("e", "E"));
    assertSignum(0, meta.compare("\u0001", "\u0002"));
    assertSignum(0, meta.compare("e", "e"));

    meta.setCollatorStrength(1);
    assertSignum(-1, meta.compare("E", "F"));
    assertSignum(-1, meta.compare("e", "\u00e9"));
    assertSignum(0, meta.compare("e", "E"));
    assertSignum(0, meta.compare("\u0001", "\u0002"));
    assertSignum(0, meta.compare("e", "e"));

    meta.setCollatorStrength(0);
    assertSignum(-1, meta.compare("E", "F"));
    assertSignum(0, meta.compare("e", "\u00e9"));
    assertSignum(0, meta.compare("e", "E"));
    assertSignum(0, meta.compare("\u0001", "\u0002"));
    assertSignum(0, meta.compare("e", "e"));
  }

  @Test
  void testGetIntegerWithoutConversionMask() throws HopValueException {
    String value = "100.56";
    IValueMeta stringValueMeta = new ValueMetaString("test");

    Long expected = 100L;
    Long result = stringValueMeta.getInteger(value);
    assertEquals(expected, result);
  }

  @Test
  void testGetNumberWithoutConversionMask() throws HopValueException {
    String value = "100.56";
    IValueMeta stringValueMeta = new ValueMetaString("test");

    Double expected = 100.56D;
    Double result = stringValueMeta.getNumber(value);
    assertEquals(expected, result);
  }

  @Test
  void testGetBigNumberWithoutConversionMask() throws HopValueException {
    String value = "100.5";
    IValueMeta stringValueMeta = new ValueMetaString("test");

    BigDecimal expected = new BigDecimal("100.5");
    BigDecimal result = stringValueMeta.getBigNumber(value);
    assertEquals(expected, result);
  }

  @Test
  void testGetDateWithoutConversionMask() throws HopValueException {
    Calendar date = new GregorianCalendar(2017, 9, 20); // month 9 = Oct
    String value = "2017/10/20 00:00:00.000";
    IValueMeta stringValueMeta = new ValueMetaString("test");

    Date expected = Date.from(date.toInstant());
    Date result = stringValueMeta.getDate(value);
    assertEquals(expected, result);
  }

  private static void assertSignum(int expected, int actual) {
    assertSignum("", expected, actual);
  }

  private static void assertSignum(String msg, int expected, int actual) {
    if (expected < 0) {
      if (actual >= 0) {
        failNotEquals(msg, "(<0)", actual);
      }
    } else if (expected > 0) {
      if (actual <= 0) {
        failNotEquals(msg, "(>0)", actual);
      }
    } else {
      assertEquals(expected, actual, msg);
    }
  }

  private static class ConfigurableMeta extends ValueMetaString {
    private boolean nullsAndEmptyAreDifferent;

    public ConfigurableMeta(String name) {
      super(name);
    }

    public void setNullsAndEmptyAreDifferent(boolean nullsAndEmptyAreDifferent) {
      this.nullsAndEmptyAreDifferent = nullsAndEmptyAreDifferent;
    }

    @Override
    public boolean isNull(Object data) throws HopValueException {
      return super.isNull(data, nullsAndEmptyAreDifferent);
    }

    @Override
    protected String convertBinaryStringToString(byte[] binary) throws HopValueException {
      return super.convertBinaryStringToString(binary, nullsAndEmptyAreDifferent);
    }
  }
}

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

package org.apache.hop.core.row;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.math.BigDecimal;
import java.util.Date;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaSerializable;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ValueMetaAndData} */
class ValueMetaAndDataTests {

  @Test
  void testStringType() {
    ValueMetaAndData vm = new ValueMetaAndData("f1", "hello");

    assertInstanceOf(ValueMetaString.class, vm.getValueMeta());
    assertEquals("hello", vm.getValueData());
  }

  @Test
  void testDoubleType() {
    ValueMetaAndData vm = new ValueMetaAndData("f2", 1.23d);

    assertInstanceOf(ValueMetaNumber.class, vm.getValueMeta());
    assertEquals(1.23d, vm.getValueData());
  }

  @Test
  void testLongType() {
    ValueMetaAndData vm = new ValueMetaAndData("f3", 123L);

    assertInstanceOf(ValueMetaInteger.class, vm.getValueMeta());
    assertEquals(123L, vm.getValueData());
  }

  @Test
  void testDateType() {
    Date now = new Date();
    ValueMetaAndData vm = new ValueMetaAndData("f4", now);

    assertInstanceOf(ValueMetaDate.class, vm.getValueMeta());
    assertEquals(now, vm.getValueData());
  }

  @Test
  void testBigDecimalType() {
    BigDecimal bd = new BigDecimal("123.45");
    ValueMetaAndData vm = new ValueMetaAndData("f5", bd);

    assertInstanceOf(ValueMetaBigNumber.class, vm.getValueMeta());
    assertEquals(bd, vm.getValueData());
  }

  @Test
  void testBooleanType() {
    ValueMetaAndData vm = new ValueMetaAndData("f6", true);

    assertInstanceOf(ValueMetaBoolean.class, vm.getValueMeta());
    assertEquals(true, vm.getValueData());
  }

  @Test
  void testByteArrayType() {
    byte[] bytes = new byte[] {1, 2, 3};
    ValueMetaAndData vm = new ValueMetaAndData("f7", bytes);

    assertInstanceOf(ValueMetaBinary.class, vm.getValueMeta());
    assertArrayEquals(bytes, (byte[]) vm.getValueData());
  }

  @Test
  void testDefaultSerializableType() {
    Object obj = new Object();
    ValueMetaAndData vm = new ValueMetaAndData("f8", obj);

    assertInstanceOf(ValueMetaSerializable.class, vm.getValueMeta());
    assertEquals(obj, vm.getValueData());
  }
}

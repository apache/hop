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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ValueMetaFactory} */
class ValueMetaFactoryTests {

  @Test
  void testBigDecimal() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(new BigDecimal("10.5"));
    assertInstanceOf(ValueMetaBigNumber.class, meta);
  }

  @Test
  void testDouble() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(10.5d);
    assertInstanceOf(ValueMetaNumber.class, meta);
  }

  @Test
  void testLong() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(10L);
    assertInstanceOf(ValueMetaInteger.class, meta);
  }

  @Test
  void testInteger_shouldReturnNullOrNeedClarification() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(10);
    assertNull(meta, "Integer is Number but not explicitly handled");
  }

  @Test
  void testFloat() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(10.5f);
    assertNull(meta);
  }

  @Test
  void testString() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface("hello");
    assertInstanceOf(ValueMetaString.class, meta);
  }

  @Test
  void testDate() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(new Date());
    assertInstanceOf(ValueMetaDate.class, meta);
  }

  @Test
  void testBoolean_true() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(true);
    assertInstanceOf(ValueMetaBoolean.class, meta);
  }

  @Test
  void testBoolean_false() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(false);
    assertInstanceOf(ValueMetaBoolean.class, meta);
  }

  @Test
  void testByteArray() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface("abc".getBytes());
    assertInstanceOf(ValueMetaBinary.class, meta);
  }

  @Test
  void testJsonNode() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree("{\"a\":1}");

    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(node);
    assertInstanceOf(ValueMetaJson.class, meta);
  }

  @Test
  void testNull() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(null);
    assertNull(meta);
  }

  @Test
  void testUnknownObject() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(new Object());
    assertNull(meta);
  }

  @Test
  void testAtomicInteger_edgeCase() {
    IValueMeta meta = ValueMetaFactory.guessValueMetaInterface(new AtomicInteger(5));
    assertNull(meta);
  }
}

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

package org.apache.hop.pipeline.transforms.fileinput.text;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.file.EncodingType;
import org.junit.jupiter.api.Test;

/** User: Dzmitry Stsiapanau Date: 3/11/14 Time: 11:44 AM */
class EncodingTypeTest {
  @Test
  void testIsReturn() {
    int lineFeed = '\n';
    int carriageReturn = '\r';
    assertTrue(EncodingType.SINGLE.isLinefeed(lineFeed), "SINGLE.isLineFeed is not line feed");
    assertTrue(
        EncodingType.DOUBLE_BIG_ENDIAN.isLinefeed(lineFeed), "DOUBLE_BIG_ENDIAN is not line feed");
    assertTrue(
        EncodingType.DOUBLE_LITTLE_ENDIAN.isLinefeed(lineFeed),
        "DOUBLE_LITTLE_ENDIAN.isLineFeed is not line feed");
    assertFalse(
        EncodingType.SINGLE.isLinefeed(carriageReturn), "SINGLE.isLineFeed is carriage return");
    assertFalse(
        EncodingType.DOUBLE_BIG_ENDIAN.isLinefeed(carriageReturn),
        "DOUBLE_BIG_ENDIAN.isLineFeed is carriage return");
    assertFalse(
        EncodingType.DOUBLE_LITTLE_ENDIAN.isLinefeed(carriageReturn),
        "DOUBLE_LITTLE_ENDIAN.isLineFeed is carriage return");
  }

  @Test
  void testIsLinefeed() {
    int lineFeed = '\n';
    int carriageReturn = '\r';
    assertFalse(EncodingType.SINGLE.isReturn(lineFeed), "SINGLE.isReturn is line feed");
    assertFalse(
        EncodingType.DOUBLE_BIG_ENDIAN.isReturn(lineFeed),
        "DOUBLE_BIG_ENDIAN.isReturn is line feed");
    assertFalse(
        EncodingType.DOUBLE_LITTLE_ENDIAN.isReturn(lineFeed),
        "DOUBLE_LITTLE_ENDIAN.isReturn is line feed");
    assertTrue(
        EncodingType.SINGLE.isReturn(carriageReturn), "SINGLE.isReturn is not carriage return");
    assertTrue(
        EncodingType.DOUBLE_BIG_ENDIAN.isReturn(carriageReturn),
        "DOUBLE_BIG_ENDIAN.isReturn is not carriage return");
    assertTrue(
        EncodingType.DOUBLE_LITTLE_ENDIAN.isReturn(carriageReturn),
        "DOUBLE_LITTLE_ENDIAN.isReturn is not carriage return");
  }
}

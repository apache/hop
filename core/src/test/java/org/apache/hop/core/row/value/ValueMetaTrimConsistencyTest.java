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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetAddress;
import java.sql.Timestamp;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.Test;

/** Timestamp and Internet address fields trim like every other value type. Issue #9255. */
class ValueMetaTrimConsistencyTest {

  private static final String PADDING_START = "\t ";
  private static final String PADDING_END = " \t";

  @Test
  void timestampTrimsTabsAndNonBreakingSpaces() throws Exception {
    ValueMetaTimestamp timestamp = new ValueMetaTimestamp("ts");
    String text = "2024/05/17 10:00:00.123456789";

    Object value =
        timestamp.convertDataFromString(
            PADDING_START + text + PADDING_END,
            new ValueMetaString("s"),
            null,
            null,
            IValueMeta.TRIM_TYPE_BOTH);

    assertEquals(
        timestamp.convertDataFromString(
            text, new ValueMetaString("s"), null, null, IValueMeta.TRIM_TYPE_NONE),
        value);
    assertEquals(123456789, ((Timestamp) value).getNanos());
  }

  @Test
  void internetAddressTrimsTabsAndNonBreakingSpaces() throws Exception {
    ValueMetaInternetAddress address = new ValueMetaInternetAddress("ip");

    Object value =
        address.convertDataFromString(
            PADDING_START + "127.0.0.1" + PADDING_END,
            new ValueMetaString("s"),
            null,
            null,
            IValueMeta.TRIM_TYPE_BOTH);

    assertEquals(InetAddress.getByName("127.0.0.1"), value);
  }
}

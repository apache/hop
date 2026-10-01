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

package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class RowPreviewSupportTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void binaryCellIsHexEncodedUnlessAvoided() throws Exception {
    ValueMetaBinary meta = new ValueMetaBinary("payload");
    byte[] bytes = new byte[] {0x0a, 0x0b};
    assertEquals("0a0b", RowPreviewSupport.formatCell(meta, bytes, false));
    assertEquals(meta.getString(bytes), RowPreviewSupport.formatCell(meta, bytes, true));
  }

  @Test
  void nullBinaryIsNull() throws Exception {
    ValueMetaBinary meta = new ValueMetaBinary("payload");
    assertNull(RowPreviewSupport.formatCell(meta, null, false));
  }

  @Test
  void stringCellUsesValueMeta() throws Exception {
    ValueMetaString meta = new ValueMetaString("name");
    assertEquals("Ada", RowPreviewSupport.formatCell(meta, "Ada", false));
  }

  @Test
  void formatColumnMetaTooltipDelegates() {
    ValueMetaString meta = new ValueMetaString("customer_name");
    meta.setLength(100);
    String tip = RowPreviewSupport.formatColumnMetaTooltip(meta);
    assertTrue(tip.contains("customer_name"));
    assertTrue(tip.contains("100"));
  }
}

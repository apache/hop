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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Unit tests for {@link ShowRowsDialog#formatColumnMetaTooltip}: cell tooltips must expose column
 * metadata (name, type, length, precision) as requested in issue #7832.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ShowRowsDialogTooltipTest {

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void nullValueMetaReturnsNull() {
    assertNull(ShowRowsDialog.formatColumnMetaTooltip(null));
  }

  @Test
  void stringFieldTooltipIncludesNameTypeAndLength() {
    ValueMetaString meta = new ValueMetaString("customer_name");
    meta.setLength(100);
    meta.setOrigin("Customers");

    String tip = ShowRowsDialog.formatColumnMetaTooltip(meta);
    assertNotNull(tip);
    assertTrue(tip.contains("customer_name"), "tooltip should include the field name");
    assertTrue(tip.contains("String") || tip.contains("string"), "tooltip should include the type");
    assertTrue(tip.contains("100"), "tooltip should include the length");
    assertTrue(tip.contains("Customers"), "tooltip should include the origin when set");
  }

  @Test
  void numberFieldTooltipIncludesPrecision() {
    ValueMetaNumber meta = new ValueMetaNumber("amount");
    meta.setLength(12);
    meta.setPrecision(2);

    String tip = ShowRowsDialog.formatColumnMetaTooltip(meta);
    assertNotNull(tip);
    assertTrue(tip.contains("amount"), "tooltip should include the field name");
    assertTrue(tip.contains("12"), "tooltip should include the length");
    assertTrue(tip.contains("2"), "tooltip should include the precision");
  }

  @Test
  void omitsUnsetLengthAndPrecision() {
    ValueMetaString meta = new ValueMetaString("notes");
    // length/precision left at defaults (-1)

    String tip = ShowRowsDialog.formatColumnMetaTooltip(meta);
    assertNotNull(tip);
    assertTrue(tip.contains("notes"));
    // No dedicated length/precision lines when values are not positive
    assertFalse(tip.toLowerCase().contains("length:"), tip);
    assertFalse(tip.toLowerCase().contains("precision:"), tip);
  }
}

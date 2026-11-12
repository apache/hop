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

package org.apache.hop.workflow.actions.mssqlbulkload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MssqlBulkLoadCodePageTest {

  @Test
  void storedValuesAreFound() {
    assertEquals(MssqlBulkLoadCodePage.ACP, MssqlBulkLoadCodePage.lookup("ACP"));
    assertEquals(MssqlBulkLoadCodePage.OEM, MssqlBulkLoadCodePage.lookup("OEM"));
    assertEquals(MssqlBulkLoadCodePage.RAW, MssqlBulkLoadCodePage.lookup("RAW"));
    assertEquals(MssqlBulkLoadCodePage.SPECIFIC, MssqlBulkLoadCodePage.lookup("Specific"));
    assertEquals(MssqlBulkLoadCodePage.SPECIFIC, MssqlBulkLoadCodePage.lookup("SPECIFIC"));
    assertNull(MssqlBulkLoadCodePage.lookup(null));
    assertNull(MssqlBulkLoadCodePage.lookup(""));
  }

  @Test
  void specificIsStoredAsSpecific() {
    // English projects already store "Specific"
    assertEquals("Specific", MssqlBulkLoadCodePage.SPECIFIC.getCode());
  }

  @Test
  void translatedSpecificLabelsFromOlderDialogsAreFound() {
    for (String label : new String[] {"Spécifique", "Específico", "Specifico", "任意", "自定义"}) {
      assertEquals(MssqlBulkLoadCodePage.SPECIFIC, MssqlBulkLoadCodePage.lookup(label), label);
    }
  }

  @Test
  void onlyWhatSqlServerAcceptsIsAValidCodePage() {
    assertTrue(MssqlBulkLoadCodePage.isValidCodePage("65001"));
    assertTrue(MssqlBulkLoadCodePage.isValidCodePage("raw"));
    assertTrue(MssqlBulkLoadCodePage.isValidCodePage(" 1252 "));
    assertFalse(MssqlBulkLoadCodePage.isValidCodePage(null));
    assertFalse(MssqlBulkLoadCodePage.isValidCodePage("UTF-8"));
    assertFalse(MssqlBulkLoadCodePage.isValidCodePage("65001', FIRE_TRIGGERS --"));
  }
}

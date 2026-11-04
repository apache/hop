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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Row metadata written with {@link RowMeta#getMetaXml()} and read back with {@link
 * RowMeta#RowMeta(org.w3c.dom.Node)} keeps the attributes of each field. Issue #9254.
 */
@ExtendWith(RestoreHopEnvironmentExtension.class)
class RowMetaXmlRoundTripTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static IRowMeta roundTrip(IRowMeta rowMeta) throws Exception {
    return new RowMeta(XmlHandler.loadXmlString(rowMeta.getMetaXml(), RowMeta.XML_META_TAG));
  }

  @Test
  void currencySortOrderAndPaddingSurvive() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    ValueMetaNumber amount = new ValueMetaNumber("amount");
    amount.setConversionMask("¤#,##0.00");
    amount.setCurrencySymbol("€");
    amount.setSortedDescending(true);
    amount.setOutputPaddingEnabled(true);
    rowMeta.addValueMeta(amount);

    IValueMeta loaded = roundTrip(rowMeta).getValueMeta(0);

    assertEquals("€", loaded.getCurrencySymbol());
    assertTrue(loaded.isSortedDescending());
    assertTrue(loaded.isOutputPaddingEnabled());
    assertEquals("€1,234.50", loaded.getString(1234.5));
  }

  @Test
  void theOtherAttributesStillSurvive() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    ValueMetaNumber number = new ValueMetaNumber("number", 9, 2);
    number.setConversionMask("#,##0.00");
    number.setDecimalSymbol(",");
    number.setGroupingSymbol(".");
    number.setOrigin("Select values");
    number.setComments("a comment");
    rowMeta.addValueMeta(number);
    ValueMetaString text = new ValueMetaString("text", 20, -1);
    text.setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    text.setStringEncoding("UTF-8");
    text.setCaseInsensitive(true);
    rowMeta.addValueMeta(text);

    IRowMeta loaded = roundTrip(rowMeta);

    assertEquals(2, loaded.size());
    IValueMeta loadedNumber = loaded.getValueMeta(0);
    assertEquals("number", loadedNumber.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, loadedNumber.getType());
    assertEquals(9, loadedNumber.getLength());
    assertEquals(2, loadedNumber.getPrecision());
    assertEquals("#,##0.00", loadedNumber.getConversionMask());
    assertEquals(",", loadedNumber.getDecimalSymbol());
    assertEquals(".", loadedNumber.getGroupingSymbol());
    assertEquals("Select values", loadedNumber.getOrigin());
    assertEquals("a comment", loadedNumber.getComments());

    IValueMeta loadedText = loaded.getValueMeta(1);
    assertEquals("text", loadedText.getName());
    assertEquals(IValueMeta.TYPE_STRING, loadedText.getType());
    assertEquals(20, loadedText.getLength());
    assertEquals(IValueMeta.TRIM_TYPE_BOTH, loadedText.getTrimType());
    assertEquals("UTF-8", loadedText.getStringEncoding());
    assertTrue(loadedText.isCaseInsensitive());
  }
}

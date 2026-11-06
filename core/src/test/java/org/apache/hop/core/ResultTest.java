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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class ResultTest {

  @Test
  void getXmlHandlesLightCloneWithoutRows() throws Exception {
    Result result = new Result();
    result.setNrErrors(2);
    result.setLogText("log text");

    String xml = result.lightClone().getXml();
    Node node = XmlHandler.loadXmlString(xml, Result.XML_TAG);
    Result copy = new Result(node);

    assertEquals(2, copy.getNrErrors());
    assertEquals("log text", copy.getLogText());
    assertEquals(0, copy.getRows().size());
    assertEquals(0, copy.getResultFiles().size());
  }

  @Test
  void cloneCopiesValuesIntoNewArraysAndNewMetadata() {
    IRowMeta rowMeta = idNamePayloadMeta();
    byte[] payload = {1, 2, 3};
    Result result = resultWith(row(rowMeta, 1L, "a", payload), row(rowMeta, 2L, "b", null));

    List<RowMetaAndData> clonedRows = result.clone().getRows();

    assertEquals(2, clonedRows.size());
    for (int i = 0; i < 2; i++) {
      RowMetaAndData original = result.getRows().get(i);
      RowMetaAndData clone = clonedRows.get(i);
      assertNotSame(original, clone);
      assertNotSame(original.getData(), clone.getData());
      assertNotSame(original.getRowMeta(), clone.getRowMeta());
      assertEquals(
          List.of(original.getRowMeta().getFieldNames()),
          List.of(clone.getRowMeta().getFieldNames()));
      assertEquals(original.getData()[0], clone.getData()[0]);
      assertEquals(original.getData()[1], clone.getData()[1]);
    }
    // cloneRow() copies binary values, as before.
    assertNotSame(payload, clonedRows.get(0).getData()[2]);
    assertArrayEquals(payload, (byte[]) clonedRows.get(0).getData()[2]);
    assertNull(clonedRows.get(1).getData()[2]);
  }

  @Test
  void changingTheOriginalDoesNotChangeTheClone() {
    IRowMeta rowMeta = idNamePayloadMeta();
    byte[] payload = {1, 2, 3};
    Result result = resultWith(row(rowMeta, 1L, "a", payload), row(rowMeta, 2L, "b", null));

    Result clone = result.clone();
    result.getRows().get(0).getData()[1] = "changed";
    payload[0] = 9;
    rowMeta.getValueMeta(1).setName("renamed");
    rowMeta.addValueMeta(new ValueMetaString("extra"));

    for (RowMetaAndData clonedRow : clone.getRows()) {
      assertEquals(
          List.of("id", "name", "payload"), List.of(clonedRow.getRowMeta().getFieldNames()));
    }
    assertEquals("a", clone.getRows().get(0).getData()[1]);
    assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) clone.getRows().get(0).getData()[2]);
  }

  @Test
  void rowsSharingMetadataShareOneClone() {
    IRowMeta shared = idNamePayloadMeta();
    Result result =
        resultWith(
            row(shared, 1L, "a", null), row(shared, 2L, "b", null), row(shared, 3L, "c", null));

    List<RowMetaAndData> clonedRows = result.clone().getRows();

    IRowMeta clonedMeta = clonedRows.get(0).getRowMeta();
    assertNotSame(shared, clonedMeta);
    for (RowMetaAndData row : clonedRows) {
      assertSame(clonedMeta, row.getRowMeta());
    }
  }

  @Test
  void rowsWithTheirOwnMetadataKeepTheirOwnClone() {
    // Two equal but distinct metadata objects: matched by identity, not by equality.
    IRowMeta first = idNamePayloadMeta();
    IRowMeta second = idNamePayloadMeta();
    Result result =
        resultWith(
            row(first, 1L, "a", null), row(second, 2L, "b", null), row(first, 3L, "c", null));

    List<RowMetaAndData> clonedRows = result.clone().getRows();

    assertNotSame(clonedRows.get(0).getRowMeta(), clonedRows.get(1).getRowMeta());
    assertSame(clonedRows.get(0).getRowMeta(), clonedRows.get(2).getRowMeta());
  }

  @Test
  void mixedLayoutsKeepEachRowsOwnLayout() {
    IRowMeta wide = idNamePayloadMeta();
    IRowMeta narrow = new RowMeta();
    narrow.addValueMeta(new ValueMetaString("code"));
    Result result =
        resultWith(
            row(wide, 1L, "a", null), new RowMetaAndData(narrow, "x"), row(wide, 2L, "b", null));

    List<RowMetaAndData> clonedRows = result.clone().getRows();

    assertEquals(
        List.of("id", "name", "payload"), List.of(clonedRows.get(0).getRowMeta().getFieldNames()));
    assertEquals(List.of("code"), List.of(clonedRows.get(1).getRowMeta().getFieldNames()));
    assertEquals("x", clonedRows.get(1).getData()[0]);
    assertSame(clonedRows.get(0).getRowMeta(), clonedRows.get(2).getRowMeta());
  }

  @Test
  void cloneOfEmptyRows() {
    Result empty = new Result();
    Result clone = empty.clone();
    assertTrue(clone.getRows().isEmpty());
    assertNotSame(empty.getRows(), clone.getRows());
  }

  private static IRowMeta idNamePayloadMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaBinary("payload"));
    return rowMeta;
  }

  private static RowMetaAndData row(IRowMeta rowMeta, Long id, String name, byte[] payload) {
    return new RowMetaAndData(rowMeta, id, name, payload);
  }

  private static Result resultWith(RowMetaAndData... rows) {
    Result result = new Result();
    result.setRows(new ArrayList<>(List.of(rows)));
    return result;
  }
}

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

package org.apache.hop.pipeline.transforms.getfilenames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Test class for GetFileNamesData */
class GetFileNamesDataTest {

  @Test
  void testDefaultConstructor() {
    GetFileNamesData data = new GetFileNamesData();

    assertNotNull(data);
    assertNotNull(data.lineBuffer);
    assertTrue(data.lineBuffer.isEmpty());
    assertNull(data.previousRow);
    assertEquals(0, data.nrRepeats);
    assertEquals(0, data.nrLinesOnPage);
    assertNotNull(data.nf);
    assertNotNull(data.df);
    assertNotNull(data.dfs);
    assertNotNull(data.daf);
    assertNull(data.outputRowMeta);
    assertNotNull(data.dafs);
    assertNull(data.files);
    assertFalse(data.isLastFile);
    assertNull(data.filename);
    assertEquals(0, data.filenr);
    assertEquals(0, data.filessize);
    assertNull(data.fr);
    assertNull(data.zi);
    assertNull(data.isr);
    assertFalse(data.doneReading);
    assertEquals(0, data.headerLinesRead);
    assertEquals(0, data.footerLinesRead);
    assertEquals(0, data.pageLinesRead);
    assertFalse(data.doneWithHeader);
    assertNull(data.dataErrorLineHandler);
    assertNull(data.filePlayList);
    assertNull(data.file);
    assertEquals(0, data.rownr);
    assertEquals(0, data.totalpreviousfields);
    assertEquals(-1, data.indexOfFilenameField);
    assertEquals(-1, data.indexOfWildcardField);
    assertEquals(-1, data.indexOfExcludeWildcardField);
    assertNull(data.inputRowMeta);
    assertNull(data.readrow);
    assertEquals(0, data.nrTransformFields);
  }

  @Test
  void testSetDateFormatLenient() {
    GetFileNamesData data = new GetFileNamesData();

    // Test setting lenient to true
    data.setDateFormatLenient(true);
    assertTrue(data.daf.isLenient());

    // Test setting lenient to false
    data.setDateFormatLenient(false);
    assertFalse(data.daf.isLenient());
  }

  @Test
  void testFieldAssignments() {
    GetFileNamesData data = new GetFileNamesData();

    // Test various field assignments
    data.nrRepeats = 5;
    data.nrLinesOnPage = 10;
    data.isLastFile = true;
    data.filename = "test.txt";
    data.filenr = 3;
    data.filessize = 100;
    data.doneReading = true;
    data.headerLinesRead = 2;
    data.footerLinesRead = 1;
    data.pageLinesRead = 5;
    data.doneWithHeader = true;
    data.rownr = 50L;
    data.totalpreviousfields = 3;
    data.indexOfFilenameField = 0;
    data.indexOfWildcardField = 1;
    data.indexOfExcludeWildcardField = 2;
    data.nrTransformFields = 5;

    assertEquals(5, data.nrRepeats);
    assertEquals(10, data.nrLinesOnPage);
    assertTrue(data.isLastFile);
    assertEquals("test.txt", data.filename);
    assertEquals(3, data.filenr);
    assertEquals(100, data.filessize);
    assertTrue(data.doneReading);
    assertEquals(2, data.headerLinesRead);
    assertEquals(1, data.footerLinesRead);
    assertEquals(5, data.pageLinesRead);
    assertTrue(data.doneWithHeader);
    assertEquals(50L, data.rownr);
    assertEquals(3, data.totalpreviousfields);
    assertEquals(0, data.indexOfFilenameField);
    assertEquals(1, data.indexOfWildcardField);
    assertEquals(2, data.indexOfExcludeWildcardField);
    assertEquals(5, data.nrTransformFields);
  }

  @Test
  void testLineBufferOperations() {
    GetFileNamesData data = new GetFileNamesData();

    assertTrue(data.lineBuffer.isEmpty());

    data.lineBuffer.add("line1");
    data.lineBuffer.add("line2");

    assertEquals(2, data.lineBuffer.size());
    assertEquals("line1", data.lineBuffer.get(0));
    assertEquals("line2", data.lineBuffer.get(1));

    data.lineBuffer.clear();
    assertTrue(data.lineBuffer.isEmpty());
  }

  @Test
  void testPreviousRowAssignment() {
    GetFileNamesData data = new GetFileNamesData();

    assertNull(data.previousRow);

    Object[] row = {"value1", "value2", "value3"};
    data.previousRow = row;

    assertNotNull(data.previousRow);
    assertEquals(3, data.previousRow.length);
    assertEquals("value1", data.previousRow[0]);
    assertEquals("value2", data.previousRow[1]);
    assertEquals("value3", data.previousRow[2]);
  }

  @Test
  void testReadrowAssignment() {
    GetFileNamesData data = new GetFileNamesData();

    assertNull(data.readrow);

    Object[] row = {"input1", "input2"};
    data.readrow = row;

    assertNotNull(data.readrow);
    assertEquals(2, data.readrow.length);
    assertEquals("input1", data.readrow[0]);
    assertEquals("input2", data.readrow[1]);
  }
}

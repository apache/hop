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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.IRowSet;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * @author Andrey Khayrutdinov
 */
public class CsvInputEnclosureTest extends CsvInputUnitTestBase {
  private static final String QUOTATION_AND_EXCLAMATION_MARK = "\"!";
  private static final String QUOTATION_MARK = "\"";
  private static final String SEMICOLON = ";";
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private CsvInput csvInput;
  private TransformMockHelper<CsvInputMeta, CsvInputData> transformMockHelper;

  @Before
  public void setUp() throws Exception {
    transformMockHelper = TransformMockUtil.getTransformMockHelper( CsvInputMeta.class, CsvInputData.class, "CsvInputEnclosureTest" );
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void hasEnclosures_HasNewLine() throws Exception {
    doTest( "\"value1\";\"value2\"\n", QUOTATION_MARK );
  }

  @Test
  public void hasEnclosures_HasNotNewLine() throws Exception {
    doTest( "\"value1\";\"value2\"", QUOTATION_MARK );
  }

  @Test
  public void hasNotEnclosures_HasNewLine() throws Exception {
    doTest( "value1;value2\n", QUOTATION_MARK );
  }

  @Test
  public void hasNotEnclosures_HasNotNewLine() throws Exception {
    doTest( "value1;value2", QUOTATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosureWithoutEnclosureAndEndFile() throws Exception {
    doTest( "value1;value2", QUOTATION_AND_EXCLAMATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosureWithEnclosureAndWithoutEndFile() throws Exception {
    doTest( "\"!value1\"!;value2", QUOTATION_AND_EXCLAMATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosurewithEnclosureInBothfield() throws Exception {
    doTest( "\"!value1\"!;\"!value2\"!", QUOTATION_AND_EXCLAMATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosureWithoutEnclosureAndWithEndfileRN() throws Exception {
    doTest( "value1;value2\r\n", QUOTATION_AND_EXCLAMATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosureWithEnclosureAndWithEndfileRN() throws Exception {
    doTest( "value1;\"!value2\"!\r\n", QUOTATION_AND_EXCLAMATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosureWithoutEnclosureAndWithEndfileN() throws Exception {
    doTest( "value1;value2\n", QUOTATION_AND_EXCLAMATION_MARK );
  }

  @Test
  public void hasMultiSymbolsEnclosureWithEnclosureAndWithEndfileN() throws Exception {
    doTest( "value1;\"!value2\"!\n", QUOTATION_AND_EXCLAMATION_MARK );
  }

  public void doTest( String content, String enclosure ) throws Exception {
    IRowSet output = new QueueRowSet();

    File tmp = createTestFile( "utf-8", content );
    try {
      CsvInputMeta meta = createMeta( tmp, createInputFileFields( "f1", "f2" ), enclosure );
      CsvInputData data = new CsvInputData();
      csvInput = new CsvInput( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
      csvInput.init();

      csvInput.addRowSetToOutputRowSets( output );

      try {
        csvInput.processRow();
      } finally {
        csvInput.dispose();
      }
    } finally {
      tmp.delete();
    }

    Object[] row = output.getRowImmediate();
    assertNotNull( row );
    assertEquals( "value1", row[ 0 ] );
    assertEquals( "value2", row[ 1 ] );

    assertNull( output.getRowImmediate() );
  }

  private CsvInputMeta createMeta( File file, TextFileInputField[] fields, String enclosure ) {
    CsvInputMeta meta = createMeta( file, fields );
    meta.setDelimiter( SEMICOLON );
    meta.setEnclosure( enclosure );
    return meta;
  }
}

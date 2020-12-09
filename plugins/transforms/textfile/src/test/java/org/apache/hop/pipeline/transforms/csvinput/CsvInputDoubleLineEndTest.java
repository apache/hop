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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for double line endings in CsvInput transform
 *
 * @author Pavel Sakun
 * @see CsvInput
 */
public class CsvInputDoubleLineEndTest extends CsvInputUnitTestBase {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();
  private static final String ASCII = "windows-1252";
  private static final String UTF8 = "UTF-8";
  private static final String UTF16LE = "UTF-16LE";
  private static final String UTF16BE = "UTF-16BE";
  private static final String TEST_DATA = "Header1\tHeader2\r\nValue\tValue\r\nValue\tValue\r\n";

  private static TransformMockHelper<CsvInputMeta, CsvInputData> transformMockHelper;

  @BeforeClass
  public static void setUp() throws HopException {
    transformMockHelper =
      TransformMockUtil.getTransformMockHelper( CsvInputMeta.class, CsvInputData.class, "CsvInputDoubleLineEndTest" );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) )
      .thenReturn( transformMockHelper.iLogChannel );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );

  }

  @AfterClass
  public static void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testASCII() throws Exception {
    doTest( ASCII, ASCII, TEST_DATA );
  }

  @Test
  public void testUTF16LE() throws Exception {
    doTest( UTF16LE, UTF16LE, TEST_DATA );
  }

  @Test
  public void testUTF16BE() throws Exception {
    doTest( UTF16BE, UTF16BE, TEST_DATA );
  }

  @Test
  public void testUTF8() throws Exception {
    doTest( UTF8, UTF8, TEST_DATA );
  }

  private void doTest( final String fileEncoding, final String transformEncoding, final String testData ) throws Exception {
    String testFilePath = createTestFile( fileEncoding, testData ).getAbsolutePath();

    CsvInputMeta meta = createTransformMeta( testFilePath, transformEncoding );
    CsvInputData data = new CsvInputData();

    CsvInput csvInput = new CsvInput( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );

    csvInput.init();
    csvInput.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          assertEquals( "Value", row[ i ] );
        }
      }
    } );

    boolean haveRowsToRead;
    do {
      haveRowsToRead = !csvInput.processRow();
    } while ( !haveRowsToRead );

    csvInput.dispose();
    assertEquals( 2, csvInput.getLinesWritten() );
  }

  private CsvInputMeta createTransformMeta( final String testFilePath, final String encoding ) {
    final CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename( testFilePath );
    meta.setDelimiter( "\t" );
    meta.setEncoding( encoding );
    meta.setEnclosure( "\"" );
    meta.setBufferSize( "50000" );
    meta.setInputFields( getInputFileFields() );
    meta.setHeaderPresent( true );
    return meta;
  }

  private TextFileInputField[] getInputFileFields() {
    return createInputFileFields( "Header1", "Header2" );
  }
}

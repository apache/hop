/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.trans.steps.csvinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.step.RowAdapter;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.trans.steps.mock.StepMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for double line endings in CsvInput step
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

  private static StepMockHelper<CsvInputMeta, StepDataInterface> stepMockHelper;

  @BeforeClass
  public static void setUp() throws HopException {
    stepMockHelper =
      StepMockUtil.getStepMockHelper( CsvInputMeta.class, "CsvInputDoubleLineEndTest" );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( stepMockHelper.logChannelInterface );
    when( stepMockHelper.trans.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void cleanUp() {
    stepMockHelper.cleanUp();
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

  private void doTest( final String fileEncoding, final String stepEncoding, final String testData ) throws Exception {
    String testFilePath = createTestFile( fileEncoding, testData ).getAbsolutePath();

    CsvInputMeta meta = createStepMeta( testFilePath, stepEncoding );
    CsvInputData data = new CsvInputData();

    CsvInput csvInput =
      new CsvInput(
        stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans );

    csvInput.init( meta, data );
    csvInput.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws HopStepException {
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          assertEquals( "Value", row[ i ] );
        }
      }
    } );

    boolean haveRowsToRead;
    do {
      haveRowsToRead = !csvInput.processRow( meta, data );
    } while ( !haveRowsToRead );

    csvInput.dispose( meta, data );
    assertEquals( 2, csvInput.getLinesWritten() );
  }

  private CsvInputMeta createStepMeta( final String testFilePath, final String encoding ) {
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

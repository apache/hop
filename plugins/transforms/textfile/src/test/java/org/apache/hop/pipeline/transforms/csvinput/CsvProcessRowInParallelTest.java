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

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.core.file.TextFileInputField;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * We take file with content
 * and run it parallel with several transforms.
 * see docs for {@link CsvInput#prepareToRunInParallel()} to understand how running file in parallel works
 * <p>
 * We measure the correctness of work by counting the number of lines, written on each transform.
 * As a result, we should come to this pseudo formula: numberOfLines = sum of number of lines written by each transform.
 * <p>
 * Just a simple example:
 * Assume, we have file with this content:
 * <p>
 * a,b\r\n
 * c,d\r\n
 * <p>
 * If we will run it with 2 transforms, we expect the first transform to read 1st line, and the second transform to read second line.
 * <p>
 * Every test is built in this pattern.
 * <p>
 * We actually play with 4 things:
 * - file content
 * - number of threads (it's actually same as number of transforms)
 * - representation of new line (it can be 2 bytes: '\r\n' (windows) or 1 byte: '\r' or '\n' (Mac, Linux) .
 * Representation can differ. So, if we have different types of new lines in one file - it's ok.
 * - file ends with new line or not
 */
public class CsvProcessRowInParallelTest extends CsvInputUnitTestBase {
  private TransformMockHelper<CsvInputMeta, CsvInputData> transformMockHelper;

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUp() {
    transformMockHelper = TransformMockUtil.getTransformMockHelper( CsvInputMeta.class, CsvInputData.class, "CsvProcessRowInParallelTest" );
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void oneByteNewLineIndicator_NewLineAtTheEnd_2Threads() throws Exception {
    final int totalNumberOfTransforms = 2;
    final String fileContent =
      "a;1\r"
        + "b;2\r";

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 1, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 1, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }

  @Test
  public void oneByteNewLineIndicator_NoNewLineAtTheEnd_2Threads() throws Exception {
    final int totalNumberOfTransforms = 2;

    final String fileContent =
      "a;1\r"
        + "b;2\r"
        + "c;3";

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 2, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 1, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }

  @Test
  public void PDI_15162_mixedByteNewLineIndicator_NewLineAtTheEnd_2Threads() throws Exception {
    final int totalNumberOfTransforms = 2;

    final String fileContent =
      "ab;111\r\n"
        + "bc;222\r\n"
        + "cd;333\r\n"
        + "de;444\r\n"
        + "ef;555\r"
        + "fg;666\r\n"
        + "gh;777\r\n"
        + "hi;888\r\n"
        + "ij;999\r"
        + "jk;000\r";

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 5, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 5, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }

  @Test
  public void PDI_15162_mixedByteNewLineIndicator_NoNewLineAtTheEnd_2Threads() throws Exception {
    final int totalNumberOfTransforms = 2;

    final String fileContent =
      "ab;111\r\n"
        + "bc;222\r\n"
        + "cd;333\r\n"
        + "de;444\r\n"
        + "ef;555\r"
        + "fg;666\r\n"
        + "gh;777\r\n"
        + "hi;888\r\n"
        + "ij;999\r"
        + "jk;000";

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 5, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 5, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }


  @Test
  public void twoByteNewLineIndicator_NewLineAtTheEnd_2Threads() throws Exception {
    final String fileContent =
      "a;1\r\n"
        + "b;2\r\n";
    final int totalNumberOfTransforms = 2;

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 1, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 1, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }

  @Test
  public void twoByteNewLineIndicator_NoNewLineAtTheEnd_2Threads() throws Exception {
    final String fileContent =
      "a;1\r\n"
        + "b;2";
    final int totalNumberOfTransforms = 2;

    File sharedFile = createTestFile( "UTF-8", fileContent );

    int t1 = createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms );
    int t2 = createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms );

    assertEquals( 2, t1 + t2 );
  }


  @Test
  public void twoByteNewLineIndicator_NewLineAtTheEnd_3Threads() throws Exception {
    final String fileContent =
      "a;1\r\n"
        + "b;2\r\n"
        // thread 1 should read until this line
        + "c;3\r\n"
        + "d;4\r\n"
        // thread 2 should read until this line
        + "e;5\r\n"
        + "f;6\r\n";
    // thread 3 should read until this line


    final int totalNumberOfTransforms = 3;

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 2, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 2, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
    assertEquals( 2, createAndRunOneTransform( sharedFile, 2, totalNumberOfTransforms ) );
  }

  /**
   * Here files content is 16 bytes summary, where 8 of this bytes is the first line, 5 is the second one, 3 is the
   * last.
   * <p>
   * As we are running this with 2 threads, we expect: 1st thread to read 1st line 2nd thread to read 2nd and 3d line.
   */
  @Test
  public void mixedBytesNewLineIndicator_NoNewLineAtTheEnd_2Threads() throws Exception {
    final String fileContent =
      "abcd;1\r\n"
        + "b;2\r\n"
        + "d;3";


    final int totalNumberOfTransforms = 2;

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 1, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 2, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }

  @Test
  public void mixedBytesNewLineIndicator_NewLineAtTheEnd_2Threads() throws Exception {
    final String fileContent =
      "abcd;1\r\n"
        + "b;2\r"
        + "d;3\r";


    final int totalNumberOfTransforms = 2;

    File sharedFile = createTestFile( "UTF-8", fileContent );

    assertEquals( 1, createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms ) );
    assertEquals( 2, createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms ) );
  }

  @Test
  public void PDI_16589_twoByteNewLineIndicator_withHeaders_NewLineAtTheEnd_4Threads() throws Exception {
    final int totalNumberOfTransforms = 4;

    final String fileContent =
      "Col1,Col2\r\n"
        + "a,1\r\n"
        + "b,2\r\n"
        + "c,3\r\n"
        + "d,4\r\n"
        + "e,5\r\n"
        + "f,6\r\n"
        + "g,7\r\n"
        + "h,8\r\n"
        + "i,9\r\n"
        + "jk,10\r\n"
        + "lm,11\r\n";

    File sharedFile = createTestFile( "UTF-8", fileContent );

    int t1 = createAndRunOneTransform( sharedFile, 0, totalNumberOfTransforms, true, "," );
    int t2 = createAndRunOneTransform( sharedFile, 1, totalNumberOfTransforms, true, "," );
    int t3 = createAndRunOneTransform( sharedFile, 2, totalNumberOfTransforms, true, "," );
    int t4 = createAndRunOneTransform( sharedFile, 3, totalNumberOfTransforms, true, "," );

    assertEquals( 11, t1 + t2 + t3 + t4 );
  }

  /**
   * So as not to heap up list of taken parameters, we are passing combi, but we expect to see CsvInput class instances
   * in it's content.
   */
  private int processRows( TransformMetaDataCombi combi ) throws Exception {

    CsvInput csvInput = (CsvInput) combi.transform;
    CsvInputData transformData = (CsvInputData) combi.data;
    CsvInputMeta transformMeta = (CsvInputMeta) combi.meta;

    final int[] writtenRows = { 0 };

    csvInput.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        writtenRows[ 0 ]++;
      }
    } );

    boolean haveRowsToRead;
    do {
      haveRowsToRead = !csvInput.processRow();
    } while ( !haveRowsToRead );

    csvInput.dispose();

    return writtenRows[ 0 ];
  }

  private CsvInput createCsvInput( CsvInputMeta meta, CsvInputData data ) {
    return new CsvInput( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta, transformMockHelper.pipeline );
  }


  private int createAndRunOneTransform( File sharedFile, int transformNr, int totalNumberOfTransforms )
    throws Exception {
    return createAndRunOneTransform( sharedFile, transformNr, totalNumberOfTransforms, false, ";" );
  }

  private int createAndRunOneTransform( File sharedFile, int transformNr, int totalNumberOfTransforms, boolean headersPresent, String delimiter )
    throws Exception {
    TransformMetaDataCombi combiTransform1 = createBaseCombi( sharedFile, headersPresent, delimiter );
    configureData( (CsvInputData) combiTransform1.data, transformNr, totalNumberOfTransforms );

    return processRows( combiTransform1 );
  }

  private TransformMetaDataCombi createBaseCombi( File sharedFile, boolean headerPresent, String delimiter ) {

    TransformMetaDataCombi combi = new TransformMetaDataCombi();

    CsvInputData data = new CsvInputData();
    CsvInputMeta meta = createMeta( sharedFile, createInputFileFields( "Field_000", "Field_001" ), headerPresent, delimiter );

    CsvInput csvInput = createCsvInput(meta, data);
    csvInput.init();

    combi.transform = csvInput;
    combi.data = data;
    combi.meta = meta;

    return combi;
  }

  private CsvInputMeta createMeta( File file, TextFileInputField[] fields, boolean headerPresent, String delimiter ) {
    CsvInputMeta meta = createMeta( file, fields );

    meta.setDelimiter( delimiter );
    meta.setEnclosure( "\"" );

    if ( !headerPresent ) {
      meta.setInputFields( fields );
    }

    meta.setHeaderPresent( headerPresent );
    meta.setRunningInParallel( true );

    return meta;
  }

  private void configureData( CsvInputData data, int currentTransformNr, int totalNumberOfTransforms ) {
    data.parallel = true;
    data.transformNumber = currentTransformNr;
    data.totalNumberOfTransforms = totalNumberOfTransforms;
  }
}

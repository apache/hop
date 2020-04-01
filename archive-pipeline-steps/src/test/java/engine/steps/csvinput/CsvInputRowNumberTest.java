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

package org.apache.hop.pipeline.steps.csvinput;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.steps.StepMockUtil;
import org.apache.hop.pipeline.steps.mock.StepMockHelper;
import org.apache.hop.pipeline.steps.fileinput.TextFileInputField;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * @author Andrey Khayrutdinov
 */
public class CsvInputRowNumberTest extends CsvInputUnitTestBase {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private CsvInput csvInput;
  private StepMockHelper<CsvInputMeta, StepDataInterface> stepMockHelper;

  @Before
  public void setUp() throws Exception {
    stepMockHelper = StepMockUtil.getStepMockHelper( CsvInputMeta.class, "CsvInputRowNumberTest" );
    csvInput = new CsvInput(
      stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.pipelineMeta,
      stepMockHelper.pipeline );
  }

  @After
  public void cleanUp() {
    stepMockHelper.cleanUp();
  }

  @Test
  public void hasNotEnclosures_HasNotNewLine() throws Exception {
    File tmp = createTestFile( "utf-8", "a,b\na," );
    try {
      doTest( tmp );
    } finally {
      tmp.delete();
    }
  }

  public void doTest( File file ) throws Exception {
    CsvInputData data = new CsvInputData();
    CsvInputMeta meta = createMeta( file, createInputFileFields( "a", "b" ) );
    List<Object[]> actual;
    try {
      csvInput.init( meta, data );
      actual = PipelineTestingUtil.execute( csvInput, meta, data, 2, false );
    } finally {
      csvInput.dispose( meta, data );
    }

    List<Object[]> expected = Arrays.asList(
      new Object[] { "a", "b", 1L },
      new Object[] { "a", null, 2L }
    );
    PipelineTestingUtil.assertResult( expected, actual );
  }

  @Override
  CsvInputMeta createMeta( File file, TextFileInputField[] fields ) {
    CsvInputMeta meta = super.createMeta( file, fields );
    meta.setRowNumField( "rownum" );
    return meta;
  }
}

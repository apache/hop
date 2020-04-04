/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.fileinput.TextFileInputField;
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
  private TransformMockHelper<CsvInputMeta, ITransformData> transformMockHelper;

  @Before
  public void setUp() throws Exception {
    transformMockHelper = TransformMockUtil.getTransformMockHelper( CsvInputMeta.class, "CsvInputRowNumberTest" );
    csvInput = new CsvInput(
      transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
      transformMockHelper.pipeline );
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
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
      csvInput.init();
      actual = PipelineTestingUtil.execute( csvInput, meta, data, 2, false );
    } finally {
      csvInput.dispose();
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

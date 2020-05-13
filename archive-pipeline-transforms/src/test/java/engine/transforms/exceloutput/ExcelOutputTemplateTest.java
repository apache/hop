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

package org.apache.hop.pipeline.transforms.exceloutput;

import junit.framework.Assert;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Test for case using template file
 *
 * @author Pavel Sakun
 */
public class ExcelOutputTemplateTest {
  private static TransformMockHelper<ExcelOutputMeta, ExcelOutputData> helper;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUp() throws HopException {
    HopEnvironment.init();
    helper =
      new TransformMockHelper<ExcelOutputMeta, ExcelOutputData>( "ExcelOutputTest", ExcelOutputMeta.class,
        ExcelOutputData.class );
    when( helper.logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      helper.logChannelInterface );
    when( helper.pipeline.isRunning() ).thenReturn( true );
  }

  @AfterClass
  public static void cleanUp() {
    helper.cleanUp();
  }

  @Test
  public void testExceptionClosingWorkbook() throws Exception {
    ExcelOutput excelOutput =
      new ExcelOutput( helper.transformMeta, helper.iTransformData, 0, helper.pipelineMeta, helper.pipeline );
    ExcelOutputMeta meta = createTransformMeta();
    excelOutput.init();
    Assert.assertEquals( "Transform init error.", 0, excelOutput.getErrors() );
    helper.initTransformDataInterface.formats = new HashMap<>();
    excelOutput.dispose();
    Assert.assertEquals( "Transform dispose error", 0, excelOutput.getErrors() );
  }

  private ExcelOutputMeta createTransformMeta() throws IOException {
    File tempFile = File.createTempFile( "PDI_tmp", ".tmp" );
    tempFile.deleteOnExit();

    final ExcelOutputMeta meta = new ExcelOutputMeta();
    meta.setFileName( tempFile.getAbsolutePath() );
    meta.setTemplateEnabled( true );
    meta.setTemplateFileName( getClass().getResource( "chart-template.xls" ).getFile() );

    return meta;
  }
}

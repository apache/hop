/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.script;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class ScriptTest {
  private TransformMockHelper<ScriptMeta, ScriptData> helper;

  @Before
  public void setUp() throws Exception {
    helper = new TransformMockHelper<>( "test-script", ScriptMeta.class, ScriptData.class );
    when( helper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
      helper.logChannelInterface );
    when( helper.pipeline.isRunning() ).thenReturn( true );
    when( helper.initTransformMetaInterface.getJSScripts() ).thenReturn(
      new ScriptValuesScript[] { new ScriptValuesScript( ScriptValuesScript.NORMAL_SCRIPT, "", "var i = 0;" ) } );
  }

  @After
  public void tearDown() throws Exception {
    helper.cleanUp();
  }

  @Test
  public void testOutputDoneIfInputEmpty() throws Exception {
    Script transform = new Script( helper.transformMeta, helper.transformDataInterface, 1, helper.pipelineMeta, helper.pipeline );
    transform.init( helper.initTransformMetaInterface, helper.initTransformDataInterface );

    RowSet rs = helper.getMockInputRowSet( new Object[ 0 ][ 0 ] );
    List<RowSet> in = new ArrayList<RowSet>();
    in.add( rs );
    transform.setInputRowSets( in );

    PipelineTestingUtil.execute( transform, helper.processRowsTransformMetaInterface, helper.processRowsTransformDataInterface, 0, true );
    rs.getRow();
  }

}

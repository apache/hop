/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.regexeval;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RegexEvalUnitTest {
  private TransformMockHelper<RegexEvalMeta, RegexEvalData> transformMockHelper;

  @Before
  public void setup() throws Exception {
    transformMockHelper =
      new TransformMockHelper<RegexEvalMeta, RegexEvalData>(
        "REGEX EVAL TEST", RegexEvalMeta.class, RegexEvalData.class );
    when( transformMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( transformMockHelper.logChannelInterface );
    when( transformMockHelper.pipeline.isRunning() ).thenReturn( true );
  }

  @After
  public void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testOutputIsMuchBiggerThanInputDoesntThrowArrayIndexOutOfBounds() throws HopException {
    RegexEval regexEval =
      new RegexEval(
        transformMockHelper.transformMeta, transformMockHelper.transformDataInterface, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    when( transformMockHelper.processRowsTransformMetaInterface.isAllowCaptureGroupsFlagSet() ).thenReturn( true );
    String[] outFields = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k" };
    when( transformMockHelper.processRowsTransformMetaInterface.getFieldName() ).thenReturn( outFields );
    when( transformMockHelper.processRowsTransformMetaInterface.getMatcher() ).thenReturn( "\\.+" );
    transformMockHelper.processRowsTransformDataInterface.pattern = Pattern.compile( "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)" );
    Object[] inputRow = new Object[] {};
    RowSet inputRowSet = transformMockHelper.getMockInputRowSet( inputRow );
    RowMetaInterface mockInputRowMeta = mock( RowMetaInterface.class );
    RowMetaInterface mockOutputRoMeta = mock( RowMetaInterface.class );
    when( mockOutputRoMeta.size() ).thenReturn( outFields.length );
    when( mockInputRowMeta.size() ).thenReturn( 0 );
    when( inputRowSet.getRowMeta() ).thenReturn( mockInputRowMeta );
    when( mockInputRowMeta.clone() ).thenReturn( mockOutputRoMeta );
    when( mockInputRowMeta.isNull( any( Object[].class ), anyInt() ) ).thenReturn( true );
    regexEval.addRowSetToInputRowSets( inputRowSet );

    regexEval.init( transformMockHelper.initTransformMetaInterface, transformMockHelper.initTransformDataInterface );
    regexEval
      .processRow( transformMockHelper.processRowsTransformMetaInterface, transformMockHelper.processRowsTransformDataInterface );
  }
}

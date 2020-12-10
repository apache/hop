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

package org.apache.hop.pipeline.transforms.regexeval;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
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
      new TransformMockHelper<>(
        "REGEX EVAL TEST", RegexEvalMeta.class, RegexEvalData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) )
      .thenReturn( transformMockHelper.iLogChannel );
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
        transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    when( transformMockHelper.iTransformMeta.isAllowCaptureGroupsFlagSet() ).thenReturn( true );
    String[] outFields = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k" };
    when( transformMockHelper.iTransformMeta.getFieldName() ).thenReturn( outFields );
    when( transformMockHelper.iTransformMeta.getMatcher() ).thenReturn( "\\.+" );
    transformMockHelper.iTransformData.pattern = Pattern.compile( "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)" );
    Object[] inputRow = new Object[] {};
    IRowSet inputRowSet = transformMockHelper.getMockInputRowSet( inputRow );
    IRowMeta mockInputRowMeta = mock( IRowMeta.class );
    IRowMeta mockOutputRoMeta = mock( IRowMeta.class );
    when( mockOutputRoMeta.size() ).thenReturn( outFields.length );
    when( mockInputRowMeta.size() ).thenReturn( 0 );
    when( inputRowSet.getRowMeta() ).thenReturn( mockInputRowMeta );
    when( mockInputRowMeta.clone() ).thenReturn( mockOutputRoMeta );
    when( mockInputRowMeta.isNull( any( Object[].class ), anyInt() ) ).thenReturn( true );
    regexEval.addRowSetToInputRowSets( inputRowSet );

    regexEval.processRow();
    regexEval
      .processRow();
  }
}

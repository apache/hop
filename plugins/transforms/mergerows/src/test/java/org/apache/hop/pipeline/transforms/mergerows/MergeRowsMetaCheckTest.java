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

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class MergeRowsMetaCheckTest {

  private PipelineMeta pipelineMeta;
  private MergeRowsMeta meta;
  private TransformMeta transformMeta;
  private static final String TRANSFORM_NAME = "MERGE_ROWS_META_CHECK_TEST_TRANSFORM_NAME";
  private static final String REFERENCE_TRANSFORM_NAME = "REFERENCE_TRANSFORM";
  private static final String COMPARISON_TRANSFORM_NAME = "COMPARISON_TRANSFORM";
  private TransformMeta referenceTransformMeta;
  private TransformMeta comparisonTransformMeta;
  private List<ICheckResult> remarks;
  private IVariables variables;

  protected IRowMeta generateRowMetaEmpty() {
    return new RowMeta();
  }

  protected IRowMeta generateRowMeta10Strings() {
    RowMeta output = new RowMeta();
    for ( int i = 0; i < 10; i++ ) {
      output.addValueMeta( new ValueMetaString( "row_" + ( i + 1 ) ) );
    }
    return output;
  }

  protected IRowMeta generateRowMeta10MixedTypes() {
    RowMeta output = new RowMeta();
    for ( int i = 0; i < 10; i++ ) {
      if ( i < 5 ) {
        output.addValueMeta( new ValueMetaString( "row_" + ( i + 1 ) ) );
      } else {
        output.addValueMeta( new ValueMetaInteger( "row_" + ( i + 1 ) ) );
      }
    }
    return output;
  }

  @Before
  public void setup() throws Exception {
    pipelineMeta = mock( PipelineMeta.class );
    meta = new MergeRowsMeta();
    transformMeta = new TransformMeta( TRANSFORM_NAME, meta );
    referenceTransformMeta = mock( TransformMeta.class );
    comparisonTransformMeta = mock( TransformMeta.class );
    when( referenceTransformMeta.getName() ).thenReturn( REFERENCE_TRANSFORM_NAME );
    when( comparisonTransformMeta.getName() ).thenReturn( COMPARISON_TRANSFORM_NAME );
    meta.getTransformIOMeta().getInfoStreams().get( 0 ).setTransformMeta( referenceTransformMeta );
    meta.getTransformIOMeta().getInfoStreams().get( 1 ).setTransformMeta( comparisonTransformMeta );
    remarks = new ArrayList<>();
  }

  @Test
  public void testCheckInputRowsBothEmpty() throws HopTransformException {
    when( pipelineMeta.getPrevTransformFields(any(IVariables.class), eq(REFERENCE_TRANSFORM_NAME) ) ).thenReturn( generateRowMetaEmpty() );
    when( pipelineMeta.getPrevTransformFields(any(IVariables.class), eq(COMPARISON_TRANSFORM_NAME) ) ).thenReturn( generateRowMetaEmpty() );

    meta.check( remarks, pipelineMeta, transformMeta, null, new String[ 0 ], new String[ 0 ],
      (RowMeta) null, new Variables(), null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), ICheckResult.TYPE_RESULT_OK );
  }

  @Test
  public void testCheckInputRowsBothNonEmpty() throws HopTransformException {
    when( pipelineMeta.getPrevTransformFields(any(IVariables.class), eq(REFERENCE_TRANSFORM_NAME) ) ).thenReturn( generateRowMeta10Strings() );
    when( pipelineMeta.getPrevTransformFields(any(IVariables.class), eq(COMPARISON_TRANSFORM_NAME) ) ).thenReturn( generateRowMeta10Strings() );

    meta.check( remarks, pipelineMeta, transformMeta, (RowMeta) null, new String[ 0 ], new String[ 0 ], (RowMeta) null, new Variables(), null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), ICheckResult.TYPE_RESULT_OK );
  }

  @Test
  public void testCheckInputRowsEmptyAndNonEmpty() throws HopTransformException {
    when( pipelineMeta.getPrevTransformFields(any(), eq(REFERENCE_TRANSFORM_NAME) ) ).thenReturn( generateRowMetaEmpty() );
    when( pipelineMeta.getPrevTransformFields(any(), eq(COMPARISON_TRANSFORM_NAME) ) ).thenReturn( generateRowMeta10Strings() );

    meta.check( remarks, pipelineMeta, transformMeta, (RowMeta) null, new String[ 0 ], new String[ 0 ], (RowMeta) null, new Variables(), null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), ICheckResult.TYPE_RESULT_ERROR );
  }

  @Test
  public void testCheckInputRowsDifferentRowMetaTypes() throws HopTransformException {
    when( pipelineMeta.getPrevTransformFields( any(), eq(REFERENCE_TRANSFORM_NAME) ) ).thenReturn( generateRowMeta10MixedTypes() );
    when( pipelineMeta.getPrevTransformFields( any(), eq(COMPARISON_TRANSFORM_NAME) ) ).thenReturn( generateRowMeta10Strings() );

    meta.check( remarks, pipelineMeta, transformMeta, null, new String[ 0 ], new String[ 0 ],
      (RowMeta) null, new Variables(), null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), ICheckResult.TYPE_RESULT_ERROR );
  }
}

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

package org.apache.hop.trans.steps.mergerows;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.metastore.api.IMetaStore;

public class MergeRowsMetaCheckTest {

  private TransMeta transMeta;
  private MergeRowsMeta meta;
  private StepMeta stepMeta;
  private static final String STEP_NAME = "MERGE_ROWS_META_CHECK_TEST_STEP_NAME";
  private static final String REFERENCE_STEP_NAME = "REFERENCE_STEP";
  private static final String COMPARISON_STEP_NAME = "COMPARISON_STEP";
  private StepMeta referenceStepMeta;
  private StepMeta comparisonStepMeta;
  private List<CheckResultInterface> remarks;

  protected RowMetaInterface generateRowMetaEmpty() {
    return new RowMeta();
  }

  protected RowMetaInterface generateRowMeta10Strings() {
    RowMeta output = new RowMeta();
    for ( int i = 0; i < 10; i++ ) {
      output.addValueMeta( new ValueMetaString( "row_" + ( i + 1 ) ) );
    }
    return output;
  }

  protected RowMetaInterface generateRowMeta10MixedTypes() {
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
  public void setup() {
    transMeta = mock( TransMeta.class );
    meta = new MergeRowsMeta();
    stepMeta = new StepMeta( STEP_NAME, meta );
    referenceStepMeta = mock( StepMeta.class );
    comparisonStepMeta = mock( StepMeta.class );
    when( referenceStepMeta.getName() ).thenReturn( REFERENCE_STEP_NAME );
    when( comparisonStepMeta.getName() ).thenReturn( COMPARISON_STEP_NAME );
    meta.getStepIOMeta().getInfoStreams().get( 0 ).setStepMeta( referenceStepMeta );
    meta.getStepIOMeta().getInfoStreams().get( 1 ).setStepMeta( comparisonStepMeta );

    remarks = new ArrayList<>();
  }

  @Test
  public void testCheckInputRowsBothEmpty() throws HopStepException {
    when( transMeta.getPrevStepFields( REFERENCE_STEP_NAME ) ).thenReturn( generateRowMetaEmpty() );
    when( transMeta.getPrevStepFields( COMPARISON_STEP_NAME ) ).thenReturn( generateRowMetaEmpty() );

    meta.check( remarks, transMeta, stepMeta, (RowMeta) null, new String[0], new String[0],
      (RowMeta) null, new Variables(), (Repository) null, (IMetaStore) null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), CheckResultInterface.TYPE_RESULT_OK );
  }

  @Test
  public void testCheckInputRowsBothNonEmpty() throws HopStepException {
    when( transMeta.getPrevStepFields( REFERENCE_STEP_NAME ) ).thenReturn( generateRowMeta10Strings() );
    when( transMeta.getPrevStepFields( COMPARISON_STEP_NAME ) ).thenReturn( generateRowMeta10Strings() );

    meta.check( remarks, transMeta, stepMeta, (RowMeta) null, new String[0], new String[0],
      (RowMeta) null, new Variables(), (Repository) null, (IMetaStore) null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), CheckResultInterface.TYPE_RESULT_OK );
  }

  @Test
  public void testCheckInputRowsEmptyAndNonEmpty() throws HopStepException {
    when( transMeta.getPrevStepFields( REFERENCE_STEP_NAME ) ).thenReturn( generateRowMetaEmpty() );
    when( transMeta.getPrevStepFields( COMPARISON_STEP_NAME ) ).thenReturn( generateRowMeta10Strings() );

    meta.check( remarks, transMeta, stepMeta, (RowMeta) null, new String[0], new String[0],
      (RowMeta) null, new Variables(), (Repository) null, (IMetaStore) null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), CheckResultInterface.TYPE_RESULT_ERROR );
  }

  @Test
  public void testCheckInputRowsDifferentRowMetaTypes() throws HopStepException {
    when( transMeta.getPrevStepFields( REFERENCE_STEP_NAME ) ).thenReturn( generateRowMeta10MixedTypes() );
    when( transMeta.getPrevStepFields( COMPARISON_STEP_NAME ) ).thenReturn( generateRowMeta10Strings() );

    meta.check( remarks, transMeta, stepMeta, (RowMeta) null, new String[0], new String[0],
      (RowMeta) null, new Variables(), (Repository) null, (IMetaStore) null );

    assertNotNull( remarks );
    assertTrue( remarks.size() >= 2 );
    assertEquals( remarks.get( 1 ).getType(), CheckResultInterface.TYPE_RESULT_ERROR );
  }
}

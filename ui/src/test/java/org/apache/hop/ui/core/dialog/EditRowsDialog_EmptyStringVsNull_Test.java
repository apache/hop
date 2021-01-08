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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.eclipse.swt.widgets.TableItem;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
@RunWith( PowerMockRunner.class )
public class EditRowsDialog_EmptyStringVsNull_Test {

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  public void emptyAndNullsAreNotDifferent() throws Exception {
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N" );
    executeAndAssertResults( new String[] { "", null, null } );
  }


  @Test
  public void emptyAndNullsAreDifferent() throws Exception {
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y" );
    executeAndAssertResults( new String[] { "", "", "" } );
  }

  private void executeAndAssertResults( String[] expected ) throws Exception {
    EditRowsDialog dialog = mock( EditRowsDialog.class );

    when( dialog.getRowForData( any( TableItem.class ), anyInt() ) ).thenCallRealMethod();
    doCallRealMethod().when( dialog ).setRowMeta( any( IRowMeta.class ) );
    doCallRealMethod().when( dialog ).setStringRowMeta( any( IRowMeta.class ) );

    when( dialog.isDisplayingNullValue( any( TableItem.class ), anyInt() ) ).thenReturn( false );

    RowMeta meta = new RowMeta();
    meta.addValueMeta( new ValueMetaString( "s1" ) );
    meta.addValueMeta( new ValueMetaString( "s2" ) );
    meta.addValueMeta( new ValueMetaString( "s3" ) );
    dialog.setRowMeta( meta );
    dialog.setStringRowMeta( meta );

    TableItem item = mock( TableItem.class );
    when( item.getText( 1 ) ).thenReturn( " " );
    when( item.getText( 2 ) ).thenReturn( "" );
    when( item.getText( 3 ) ).thenReturn( null );

    Object[] data = dialog.getRowForData( item, 0 );
    PipelineTestingUtil.assertResult( expected, data );
  }
}

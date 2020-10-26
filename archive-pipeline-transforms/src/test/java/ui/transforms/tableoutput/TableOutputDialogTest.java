/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.ui.pipeline.transforms.tableoutput;

import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TextVar;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

public class TableOutputDialogTest {

  private static IRowMeta filled;
  private static IRowMeta empty;
  private static String[] sample = { "1", "2", "3" };

  @Before
  public void setup() {
    filled = createRowMeta( sample, false );
    empty = createRowMeta( sample, true );
  }

  @Test
  public void validationRowMetaTest() throws Exception {
    Method m = TableOutputDialog.class.getDeclaredMethod( "isValidRowMeta", IRowMeta.class );
    m.setAccessible( true );
    Object result1 = m.invoke( null, filled );
    Object result2 = m.invoke( null, empty );
    assertTrue( Boolean.parseBoolean( result1 + "" ) );
    assertFalse( Boolean.parseBoolean( result2 + "" ) );
  }

  private IRowMeta createRowMeta( String[] args, boolean hasEmptyFields ) {
    IRowMeta result = new RowMeta();
    if ( hasEmptyFields ) {
      result.addValueMeta( new ValueMetaString( "" ) );
    }
    for ( String s : args ) {
      result.addValueMeta( new ValueMetaString( s ) );
    }
    return result;
  }

  private void isConnectionSupportedTest( boolean supported ) {
    TableOutputDialog dialog = mock( TableOutputDialog.class );

    PipelineMeta pipelineMeta = mock( PipelineMeta.class );
    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    TextVar text = mock( TextVar.class );
    MetaSelectionLine<DatabaseMeta> combo = mock( MetaSelectionLine.class );
    IDatabase dbInterface = mock( IDatabase.class );

    setInternalState( dialog, "wTable", text );
    setInternalState( dialog, "wConnection", combo );
    setInternalState( dialog, "pipelineMeta", pipelineMeta );

    when( text.getText() ).thenReturn( "someTable" );
    when( combo.getText() ).thenReturn( "someConnection" );
    when( pipelineMeta.findDatabase( anyString() ) ).thenReturn( dbMeta );
    when( dbMeta.getIDatabase() ).thenReturn( dbInterface );

    doNothing().when( dialog ).showUnsupportedConnectionMessageBox( dbInterface );
    doCallRealMethod().when( dialog ).isConnectionSupported();

    //Check that if the db interface does not support standard output then showUnsupportedConnection is called
    when( dbInterface.supportsStandardTableOutput() ).thenReturn( supported );
    dialog.isConnectionSupported();
    verify( dialog, times( !supported ? 1 : 0 ) ).showUnsupportedConnectionMessageBox( dbInterface );
  }

  @Test
  public void isConnectionSupportedValidTest() {
    isConnectionSupportedTest( true );
  }

  @Test
  public void isConnectionSupportedInvalidTest() {
    isConnectionSupportedTest( false );
  }
}

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

package org.apache.hop.ui.hopui.delegates;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.TransLogTable;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.trans.TransGraph;
import org.apache.hop.ui.hopui.trans.TransLogDelegate;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class HopUiTransformationDelegateTest {
  private static final String[] EMPTY_STRING_ARRAY = new String[] {};
  private static final String TEST_PARAM_KEY = "paramKey";
  private static final String TEST_PARAM_VALUE = "paramValue";
  private static final Map<String, String> MAP_WITH_TEST_PARAM = new HashMap<String, String>() {
    {
      put( TEST_PARAM_KEY, TEST_PARAM_VALUE );
    }
  };
  private static final LogLevel TEST_LOG_LEVEL = LogLevel.BASIC;
  private static final boolean TEST_BOOLEAN_PARAM = true;

  private HopUiTransformationDelegate delegate;
  private HopUi hopUi;

  private TransLogTable transLogTable;
  private TransMeta transMeta;
  private List<TransMeta> transformationMap;

  @Before
  public void before() {
    transformationMap = new ArrayList<TransMeta>();

    transMeta = mock( TransMeta.class );
    delegate = mock( HopUiTransformationDelegate.class );
    hopUi = mock( HopUi.class );
    hopUi.delegates = mock( HopUiDelegates.class );
    hopUi.delegates.tabs = mock( HopUiTabsDelegate.class );
    hopUi.variables = mock( RowMetaAndData.class );
    delegate.hopUi = hopUi;

    doReturn( transformationMap ).when( delegate ).getTransformationList();
    doReturn( hopUi ).when( delegate ).getSpoon();
    doCallRealMethod().when( delegate ).isLogTableDefined( any() );
    transLogTable = mock( TransLogTable.class );
  }

  @Test
  public void testIsLogTableDefinedLogTableDefined() {
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    doReturn( databaseMeta ).when( transLogTable ).getDatabaseMeta();
    doReturn( "test_table" ).when( transLogTable ).getTableName();

    assertTrue( delegate.isLogTableDefined( transLogTable ) );
  }

  @Test
  public void testIsLogTableDefinedLogTableNotDefined() {
    DatabaseMeta databaseMeta = mock( DatabaseMeta.class );
    doReturn( databaseMeta ).when( transLogTable ).getDatabaseMeta();

    assertFalse( delegate.isLogTableDefined( transLogTable ) );
  }

  @Test
  public void testAddAndCloseTransformation() {
    doCallRealMethod().when( delegate ).closeTransformation( any() );
    doCallRealMethod().when( delegate ).addTransformation( any() );
    assertTrue( delegate.addTransformation( transMeta ) );
    assertFalse( delegate.addTransformation( transMeta ) );
    delegate.closeTransformation( transMeta );
    assertTrue( delegate.addTransformation( transMeta ) );
  }

  @Test
  @SuppressWarnings( "ResultOfMethodCallIgnored" )
  public void testSetParamsIntoMetaInExecuteTransformation() throws HopException {
    doCallRealMethod().when( delegate ).executeTransformation( transMeta, true, false, false,
      false, false, null, false, LogLevel.BASIC );

    RowMetaInterface rowMeta = mock( RowMetaInterface.class );
    TransExecutionConfiguration transExecutionConfiguration = mock( TransExecutionConfiguration.class );
    TransGraph activeTransGraph = mock( TransGraph.class );
    activeTransGraph.transLogDelegate = mock( TransLogDelegate.class );

    doReturn( rowMeta ).when( hopUi.variables ).getRowMeta();
    doReturn( EMPTY_STRING_ARRAY ).when( rowMeta ).getFieldNames();
    doReturn( transExecutionConfiguration ).when( hopUi ).getTransExecutionConfiguration();
    doReturn( MAP_WITH_TEST_PARAM ).when( transExecutionConfiguration ).getParams();
    doReturn( activeTransGraph ).when( hopUi ).getActiveTransGraph();
    doReturn( TEST_LOG_LEVEL ).when( transExecutionConfiguration ).getLogLevel();
    doReturn( TEST_BOOLEAN_PARAM ).when( transExecutionConfiguration ).isClearingLog();
    doReturn( TEST_BOOLEAN_PARAM ).when( transExecutionConfiguration ).isSafeModeEnabled();
    doReturn( TEST_BOOLEAN_PARAM ).when( transExecutionConfiguration ).isGatheringMetrics();

    delegate.executeTransformation( transMeta, true, false, false, false, false,
      null, false, LogLevel.BASIC );

    verify( transMeta ).setParameterValue( TEST_PARAM_KEY, TEST_PARAM_VALUE );
    verify( transMeta ).activateParameters();
    verify( transMeta ).setLogLevel( TEST_LOG_LEVEL );
    verify( transMeta ).setClearingLog( TEST_BOOLEAN_PARAM );
    verify( transMeta ).setSafeModeEnabled( TEST_BOOLEAN_PARAM );
    verify( transMeta ).setGatheringMetrics( TEST_BOOLEAN_PARAM );
  }
}

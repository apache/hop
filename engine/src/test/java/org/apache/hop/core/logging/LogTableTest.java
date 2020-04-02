/*!
 * Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.core.logging;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.metastore.api.IMetaStore;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class LogTableTest {
  private static String PARAM_START_SYMBOL = "${";
  private static String PARAM_END_SYMBOL = "}";
  private static String GLOBAL_PARAM = PARAM_START_SYMBOL + Const.HOP_TRANSFORM_LOG_DB + PARAM_END_SYMBOL;
  private static String USER_PARAM = PARAM_START_SYMBOL + "param-content" + PARAM_END_SYMBOL;
  private static String HARDCODED_VALUE = "hardcoded";
  private VariableSpace mockedVariableSpace;
  private IMetaStore mockedMetaStore;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void init() {
    System.setProperty( Const.HOP_TRANSFORM_LOG_DB, "HOP_TRANSFORM_LOG_DB_VALUE" );
    System.setProperty( Const.HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT, "true" );
    mockedVariableSpace = mock( VariableSpace.class );
    mockedMetaStore = mock( IMetaStore.class );
  }

  @Test
  public void hardcodedFieldsNotChanged() {
    tableFieldsChangedCorrectlyAfterNullingGlobalParams( HARDCODED_VALUE, HARDCODED_VALUE );
  }

  @Test
  public void userParamsFieldsNotChanged() {
    tableFieldsChangedCorrectlyAfterNullingGlobalParams( USER_PARAM, USER_PARAM );
  }

  @Test
  public void globalParamsFieldsAreNulled() {
    tableFieldsChangedCorrectlyAfterNullingGlobalParams( GLOBAL_PARAM, null );
  }

  @Test
  public void globalParamsFieldsAreNotNulled() {
    System.setProperty( Const.HOP_GLOBAL_LOG_VARIABLES_CLEAR_ON_EXPORT, "false" );
    tableFieldsChangedCorrectlyAfterNullingGlobalParams( GLOBAL_PARAM, GLOBAL_PARAM );
  }

  public void tableFieldsChangedCorrectlyAfterNullingGlobalParams( String valueForAllFields,
                                                                   String expectedAfterNullingGlobalParams ) {

    PerformanceLogTable performanceLogTable = getPerformanceLogTableWithAllEqFields( valueForAllFields );
    performanceLogTable.setAllGlobalParametersToNull();
    commonTableFieldsValueChecker( performanceLogTable, expectedAfterNullingGlobalParams );
    assertEquals( performanceLogTable.getLogInterval(), expectedAfterNullingGlobalParams );

    JobLogTable jobLogTable = getJobLogTableWithAllEqFields( valueForAllFields );
    jobLogTable.setAllGlobalParametersToNull();
    commonTableFieldsValueChecker( jobLogTable, expectedAfterNullingGlobalParams );
    assertEquals( jobLogTable.getLogInterval(), expectedAfterNullingGlobalParams );
    assertEquals( jobLogTable.getLogSizeLimit(), expectedAfterNullingGlobalParams );

    PipelineLogTable pipelineLogTable = getPipelineLogTableWithAllEqFields( valueForAllFields );
    pipelineLogTable.setAllGlobalParametersToNull();
    commonTableFieldsValueChecker( pipelineLogTable, expectedAfterNullingGlobalParams );
    assertEquals( pipelineLogTable.getLogInterval(), expectedAfterNullingGlobalParams );
    assertEquals( pipelineLogTable.getLogSizeLimit(), expectedAfterNullingGlobalParams );
  }

  private PerformanceLogTable getPerformanceLogTableWithAllEqFields( String fieldsValue ) {
    PerformanceLogTable performanceLogTable =
      PerformanceLogTable.getDefault( mockedVariableSpace, mockedMetaStore );
    initCommonTableFields( performanceLogTable, fieldsValue );
    performanceLogTable.setLogInterval( fieldsValue );

    return performanceLogTable;
  }

  private JobLogTable getJobLogTableWithAllEqFields( String fieldsValue ) {
    JobLogTable jobLogTable = JobLogTable.getDefault( mockedVariableSpace, mockedMetaStore );
    initCommonTableFields( jobLogTable, fieldsValue );
    jobLogTable.setLogSizeLimit( fieldsValue );
    jobLogTable.setLogInterval( fieldsValue );

    return jobLogTable;
  }

  private PipelineLogTable getPipelineLogTableWithAllEqFields( String fieldsValue ) {
    PipelineLogTable pipelineLogTable = PipelineLogTable.getDefault( mockedVariableSpace, mockedMetaStore, null );
    initCommonTableFields( pipelineLogTable, fieldsValue );
    pipelineLogTable.setLogInterval( fieldsValue );
    pipelineLogTable.setLogSizeLimit( fieldsValue );

    return pipelineLogTable;
  }

  private void initCommonTableFields( BaseLogTable logTable, String value ) {
    logTable.setTableName( value );
    logTable.setConnectionName( value );
    logTable.setSchemaName( value );
    logTable.setTimeoutInDays( value );
  }

  private void commonTableFieldsValueChecker( BaseLogTable logTable, String expectedForAllFields ) {
    assertEquals( logTable.getTableName(), expectedForAllFields );
    assertEquals( logTable.getConnectionName(), expectedForAllFields );
    assertEquals( logTable.getSchemaName(), expectedForAllFields );
    assertEquals( logTable.getTimeoutInDays(), expectedForAllFields );
  }
}

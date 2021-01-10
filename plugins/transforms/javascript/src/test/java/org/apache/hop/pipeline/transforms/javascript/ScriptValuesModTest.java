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

package org.apache.hop.pipeline.transforms.javascript;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author Andrey Khayrutdinov
 */
public class ScriptValuesModTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  @Ignore
  public void bigNumberAreNotTrimmedToInt() throws Exception {
    ScriptValuesMod transform = TransformMockUtil.getTransform( ScriptValuesMod.class, ScriptValuesMetaMod.class, ScriptValuesModData.class, "test" );

    RowMeta input = new RowMeta();
    input.addValueMeta( new ValueMetaBigNumber( "value_int" ) );
    input.addValueMeta( new ValueMetaBigNumber( "value_double" ) );
    transform.setInputRowMeta( input );

    transform = spy( transform );
    doReturn( new Object[] { BigDecimal.ONE, BigDecimal.ONE } ).when( transform ).getRow();

    ScriptValuesMetaMod meta = new ScriptValuesMetaMod();
    meta.allocate( 2 );
    meta.setFieldname( new String[] { "value_int", "value_double" } );
    meta.setType( new int[] { IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_BIGNUMBER } );
    meta.setReplace( new boolean[] { true, true } );

    meta.setJSScripts( new ScriptValuesScript[] {
      new ScriptValuesScript( ScriptValuesScript.TRANSFORM_SCRIPT, "script",
        "value_int = 10.00;\nvalue_double = 10.50" )
    } );

    ScriptValuesModData data = new ScriptValuesModData();
    transform.init();

    Object[] expectedRow = { BigDecimal.TEN, new BigDecimal( "10.5" ) };
    Object[] row = PipelineTestingUtil.execute( transform, /*meta, data,*/ 1, false ).get( 0 );
    PipelineTestingUtil.assertResult( expectedRow, row );
  }

  @Test
  @Ignore
  public void variableIsSetInScopeOfTransform() throws Exception {
    ScriptValuesMod transform = TransformMockUtil.getTransform( ScriptValuesMod.class, ScriptValuesMetaMod.class, ScriptValuesModData.class, "test" );

    RowMeta input = new RowMeta();
    input.addValueMeta( new ValueMetaString( "str" ) );
    transform.setInputRowMeta( input );

    transform = spy( transform );
    doReturn( new Object[] { "" } ).when( transform ).getRow();

    ScriptValuesMetaMod meta = new ScriptValuesMetaMod();
    meta.allocate( 1 );
    meta.setFieldname( new String[] { "str" } );
    meta.setType( new int[] { IValueMeta.TYPE_STRING } );
    meta.setReplace( new boolean[] { true } );

    meta.setJSScripts( new ScriptValuesScript[] {
      new ScriptValuesScript( ScriptValuesScript.TRANSFORM_SCRIPT, "script",
        "setVariable('temp', 'pass', 'r');\nstr = getVariable('temp', 'fail');" )
    } );

    ScriptValuesModData data = new ScriptValuesModData();
    transform.init();

    Object[] expectedRow = { "pass" };
    Object[] row = PipelineTestingUtil.execute( transform, /*meta, data,*/ 1, false ).get( 0 );
    PipelineTestingUtil.assertResult( expectedRow, row );
  }
}

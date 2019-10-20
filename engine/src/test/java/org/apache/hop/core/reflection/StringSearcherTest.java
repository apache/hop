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
package org.apache.hop.core.reflection;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.apache.hop.core.Condition;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.dummytrans.DummyTransMeta;
import org.apache.hop.trans.steps.filterrows.FilterRowsMeta;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StringSearcherTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws org.apache.hop.core.exception.HopException {
    HopEnvironment.init();
  }

  @Test
  public void testSearchConditionCase() {
    String dummyStepname = "Output";
    DummyTransMeta dummyMeta = new DummyTransMeta();
    String dummyStepPid = PluginRegistry.getInstance().getPluginId( StepPluginType.class, dummyMeta );
    StepMeta dummyStep = new StepMeta( dummyStepPid, dummyStepname, dummyMeta );

    List<StringSearchResult> stringList = new ArrayList<StringSearchResult>();
    StringSearcher.findMetaData( dummyStep, 0, stringList, dummyMeta, 0 );

    int checkCount = 0;
    String aResult = null;
    // Check that it found a couple of fields and emits the values properly
    for ( int i = 0; i < stringList.size(); i++ ) {
      aResult = stringList.get( i ).toString();
      if ( aResult.endsWith( "Dummy (stepid)" ) ) {
        checkCount++;
      } else if ( aResult.endsWith( "Output (name)" ) ) {
        checkCount++;
      }
      if ( checkCount == 2 ) {
        break;
      }
    }
    assertEquals( 2, checkCount );

    FilterRowsMeta filterRowsMeta = new FilterRowsMeta();
    Condition condition = new Condition();
    condition.setNegated( false );
    condition.setLeftValuename( "wibble_t" );
    condition.setRightValuename( "wobble_s" );
    condition.setFunction( org.apache.hop.core.Condition.FUNC_EQUAL );
    filterRowsMeta.setDefault();
    filterRowsMeta.setCondition( condition );

    String filterRowsPluginPid = PluginRegistry.getInstance().getPluginId( StepPluginType.class, filterRowsMeta );
    StepMeta filterRowsStep = new StepMeta( filterRowsPluginPid, "Filter Rows", filterRowsMeta );

    stringList.clear();
    StringSearcher.findMetaData( filterRowsStep, 0, stringList, filterRowsMeta, 0 );

    checkCount = 0;
    for ( int i = 0; i < stringList.size(); i++ ) {
      aResult = stringList.get( i ).toString();
      if ( aResult.endsWith( "FilterRows (stepid)" ) ) {
        checkCount++;
      } else  if ( aResult.endsWith( "Filter Rows (name)" ) ) {
        checkCount++;
      }
      if ( checkCount == 2 ) {
        break;
      }
    }
    assertEquals( 2, checkCount );
  }
}

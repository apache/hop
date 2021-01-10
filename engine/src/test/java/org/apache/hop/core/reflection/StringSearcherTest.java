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
package org.apache.hop.core.reflection;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;

//import org.apache.hop.pipeline.transforms.filterrows.FilterRowsMeta;

public class StringSearcherTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws org.apache.hop.core.exception.HopException {
    HopEnvironment.init();
  }

  //TODO: Move Test
 /* @Test
  public void testSearchConditionCase() {
    String dummyTransformName = "Output";
    DummyMeta dummyMeta = new DummyMeta();
    String dummyTransformPid = PluginRegistry.getInstance().getPluginId( TransformPluginType.class, dummyMeta );
    TransformMeta dummyTransform = new TransformMeta( dummyTransformPid, dummyTransformname, dummyMeta );

    List<StringSearchResult> stringList = new ArrayList<StringSearchResult>();
    StringSearcher.findMetaData( dummyTransform, 0, stringList, dummyMeta, 0 );

    int checkCount = 0;
    String aResult = null;
    // Check that it found a couple of fields and emits the values properly
    for ( int i = 0; i < stringList.size(); i++ ) {
      aResult = stringList.get( i ).toString();
      if ( aResult.endsWith( "Dummy (transformId)" ) ) {
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

    String filterRowsPluginPid = PluginRegistry.getInstance().getPluginId( TransformPluginType.class, filterRowsMeta );
    TransformMeta filterRowsTransform = new TransformMeta( filterRowsPluginPid, "Filter Rows", filterRowsMeta );

    stringList.clear();
    StringSearcher.findMetaData( filterRowsTransform, 0, stringList, filterRowsMeta, 0 );

    checkCount = 0;
    for ( int i = 0; i < stringList.size(); i++ ) {
      aResult = stringList.get( i ).toString();
      if ( aResult.endsWith( "FilterRows (transformId)" ) ) {
        checkCount++;
      } else if ( aResult.endsWith( "Filter Rows (name)" ) ) {
        checkCount++;
      }
      if ( checkCount == 2 ) {
        break;
      }
    }
    assertEquals( 2, checkCount );
  }*/
}

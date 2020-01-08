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
package org.apache.hop.trans.safestop;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SafeStopTest {
  @Test
  public void testDownStreamStepsFinishProcessing() throws HopException {
    String path = getClass().getResource( "/safe-stop-gen-rows.ktr" ).getPath();
    TransMeta transMeta = new TransMeta( path, new Variables() );
    Trans trans = new Trans( transMeta );
    trans.execute( new String[] {} );
    trans.safeStop();
    trans.waitUntilFinished();
    assertEquals( trans.getSteps().get( 0 ).step.getLinesWritten(), trans.getSteps().get( 2 ).step.getLinesRead() );
  }

  @BeforeClass
  public static void init() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.addPluginType( StepPluginType.getInstance() );
    PluginRegistry.init();
    if ( !Props.isInitialized() ) {
      Props.init( 0 );
    }
  }
}

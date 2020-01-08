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

package org.apache.hop.trans.steps.calculator;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author Andrey Khayrutdinov
 */
public class CalculatorDataTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void dataReturnsCachedValues() throws Exception {
    HopEnvironment.init( false );

    CalculatorData data = new CalculatorData();
    ValueMetaInterface valueMeta = data.getValueMetaFor( ValueMetaInterface.TYPE_INTEGER, null );
    ValueMetaInterface shouldBeTheSame = data.getValueMetaFor( ValueMetaInterface.TYPE_INTEGER, null );
    assertTrue( "CalculatorData should cache loaded value meta instances", valueMeta == shouldBeTheSame );
  }
}

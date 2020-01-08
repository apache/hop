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

package org.apache.hop.trans.steps.concatfields;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.apache.hop.trans.steps.textfileoutput.TextFileOutputMetaTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConcatFieldsMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init( false );
  }

  @Test
  public void testLoadSave() throws HopException {
    List<String> attributes = new ArrayList<String>( TextFileOutputMetaTest.getMetaAttributes() );
    attributes.addAll( Arrays.asList( "targetFieldName", "targetFieldLength", "removeSelectedFields" ) );

    LoadSaveTester<ConcatFieldsMeta> loadSaveTester =
      new LoadSaveTester<ConcatFieldsMeta>( ConcatFieldsMeta.class, attributes, TextFileOutputMetaTest.getGetterMap(),
        TextFileOutputMetaTest.getSetterMap(), TextFileOutputMetaTest.getAttributeValidators(),
        TextFileOutputMetaTest.getTypeValidators() );

    loadSaveTester.testSerialization();
  }
}

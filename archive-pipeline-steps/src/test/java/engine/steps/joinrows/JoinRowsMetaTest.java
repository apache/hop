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
package org.apache.hop.pipeline.steps.joinrows;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.steps.loadsave.validator.ConditionLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.FieldLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class JoinRowsMetaTest {
  LoadSaveTester loadSaveTester;
  Class<JoinRowsMeta> testMetaClass = JoinRowsMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "directory", "prefix", "cacheSize", "mainStepname", "condition" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "condition", new ConditionLoadSaveValidator() );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testCleanAfterHopToRemove_NullParameter() {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();
    StepMeta stepMeta1 = new StepMeta( "Step1", mock( StepMetaInterface.class ) );
    joinRowsMeta.setMainStep( stepMeta1 );
    joinRowsMeta.setMainStepname( stepMeta1.getName() );

    // This call must not throw an exception
    joinRowsMeta.cleanAfterHopToRemove( null );

    // And no change to the step should be made
    assertEquals( stepMeta1, joinRowsMeta.getMainStep() );
    assertEquals( stepMeta1.getName(), joinRowsMeta.getMainStepname() );
  }

  @Test
  public void testCleanAfterHopToRemove_UnknownStep() {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();

    StepMeta stepMeta1 = new StepMeta( "Step1", mock( StepMetaInterface.class ) );
    StepMeta stepMeta2 = new StepMeta( "Step2", mock( StepMetaInterface.class ) );
    joinRowsMeta.setMainStep( stepMeta1 );
    joinRowsMeta.setMainStepname( stepMeta1.getName() );

    joinRowsMeta.cleanAfterHopToRemove( stepMeta2 );

    // No change to the step should be made
    assertEquals( stepMeta1, joinRowsMeta.getMainStep() );
    assertEquals( stepMeta1.getName(), joinRowsMeta.getMainStepname() );
  }

  @Test
  public void testCleanAfterHopToRemove_ReferredStep() {
    JoinRowsMeta joinRowsMeta = new JoinRowsMeta();

    StepMeta stepMeta1 = new StepMeta( "Step1", mock( StepMetaInterface.class ) );
    joinRowsMeta.setMainStep( stepMeta1 );
    joinRowsMeta.setMainStepname( stepMeta1.getName() );

    joinRowsMeta.cleanAfterHopToRemove( stepMeta1 );

    // No change to the step should be made
    assertNull( joinRowsMeta.getMainStep() );
    assertNull( joinRowsMeta.getMainStepname() );
  }
}

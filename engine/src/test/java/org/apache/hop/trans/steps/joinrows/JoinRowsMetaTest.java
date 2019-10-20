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
package org.apache.hop.trans.steps.joinrows;

import static org.mockito.Mockito.*;
import static junit.framework.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Counter;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceNamingInterface;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.trans.DatabaseImpact;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepInjectionMetaEntry;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInjectionInterface;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.steps.loadsave.LoadSaveTester;
import org.apache.hop.trans.steps.loadsave.validator.ConditionLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

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

    Map<String, String> getterMap = new HashMap<String, String>();
    Map<String, String> setterMap = new HashMap<String, String>();

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

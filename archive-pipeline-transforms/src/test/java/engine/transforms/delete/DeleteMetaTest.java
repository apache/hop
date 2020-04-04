/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.delete;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class DeleteMetaTest implements InitializerInterface<TransformMetaInterface> {
  LoadSaveTester loadSaveTester;
  Class<DeleteMeta> testMetaClass = DeleteMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "schemaName", "tableName", "commitSize", "databaseMeta", "keyStream", "keyLookup", "keyCondition", "keyStream2" );

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "keyStream", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyLookup", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyCondition", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "keyStream2", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "databaseMeta", new DatabaseMetaLoadSaveValidator() );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( TransformMetaInterface someMeta ) {
    if ( someMeta instanceof DeleteMeta ) {
      ( (DeleteMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }


  private TransformMeta transformMeta;
  private Delete del;
  private DeleteData dd;
  private DeleteMeta dmi;


  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName( "delete1" );

    Map<String, String> vars = new HashMap<>();
    vars.put( "max.sz", "10" );
    pipelineMeta.injectVariables( vars );

    dmi = new DeleteMeta();
    dd = new DeleteData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId( TransformPluginType.class, dmi );

    transformMeta = new TransformMeta( deletePid, "delete", dmi );
    Pipeline pipeline = new Pipeline( pipelineMeta );
    pipelineMeta.addTransform( transformMeta );
    del = new Delete( transformMeta, dd, 1, pipelineMeta, pipeline );
    del.copyVariablesFrom( pipelineMeta );
  }

  @Test
  public void testCommitCountFixed() {
    dmi.setCommitSize( "100" );
    assertTrue( dmi.getCommitSize( del ) == 100 );
  }

  @Test
  public void testCommitCountVar() {
    dmi.setCommitSize( "${max.sz}" );
    assertTrue( dmi.getCommitSize( del ) == 10 );
  }

  @Test
  public void testCommitCountMissedVar() {
    dmi.setCommitSize( "missed-var" );
    try {
      dmi.getCommitSize( del );
      fail();
    } catch ( Exception ex ) {
    }
  }
}

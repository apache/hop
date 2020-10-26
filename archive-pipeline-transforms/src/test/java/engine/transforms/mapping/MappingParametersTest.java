/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.TransformMockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

public class MappingParametersTest {

  private Mapping transform;
  private Pipeline pipeline;
  private PipelineMeta pipelineMeta;

  @Before
  public void setUp() throws Exception {
    transform = TransformMockUtil.getTransform( Mapping.class, MappingMeta.class, "junit" );
    pipeline = Mockito.mock( Pipeline.class );
    pipelineMeta = Mockito.mock( PipelineMeta.class );
  }

  @After
  public void tearDown() throws Exception {
    transform = null;
    pipeline = null;
    pipelineMeta = null;
  }

  /**
   * PDI-3064 Test parent pipeline overrides parameters for child pipeline.
   *
   * @throws HopException
   */
  @Test
  public void testOverrideMappingParameters() throws HopException {
    MappingParameters param = Mockito.mock( MappingParameters.class );
    Mockito.when( param.getVariable() ).thenReturn( new String[] { "a", "b" } );
    Mockito.when( param.getInputField() ).thenReturn( new String[] { "11", "12" } );
    Mockito.when( param.isInheritingAllVariables() ).thenReturn( true );
    when( pipelineMeta.listParameters() ).thenReturn( new String[] { "a" } );
    TransformWithMappingMeta
      .activateParams( pipeline, pipeline, transform, pipelineMeta.listParameters(), param.getVariable(), param.getInputField(), param.isInheritingAllVariables() );
    // parameters was overridden 2 times
    // new call of setParameterValue added in TransformWithMappingMeta - wantedNumberOfInvocations is now to 2
    Mockito.verify( pipeline, Mockito.times( 2 ) ).setParameterValue( Mockito.anyString(), Mockito.anyString() );
    Mockito.verify( pipeline, Mockito.times( 1 ) ).setVariable( Mockito.anyString(), Mockito.anyString() );
  }

  /**
   * Regression of PDI-3064 : keep correct 'inherit all variables' settings. This is a case for 'do not override'
   *
   * @throws HopException
   */
  @Test
  public void testDoNotOverrideMappingParametes() throws HopException {
    prepareMappingParametesActions( false );
    Mockito.verify( pipelineMeta, never() ).copyVariablesFrom( Mockito.any( IVariables.class ) );
  }

  private void prepareMappingParametesActions( boolean override ) throws HopException {
    MappingMeta meta = new MappingMeta();

    MappingParameters mapPar = new MappingParameters();
    mapPar.setInheritingAllVariables( override );
    meta.setMappingParameters( mapPar );

    MappingData data = new MappingData();
    transform.init();
  }

}

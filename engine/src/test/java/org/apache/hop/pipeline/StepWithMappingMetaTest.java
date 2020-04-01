/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.apache.hop.pipeline;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.step.StepMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Created by Yury_Bakhmutski on 2/8/2017.
 */
@RunWith( PowerMockRunner.class )
public class StepWithMappingMetaTest {

  @Mock
  PipelineMeta pipelineMeta;

  @Before
  public void setupBefore() throws Exception {
    // Without initialization of the Hop Environment, the load of the pipeline fails
    // when run in Windows (saying it cannot find the Database plugin ID for Oracle). Digging into
    // it I discovered that it's during the read of the shared objects xml which doesn't reference Oracle
    // at all. Initializing the environment fixed everything.
    HopEnvironment.init();
  }

  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  @Ignore // TODO: move database connections out of .hpls and into a memory metastore if needed
  public void loadMappingMetaTest_PathShouldBeTakenFromParentPipeline() throws Exception {

    String fileName = "subpipeline-executor-sub.hpl";
    Path parentFolder = Paths.get( getClass().getResource( "subpipeline-executor-sub.hpl)" ).toURI() ).getParent();

    //we have pipeline
    VariableSpace variables = new Variables();
    variables.setVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY, parentFolder.toString() );
    PipelineMeta parentPipelineMeta = new PipelineMeta( variables );

    //we have step in this pipeline
    StepMeta stepMeta = new StepMeta();
    stepMeta.setParentPipelineMeta( parentPipelineMeta );

    //attach the executor to step which was described above
    StepWithMappingMeta mappingMetaMock = mock( StepWithMappingMeta.class );
    when( mappingMetaMock.getFileName() ).thenReturn( "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}/" + fileName );
    when( mappingMetaMock.getParentStepMeta() ).thenReturn( stepMeta );

    //we will try to load the subtras which was linked at the step metas
    PipelineMeta pipelineMeta = StepWithMappingMeta.loadMappingMeta( mappingMetaMock, null, variables, true );

    StringBuilder expected = new StringBuilder( parentFolder.toUri().toString() );
    /**
     * we need to remove "/" at the end of expected string because during load the pipeline from file
     * internal variables will be replaced by uri from kettle vfs
     * check the follow points
     * {@link PipelineMeta#setInternalFilenameHopVariables(VariableSpace)}
     *
     */
    Assert.assertEquals( expected.deleteCharAt( expected.length() - 1 ).toString(), pipelineMeta.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
  }


  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  public void activateParamsTest() throws Exception {
    String childParam = "childParam";
    String childValue = "childValue";
    String paramOverwrite = "paramOverwrite";
    String parentValue = "parentValue";
    String stepValue = "stepValue";

    VariableSpace parent = new Variables();
    parent.setVariable( paramOverwrite, parentValue );

    PipelineMeta childVariableSpace = new PipelineMeta();
    childVariableSpace.addParameterDefinition( childParam, "", "" );
    childVariableSpace.setParameterValue( childParam, childValue );

    String[] parameters = childVariableSpace.listParameters();
    StepWithMappingMeta.activateParams( childVariableSpace, childVariableSpace, parent,
      parameters, new String[] { childParam, paramOverwrite }, new String[] { childValue, stepValue }, true );

    Assert.assertEquals( childValue, childVariableSpace.getVariable( childParam ) );
    // the step parameter prevails
    Assert.assertEquals( stepValue, childVariableSpace.getVariable( paramOverwrite ) );
  }

  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  public void activateParamsWithFalsePassParametersFlagTest() throws Exception {
    String childParam = "childParam";
    String childValue = "childValue";
    String paramOverwrite = "paramOverwrite";
    String parentValue = "parentValue";
    String stepValue = "stepValue";
    String parentAndChildParameter = "parentAndChildParameter";

    VariableSpace parent = new Variables();
    parent.setVariable( paramOverwrite, parentValue );
    parent.setVariable( parentAndChildParameter, parentValue );

    PipelineMeta childVariableSpace = new PipelineMeta();
    childVariableSpace.addParameterDefinition( childParam, "", "" );
    childVariableSpace.setParameterValue( childParam, childValue );
    childVariableSpace.addParameterDefinition( parentAndChildParameter, "", "" );
    childVariableSpace.setParameterValue( parentAndChildParameter, childValue );

    String[] parameters = childVariableSpace.listParameters();
    StepWithMappingMeta.activateParams( childVariableSpace, childVariableSpace, parent,
      parameters, new String[] { childParam, paramOverwrite }, new String[] { childValue, stepValue }, false );

    Assert.assertEquals( childValue, childVariableSpace.getVariable( childParam ) );
    // the step parameter prevails
    Assert.assertEquals( stepValue, childVariableSpace.getVariable( paramOverwrite ) );

    Assert.assertEquals( childValue, childVariableSpace.getVariable( parentAndChildParameter ) );
  }

  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  public void activateParamsWithTruePassParametersFlagTest() throws Exception {
    String childParam = "childParam";
    String childValue = "childValue";
    String paramOverwrite = "paramOverwrite";
    String parentValue = "parentValue";
    String stepValue = "stepValue";
    String parentAndChildParameter = "parentAndChildParameter";

    VariableSpace parent = new Variables();
    parent.setVariable( paramOverwrite, parentValue );
    parent.setVariable( parentAndChildParameter, parentValue );

    PipelineMeta childVariableSpace = new PipelineMeta();
    childVariableSpace.addParameterDefinition( childParam, "", "" );
    childVariableSpace.setParameterValue( childParam, childValue );
    childVariableSpace.addParameterDefinition( parentAndChildParameter, "", "" );
    childVariableSpace.setParameterValue( parentAndChildParameter, childValue );

    String[] parameters = childVariableSpace.listParameters();

    StepWithMappingMeta.activateParams( childVariableSpace, childVariableSpace, parent,
      parameters, new String[] { childParam, paramOverwrite }, new String[] { childValue, stepValue }, true );

    //childVariableSpace.setVariable( parentAndChildParameter, parentValue);

    Assert.assertEquals( childValue, childVariableSpace.getVariable( childParam ) );
    // the step parameter prevails
    Assert.assertEquals( stepValue, childVariableSpace.getVariable( paramOverwrite ) );

    Assert.assertEquals( parentValue, childVariableSpace.getVariable( parentAndChildParameter ) );
  }

  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  public void activateParamsTestWithNoParameterChild() throws Exception {
    String newParam = "newParamParent";
    String parentValue = "parentValue";

    PipelineMeta parentMeta = new PipelineMeta();
    PipelineMeta childVariableSpace = new PipelineMeta();

    String[] parameters = childVariableSpace.listParameters();

    StepWithMappingMeta.activateParams( childVariableSpace, childVariableSpace, parentMeta,
      parameters, new String[] { newParam }, new String[] { parentValue }, true );

    Assert.assertEquals( parentValue, childVariableSpace.getParameterValue( newParam ) );
  }


  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  public void replaceVariablesWithJobInternalVariablesTest() {
    String variableOverwrite = "paramOverwrite";
    String variableChildOnly = "childValueVariable";
    String[] jobVariables = Const.INTERNAL_JOB_VARIABLES;
    VariableSpace ChildVariables = new Variables();
    VariableSpace replaceByParentVariables = new Variables();

    for ( String internalVariable : jobVariables ) {
      ChildVariables.setVariable( internalVariable, "childValue" );
      replaceByParentVariables.setVariable( internalVariable, "parentValue" );
    }

    ChildVariables.setVariable( variableChildOnly, "childValueVariable" );
    ChildVariables.setVariable( variableOverwrite, "childNotInternalValue" );
    replaceByParentVariables.setVariable( variableOverwrite, "parentNotInternalValue" );

    StepWithMappingMeta.replaceVariableValues( ChildVariables, replaceByParentVariables );
    // do not replace internal variables
    Assert.assertEquals( "childValue", ChildVariables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
    // replace non internal variables
    Assert.assertEquals( "parentNotInternalValue", ChildVariables.getVariable( variableOverwrite ) );
    // keep child only variables
    Assert.assertEquals( variableChildOnly, ChildVariables.getVariable( variableChildOnly ) );

  }

  @Test
  @PrepareForTest( StepWithMappingMeta.class )
  public void replaceVariablesWithPipelineInternalVariablesTest() {
    String variableOverwrite = "paramOverwrite";
    String variableChildOnly = "childValueVariable";
    String[] jobVariables = Const.INTERNAL_PIPELINE_VARIABLES;
    VariableSpace ChildVariables = new Variables();
    VariableSpace replaceByParentVariables = new Variables();

    for ( String internalVariable : jobVariables ) {
      ChildVariables.setVariable( internalVariable, "childValue" );
      replaceByParentVariables.setVariable( internalVariable, "parentValue" );
    }

    ChildVariables.setVariable( variableChildOnly, "childValueVariable" );
    ChildVariables.setVariable( variableOverwrite, "childNotInternalValue" );
    replaceByParentVariables.setVariable( variableOverwrite, "parentNotInternalValue" );

    StepWithMappingMeta.replaceVariableValues( ChildVariables, replaceByParentVariables );
    // do not replace internal variables
    Assert.assertEquals( "childValue", ChildVariables.getVariable( Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY ) );
    // replace non internal variables
    Assert.assertEquals( "parentNotInternalValue", ChildVariables.getVariable( variableOverwrite ) );
    // keep child only variables
    Assert.assertEquals( variableChildOnly, ChildVariables.getVariable( variableChildOnly ) );

  }
}

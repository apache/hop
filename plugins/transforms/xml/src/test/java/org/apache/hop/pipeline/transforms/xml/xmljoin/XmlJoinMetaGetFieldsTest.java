/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2016-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.pipeline.transforms.xml.xmljoin;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


public class XmlJoinMetaGetFieldsTest {

  XmlJoinMeta xmlJoinMeta;
  PipelineMeta transMeta;

  @Before
  public void setup() throws Exception {
    xmlJoinMeta = new XmlJoinMeta();
    transMeta = mock( PipelineMeta.class );
  }

  @Test
  public void testGetFieldsReturnTargetStepFieldsPlusResultXmlField() throws Exception {
    String sourceXmlStep = "source xml step name";
    String sourceStepField = "source field test name";
    String targetStepField = "target field test name";
    String resultXmlFieldName = "result xml field name";
    RowMeta rowMetaPreviousSteps = new RowMeta();
    rowMetaPreviousSteps.addValueMeta( new ValueMetaString( sourceStepField ) );
    xmlJoinMeta.setSourceXmlStep( sourceXmlStep );
    xmlJoinMeta.setValueXmlField( "result xml field name" );
    TransformMeta sourceStepMeta = new TransformMeta();
    sourceStepMeta.setName( sourceXmlStep );

    doReturn( sourceStepMeta ).when( transMeta ).findTransform( sourceXmlStep );
    doReturn( rowMetaPreviousSteps ).when( transMeta ).getTransformFields( sourceStepMeta, null, null );


    RowMeta rowMeta = new RowMeta();
    ValueMetaString keepValueMeta = new ValueMetaString( targetStepField );
    ValueMetaString removeValueMeta = new ValueMetaString( sourceStepField );
    rowMeta.addValueMeta( keepValueMeta );
    rowMeta.addValueMeta( removeValueMeta );

    xmlJoinMeta.getFields( rowMeta, "testStepName", null, null, transMeta, null );
    assertEquals( 2, rowMeta.size() );
    String[] strings = rowMeta.getFieldNames();
    assertEquals( targetStepField, strings[0] );
    assertEquals( resultXmlFieldName, strings[1] );
  }

  @Test
  public void testGetFieldsReturnTargetStepFieldsWithDuplicates() throws Exception {
    // Source Step
    String sourceXmlStep = "source xml step name";
    String sourceStepField1 = "a";
    String sourceStepField2 = "b";

    // Target Step
    String targetXmlStep = "target xml step name";
    String targetStepField1 = "b";
    String targetStepField2 = "c";

    // XML Join Result
    String resultXmlFieldName = "result xml field name";

    // Source Row Meta
    RowMeta rowMetaPreviousSourceStep = new RowMeta();
    rowMetaPreviousSourceStep.addValueMeta( new ValueMetaString( sourceStepField1) );
    rowMetaPreviousSourceStep.addValueMeta( new ValueMetaString( sourceStepField2) );

    // Set source step in XML Join step.
    xmlJoinMeta.setSourceXmlStep( sourceXmlStep );
    TransformMeta sourceStepMeta = new TransformMeta();
    sourceStepMeta.setName( sourceXmlStep );

    doReturn( sourceStepMeta ).when( transMeta ).findTransform( sourceXmlStep );
    doReturn( rowMetaPreviousSourceStep ).when( transMeta ).getTransformFields( sourceStepMeta, null, null );

    // Target Row Meta
    RowMeta rowMetaPreviousTargetStep = new RowMeta();
    rowMetaPreviousTargetStep.addValueMeta( new ValueMetaString( targetStepField1) );
    rowMetaPreviousTargetStep.addValueMeta( new ValueMetaString( targetStepField2) );

    // Set target step in XML Join step.
    xmlJoinMeta.setTargetXmlStep( targetXmlStep );
    TransformMeta targetStepMeta = new TransformMeta();
    targetStepMeta.setName( targetXmlStep );

    doReturn( targetStepMeta ).when( transMeta ).findTransform( targetXmlStep );
    doReturn( rowMetaPreviousTargetStep ).when( transMeta ).getTransformFields( targetStepMeta, null, null );

    // Set result field name
    xmlJoinMeta.setValueXmlField( resultXmlFieldName );

    RowMeta rowMeta = new RowMeta();
    ValueMetaString removeValueMeta1 = new ValueMetaString( "a" );
    rowMeta.addValueMeta( removeValueMeta1 );
    ValueMetaString keepValueMeta1 = new ValueMetaString( "b" );
    rowMeta.addValueMeta( keepValueMeta1 );
    ValueMetaString keepValueMeta2 = new ValueMetaString( "c" );
    rowMeta.addValueMeta( keepValueMeta2 );

    // Get output fields
    xmlJoinMeta.getFields( rowMeta, "testStepName", null, null, transMeta, null );
    assertEquals( 3, rowMeta.size() );
    String[] strings = rowMeta.getFieldNames();
    assertEquals( "b", strings[0] );
    assertEquals( "c", strings[1] );
    assertEquals( "result xml field name", strings[2] );
  }
}

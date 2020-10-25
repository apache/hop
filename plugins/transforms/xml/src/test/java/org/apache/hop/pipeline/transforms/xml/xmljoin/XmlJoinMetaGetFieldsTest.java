/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
  public void testGetFieldsReturnTargetTransformFieldsPlusResultXmlField() throws Exception {
    String sourceXmlTransform = "source xml transform name";
    String sourceTransformField = "source field test name";
    String targetTransformField = "target field test name";
    String resultXmlFieldName = "result xml field name";
    RowMeta rowMetaPreviousTransforms = new RowMeta();
    rowMetaPreviousTransforms.addValueMeta( new ValueMetaString( sourceTransformField ) );
    xmlJoinMeta.setSourceXmlTransform( sourceXmlTransform );
    xmlJoinMeta.setValueXmlField( "result xml field name" );
    TransformMeta sourceTransformMeta = new TransformMeta();
    sourceTransformMeta.setName( sourceXmlTransform );

    doReturn( sourceTransformMeta ).when( transMeta ).findTransform( sourceXmlTransform );
    doReturn( rowMetaPreviousTransforms ).when( transMeta ).getTransformFields( sourceTransformMeta, null, null );


    RowMeta rowMeta = new RowMeta();
    ValueMetaString keepValueMeta = new ValueMetaString( targetTransformField );
    ValueMetaString removeValueMeta = new ValueMetaString( sourceTransformField );
    rowMeta.addValueMeta( keepValueMeta );
    rowMeta.addValueMeta( removeValueMeta );

    xmlJoinMeta.getFields( rowMeta, "testTransformName", null, null, transMeta, null );
    assertEquals( 2, rowMeta.size() );
    String[] strings = rowMeta.getFieldNames();
    assertEquals( targetTransformField, strings[0] );
    assertEquals( resultXmlFieldName, strings[1] );
  }

  @Test
  public void testGetFieldsReturnTargetTransformFieldsWithDuplicates() throws Exception {
    // Source Transform
    String sourceXmlTransform = "source xml transform name";
    String sourceTransformField1 = "a";
    String sourceTransformField2 = "b";

    // Target Transform
    String targetXmlTransform = "target xml transform name";
    String targetTransformField1 = "b";
    String targetTransformField2 = "c";

    // XML Join Result
    String resultXmlFieldName = "result xml field name";

    // Source Row Meta
    RowMeta rowMetaPreviousSourceTransform = new RowMeta();
    rowMetaPreviousSourceTransform.addValueMeta( new ValueMetaString( sourceTransformField1) );
    rowMetaPreviousSourceTransform.addValueMeta( new ValueMetaString( sourceTransformField2) );

    // Set source transform in XML Join transform.
    xmlJoinMeta.setSourceXmlTransform( sourceXmlTransform );
    TransformMeta sourceTransformMeta = new TransformMeta();
    sourceTransformMeta.setName( sourceXmlTransform );

    doReturn( sourceTransformMeta ).when( transMeta ).findTransform( sourceXmlTransform );
    doReturn( rowMetaPreviousSourceTransform ).when( transMeta ).getTransformFields( sourceTransformMeta, null, null );

    // Target Row Meta
    RowMeta rowMetaPreviousTargetTransform = new RowMeta();
    rowMetaPreviousTargetTransform.addValueMeta( new ValueMetaString( targetTransformField1) );
    rowMetaPreviousTargetTransform.addValueMeta( new ValueMetaString( targetTransformField2) );

    // Set target transform in XML Join transform.
    xmlJoinMeta.setTargetXmlTransform( targetXmlTransform );
    TransformMeta targetTransformMeta = new TransformMeta();
    targetTransformMeta.setName( targetXmlTransform );

    doReturn( targetTransformMeta ).when( transMeta ).findTransform( targetXmlTransform );
    doReturn( rowMetaPreviousTargetTransform ).when( transMeta ).getTransformFields( targetTransformMeta, null, null );

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
    xmlJoinMeta.getFields( rowMeta, "testTransformName", null, null, transMeta, null );
    assertEquals( 3, rowMeta.size() );
    String[] strings = rowMeta.getFieldNames();
    assertEquals( "b", strings[0] );
    assertEquals( "c", strings[1] );
    assertEquals( "result xml field name", strings[2] );
  }
}

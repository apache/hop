/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.fieldsplitter;

import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Khayrutdinov
 */
@RunWith( PowerMockRunner.class )
public class FieldSplitter_EmptyStringVsNull_Test {
  private TransformMockHelper<FieldSplitterMeta, ITransformData> helper;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    helper = TransformMockUtil.getTransformMockHelper( FieldSplitterMeta.class, "FieldSplitter_EmptyStringVsNull_Test" );
  }

  @After
  public void cleanUp() {
    helper.cleanUp();
  }

  @Test
  public void emptyAndNullsAreNotDifferent() throws Exception {
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N" );
    List<Object[]> expected = Arrays.asList(
      new Object[] { "a", "", "a" },
      new Object[] { "b", null, "b" },
      new Object[] { null }
    );
    executeAndAssertResults( expected );
  }


  @Test
  public void emptyAndNullsAreDifferent() throws Exception {
    System.setProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "Y" );
    List<Object[]> expected = Arrays.asList(
      new Object[] { "a", "", "a" },
      new Object[] { "b", "", "b" },
      new Object[] { "", "", "" }
    );
    executeAndAssertResults( expected );
  }

  private void executeAndAssertResults( List<Object[]> expected ) throws Exception {
    FieldSplitterMeta meta = new FieldSplitterMeta();
    meta.allocate( 3 );
    meta.setFieldName( new String[] { "s1", "s2", "s3" } );
    meta.setFieldType( new int[] { IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING } );
    meta.setSplitField( "string" );
    meta.setDelimiter( "," );

    FieldSplitterData data = new FieldSplitterData();

    FieldSplitter transform = createAndInitTransform( meta, data );

    RowMeta input = new RowMeta();
    input.addValueMeta( new ValueMetaString( "string" ) );
    transform.setInputRowMeta( input );

    transform = spy( transform );
    doReturn( new String[] { "a, ,a" } )
      .doReturn( new String[] { "b,,b" } )
      .doReturn( new String[] { null } )
      .when( transform ).getRow();

    List<Object[]> actual = PipelineTestingUtil.execute( transform, 3, false );
    PipelineTestingUtil.assertResult( expected, actual );
  }

  private FieldSplitter createAndInitTransform( FieldSplitterMeta meta, FieldSplitterData data ) throws Exception {
    when( helper.transformMeta.getTransform() ).thenReturn( meta );

    FieldSplitter transform = new FieldSplitter( helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline );
    transform.init();
    return transform;
  }
}

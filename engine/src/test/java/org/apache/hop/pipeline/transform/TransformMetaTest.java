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

package org.apache.hop.pipeline.transform;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.util.AbstractTransformMeta;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.utils.TestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Khayrutdinov
 */
public class TransformMetaTest {

  private static final Random rand = new Random();

  private static final String TRANSFORM_ID = "transform_id";

  @Test
  public void cloning() throws Exception {
    TransformMeta meta = createTestMeta();
    TransformMeta clone = (TransformMeta) meta.clone();
    assertEquals( meta, clone );
  }

  @Test
  public void testEqualsHashCodeConsistency() throws Exception {
    TransformMeta transform = new TransformMeta();
    transform.setName( "transform" );
    TestUtils.checkEqualsHashCodeConsistency( transform, transform );

    TransformMeta transformSame = new TransformMeta();
    transformSame.setName( "transform" );
    assertTrue( transform.equals( transformSame ) );
    TestUtils.checkEqualsHashCodeConsistency( transform, transformSame );

    TransformMeta transformCaps = new TransformMeta();
    transformCaps.setName( "TRANSFORM" );
    TestUtils.checkEqualsHashCodeConsistency( transform, transformCaps );

    TransformMeta transformOther = new TransformMeta();
    transformOther.setName( "something else" );
    TestUtils.checkEqualsHashCodeConsistency( transform, transformOther );
  }

  @Test
  public void transformMetaXmlConsistency() throws Exception {
    TransformMeta meta = new TransformMeta( "id", "name", null );
    ITransformMeta smi = new Missing( meta.getName(), meta.getTransformPluginId() );
    meta.setTransform( smi );
    TransformMeta fromXml = TransformMeta.fromXml( meta.getXml() );
    assertThat( meta.getXml(), is( fromXml.getXml() ) );
  }

  private static TransformMeta createTestMeta() throws Exception {
    ITransformMeta transformMetaInterface = mock( AbstractTransformMeta.class );
    when( transformMetaInterface.clone() ).thenReturn( transformMetaInterface );

    TransformMeta meta = new TransformMeta( TRANSFORM_ID, "transformName", transformMetaInterface );
    meta.setSelected( true );
    meta.setDistributes( false );
    meta.setCopiesString( "2" );
    meta.setLocation( 1, 2 );
    meta.setDescription( "description" );
    meta.setTerminator( true );

    boolean shouldDistribute = rand.nextBoolean();
    meta.setDistributes( shouldDistribute );
    if ( shouldDistribute ) {
      meta.setRowDistribution( selectRowDistribution() );
    }

    Map<String, Map<String, String>> attributes = new HashMap<>();
    Map<String, String> map1 = new HashMap<>();
    map1.put( "1", "1" );
    Map<String, String> map2 = new HashMap<>();
    map2.put( "2", "2" );

    attributes.put( "qwerty", map1 );
    attributes.put( "asdfg", map2 );
    meta.setAttributesMap( attributes );

    meta.setTransformPartitioningMeta( createTransformPartitioningMeta( "transformMethod", "transformSchema" ) );
    meta.setTargetTransformPartitioningMeta( createTransformPartitioningMeta( "targetMethod", "targetSchema" ) );

    return meta;
  }

  private static IRowDistribution selectRowDistribution() {
    return new FakeRowDistribution();
  }

  private static TransformPartitioningMeta createTransformPartitioningMeta( String method, String schemaName ) throws Exception {
    TransformPartitioningMeta meta = new TransformPartitioningMeta( method, new PartitionSchema( schemaName,
      Collections.<String>emptyList() ) );
    meta.setPartitionSchema( new PartitionSchema() );
    return meta;
  }

  private static void assertEquals( TransformMeta meta, TransformMeta another ) {
    assertTrue( EqualsBuilder.reflectionEquals( meta, another, false, TransformMeta.class,
      new String[] { "location", "targetTransformPartitioningMeta" } ) );

    boolean manualCheck = new EqualsBuilder()
      .append( meta.getLocation().x, another.getLocation().x )
      .append( meta.getLocation().y, another.getLocation().y )
      .isEquals();
    assertTrue( manualCheck );
  }
}

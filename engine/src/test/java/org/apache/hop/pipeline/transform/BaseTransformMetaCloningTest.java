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

import org.apache.hop.core.database.Database;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class BaseTransformMetaCloningTest {

  @Test
  public void testClone() throws Exception {
    final Database db1 = mock( Database.class );
    final Database db2 = mock( Database.class );
    final TransformMeta transformMeta = mock( TransformMeta.class );

    BaseTransformMeta meta = new BaseTransformMeta();
    meta.setChanged( true );
    meta.databases = new Database[] { db1, db2 };
    ITransformIOMeta ioMeta = new TransformIOMeta( true, false, false, false, false, false );
    meta.setTransformIOMeta( ioMeta );
    meta.parentTransformMeta = transformMeta;

    BaseTransformMeta clone = (BaseTransformMeta) meta.clone();
    assertTrue( clone.hasChanged() );

    // is it OK ?
    assertTrue( clone.databases == meta.databases );
    assertArrayEquals( meta.databases, clone.databases );

    assertEquals( meta.parentTransformMeta, clone.parentTransformMeta );

    ITransformIOMeta cloneIOMeta = clone.getTransformIOMeta();
    assertNotNull( cloneIOMeta );
    assertEquals( ioMeta.isInputAcceptor(), cloneIOMeta.isInputAcceptor() );
    assertEquals( ioMeta.isInputDynamic(), cloneIOMeta.isInputDynamic() );
    assertEquals( ioMeta.isInputOptional(), cloneIOMeta.isInputOptional() );
    assertEquals( ioMeta.isOutputDynamic(), cloneIOMeta.isOutputDynamic() );
    assertEquals( ioMeta.isOutputProducer(), cloneIOMeta.isOutputProducer() );
    assertEquals( ioMeta.isSortedDataRequired(), cloneIOMeta.isSortedDataRequired() );
    assertNotNull( cloneIOMeta.getInfoStreams() );
    assertEquals( 0, cloneIOMeta.getInfoStreams().size() );
  }

  @Test
  public void testCloneWithInfoTransforms() throws Exception {
    final Database db1 = mock( Database.class );
    final Database db2 = mock( Database.class );
    final TransformMeta transformMeta = mock( TransformMeta.class );

    BaseTransformMeta meta = new BaseTransformMeta();
    meta.setChanged( true );
    meta.databases = new Database[] { db1, db2 };
    ITransformIOMeta ioMeta = new TransformIOMeta( true, false, false, false, false, false );
    meta.setTransformIOMeta( ioMeta );

    final String refTransformName = "referenced transform";
    final TransformMeta refTransformMeta = mock( TransformMeta.class );
    doReturn( refTransformName ).when( refTransformMeta ).getName();
    IStream stream = new Stream( IStream.StreamType.INFO, refTransformMeta, null, null, refTransformName );
    ioMeta.addStream( stream );
    meta.parentTransformMeta = transformMeta;

    BaseTransformMeta clone = (BaseTransformMeta) meta.clone();
    assertTrue( clone.hasChanged() );

    // is it OK ?
    assertTrue( clone.databases == meta.databases );
    assertArrayEquals( meta.databases, clone.databases );

    assertEquals( meta.parentTransformMeta, clone.parentTransformMeta );


    ITransformIOMeta cloneIOMeta = clone.getTransformIOMeta();
    assertNotNull( cloneIOMeta );
    assertEquals( ioMeta.isInputAcceptor(), cloneIOMeta.isInputAcceptor() );
    assertEquals( ioMeta.isInputDynamic(), cloneIOMeta.isInputDynamic() );
    assertEquals( ioMeta.isInputOptional(), cloneIOMeta.isInputOptional() );
    assertEquals( ioMeta.isOutputDynamic(), cloneIOMeta.isOutputDynamic() );
    assertEquals( ioMeta.isOutputProducer(), cloneIOMeta.isOutputProducer() );
    assertEquals( ioMeta.isSortedDataRequired(), cloneIOMeta.isSortedDataRequired() );

    final List<IStream> clonedInfoStreams = cloneIOMeta.getInfoStreams();
    assertNotNull( clonedInfoStreams );
    assertEquals( 1, clonedInfoStreams.size() );

    final IStream clonedStream = clonedInfoStreams.get( 0 );
    assertNotSame( stream, clonedStream );
    assertEquals( stream.getStreamType(), clonedStream.getStreamType() );
    assertEquals( refTransformName, clonedStream.getTransformName() );

    assertSame( refTransformMeta, clonedStream.getTransformMeta() ); // PDI-15799
  }
}

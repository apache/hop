/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.mock;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogChannelFactory;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogMessageInterface;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TransformMockHelper<Meta extends TransformMetaInterface, Data extends ITransformData> {
  public final TransformMeta transformMeta;
  public final Data iTransformData;
  public final PipelineMeta pipelineMeta;
  public final Pipeline pipeline;
  public final Meta initTransformMetaInterface;
  public final Data initTransformDataInterface;
  public final Meta processRowsTransformMetaInterface;
  public final Data processRowsTransformDataInterface;
  public final LogChannelInterface logChannelInterface;
  public final LogChannelFactory logChannelFactory;
  public final LogChannelFactory originalLogChannelFactory;

  public TransformMockHelper( String transformName, Class<Meta> transformMetaClass, Class<Data> transformDataClass ) {
    originalLogChannelFactory = HopLogStore.getLogChannelFactory();
    logChannelFactory = mock( LogChannelFactory.class );
    logChannelInterface = mock( LogChannelInterface.class );
    HopLogStore.setLogChannelFactory( logChannelFactory );
    transformMeta = mock( TransformMeta.class );
    when( transformMeta.getName() ).thenReturn( transformName );
    iTransformData = mock( transformDataClass );
    pipelineMeta = mock( PipelineMeta.class );
    when( pipelineMeta.findTransform( transformName ) ).thenReturn( transformMeta );
    pipeline = mock( Pipeline.class );
    initTransformMetaInterface = mock( transformMetaClass );
    initTransformDataInterface = mock( transformDataClass );
    processRowsTransformDataInterface = mock( transformDataClass );
    processRowsTransformMetaInterface = mock( transformMetaClass );
  }

  public RowSet getMockInputRowSet( Object[]... rows ) {
    return getMockInputRowSet( asList( rows ) );
  }

  public RowSet getMockInputRowSet( final List<Object[]> rows ) {
    final AtomicInteger index = new AtomicInteger( 0 );
    RowSet rowSet = mock( RowSet.class, Mockito.RETURNS_MOCKS );
    Answer<Object[]> answer = new Answer<Object[]>() {
      @Override
      public Object[] answer( InvocationOnMock invocation ) throws Throwable {
        int i = index.getAndIncrement();
        return i < rows.size() ? rows.get( i ) : null;
      }
    };
    when( rowSet.getRowWait( anyLong(), any( TimeUnit.class ) ) ).thenAnswer( answer );
    when( rowSet.getRow() ).thenAnswer( answer );
    when( rowSet.isDone() ).thenAnswer( new Answer<Boolean>() {

      @Override
      public Boolean answer( InvocationOnMock invocation ) throws Throwable {
        return index.get() >= rows.size();
      }
    } );
    return rowSet;
  }

  public static List<Object[]> asList( Object[]... objects ) {
    List<Object[]> result = new ArrayList<Object[]>();
    Collections.addAll( result, objects );
    return result;
  }

  public void cleanUp() {
    HopLogStore.setLogChannelFactory( originalLogChannelFactory );
  }

  /**
   * In case you need to use log methods during the tests
   * use redirectLog method after creating new TransformMockHelper object.
   * Examples:
   * transformMockHelper.redirectLog( System.out, LogLevel.ROWLEVEL );
   * transformMockHelper.redirectLog( new FileOutputStream("log.txt"), LogLevel.BASIC );
   */
  public void redirectLog( final OutputStream out, LogLevel channelLogLevel ) {
    final LogChannel log = spy( new LogChannel( this.getClass().getName(), true ) );
    log.setLogLevel( channelLogLevel );
    when( logChannelFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn( log );
    doAnswer( new Answer<Object>() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        Object[] args = invocation.getArguments();

        LogLevel logLevel = (LogLevel) args[ 1 ];
        LogLevel channelLogLevel = log.getLogLevel();

        if ( !logLevel.isVisible( channelLogLevel ) ) {
          return null; // not for our eyes.
        }
        if ( channelLogLevel.getLevel() >= logLevel.getLevel() ) {
          LogMessageInterface logMessage = (LogMessageInterface) args[ 0 ];
          out.write( logMessage.getMessage().getBytes() );
          out.write( '\n' );
          out.write( '\r' );
          out.flush();
          return true;
        }
        return false;
      }
    } ).when( log ).println( (LogMessageInterface) anyObject(), (LogLevel) anyObject() );
  }
}

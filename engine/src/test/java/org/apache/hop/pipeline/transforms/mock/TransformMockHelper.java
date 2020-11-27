/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.ILogMessage;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.mockito.Mockito;
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

public class TransformMockHelper<Meta extends ITransformMeta, Data extends ITransformData> {
  public final TransformMeta transformMeta;
  public final Data iTransformData;
  public final Meta iTransformMeta;
  public final PipelineMeta pipelineMeta;
  public final Pipeline pipeline;

  public final ILogChannel iLogChannel;
  public final ILogChannelFactory logChannelFactory;
  public final ILogChannelFactory originalLogChannelFactory;

  public TransformMockHelper( String transformName, Class<Meta> transformMetaClass, Class<Data> transformDataClass ) {
    originalLogChannelFactory = HopLogStore.getLogChannelFactory();
    logChannelFactory = mock( ILogChannelFactory.class );
    iLogChannel = mock( ILogChannel.class );
    HopLogStore.setLogChannelFactory( logChannelFactory );
    transformMeta = mock( TransformMeta.class );
    when( transformMeta.getName() ).thenReturn( transformName );
    iTransformData = mock( transformDataClass );
    iTransformMeta = mock( transformMetaClass );
    pipelineMeta = mock( PipelineMeta.class );
    when( pipelineMeta.findTransform( transformName ) ).thenReturn( transformMeta );
    pipeline = mock( Pipeline.class );
  }

  public IRowSet getMockInputRowSet( Object[]... rows ) {
    return getMockInputRowSet( asList( rows ) );
  }

  public IRowSet getMockInputRowSet( final List<Object[]> rows ) {
    final AtomicInteger index = new AtomicInteger( 0 );
    IRowSet rowSet = mock( IRowSet.class, Mockito.RETURNS_MOCKS );
    Answer<Object[]> answer = invocation -> {
      int i = index.getAndIncrement();
      return i < rows.size() ? rows.get( i ) : null;
    };
    when( rowSet.getRowWait( anyLong(), any( TimeUnit.class ) ) ).thenAnswer( answer );
    when( rowSet.getRow() ).thenAnswer( answer );
    when( rowSet.isDone() ).thenAnswer( (Answer<Boolean>) invocation -> index.get() >= rows.size() );
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
    when( logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn( log );
    doAnswer( (Answer<Object>) invocation -> {
      Object[] args = invocation.getArguments();

      LogLevel logLevel = (LogLevel) args[ 1 ];
      LogLevel channelLogLevel1 = log.getLogLevel();

      if ( !logLevel.isVisible( channelLogLevel1 ) ) {
        return null; // not for our eyes.
      }
      if ( channelLogLevel1.getLevel() >= logLevel.getLevel() ) {
        ILogMessage logMessage = (ILogMessage) args[ 0 ];
        out.write( logMessage.getMessage().getBytes() );
        out.write( '\n' );
        out.write( '\r' );
        out.flush();
        return true;
      }
      return false;
    } ).when( log ).println( (ILogMessage) anyObject(), (LogLevel) anyObject() );
  }
}

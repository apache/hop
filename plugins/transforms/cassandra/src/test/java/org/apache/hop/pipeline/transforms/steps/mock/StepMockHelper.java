/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.pipeline.transforms.steps.mock;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.RowSet;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.di.core.logging.KettleLogStore;
import org.apache.hop.di.core.logging.LogMessageInterface;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * Copied from kettle-engine tests. Should be deleted after introducing pentaho-common-tests project
 */
public class StepMockHelper<Meta extends ITransformMeta, Data extends ITransformData> {
  public final TransformMeta stepMeta;
  public final Data stepDataInterface;
  public final PipelineMeta transMeta;
  public final Pipeline trans;
  public final Meta initTransformMetaInterface;
  public final Data initITransformData;
  public final Meta processRowsTransformMetaInterface;
  public final Data processRowsITransformData;
  public final ILogChannel logChannelInterface;
  public final ILogChannelFactory logChannelInterfaceFactory;
  public final ILogChannelFactory originalILogChannelFactory;

  public StepMockHelper( String stepName, Class<Meta> stepMetaClass, Class<Data> stepDataClass ) {
    originalILogChannelFactory = KettleLogStore.getILogChannelFactory();
    logChannelInterfaceFactory = mock( ILogChannelFactory.class );
    logChannelInterface = mock( ILogChannel.class );
    KettleLogStore.setILogChannelFactory( logChannelInterfaceFactory );
    stepMeta = mock( TransformMeta.class );
    when( stepMeta.getName() ).thenReturn( stepName );
    stepDataInterface = mock( stepDataClass );
    transMeta = mock( PipelineMeta.class );
    when( transMeta.findStep( stepName ) ).thenReturn( stepMeta );
    trans = mock( Pipeline.class );
    initTransformMetaInterface = mock( stepMetaClass );
    initITransformData = mock( stepDataClass );
    processRowsITransformData = mock( stepDataClass );
    processRowsTransformMetaInterface = mock( stepMetaClass );
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
    KettleLogStore.setILogChannelFactory( originalILogChannelFactory );
  }

  /**
   *  In case you need to use log methods during the tests
   *  use redirectLog method after creating new StepMockHelper object.
   *  Examples:
   *    stepMockHelper.redirectLog( System.out, LogLevel.ROWLEVEL );
   *    stepMockHelper.redirectLog( new FileOutputStream("log.txt"), LogLevel.BASIC );
   */
  public void redirectLog( final OutputStream out, LogLevel channelLogLevel ) {
    final LogChannel log = spy( new LogChannel( this.getClass().getName(), true ) );
    log.setLogLevel( channelLogLevel );
    when( logChannelInterfaceFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn( log );
    doAnswer( new Answer<Object>() {
      @Override
      public Object answer( InvocationOnMock invocation ) throws Throwable {
        Object[] args = invocation.getArguments();

        LogLevel logLevel = (LogLevel) args[1];
        LogLevel channelLogLevel = log.getLogLevel();

        if ( !logLevel.isVisible( channelLogLevel ) ) {
          return null; // not for our eyes.
        }
        if ( channelLogLevel.getLevel() >= logLevel.getLevel() ) {
          LogMessageInterface logMessage = (LogMessageInterface) args[0];
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

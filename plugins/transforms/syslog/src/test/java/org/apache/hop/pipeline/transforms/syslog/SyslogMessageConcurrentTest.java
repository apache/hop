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

package org.apache.hop.pipeline.transforms.syslog;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyslogMessageConcurrentTest {

  AtomicInteger numOfErrors = null;
  CountDownLatch countDownLatch = null;
  private String testMessage = "message value";
  int numOfTasks = 5;
  private TransformMockHelper<SyslogMessageMeta, SyslogMessageData> transformMockHelper;

  @Before
  public void setUp() throws Exception {
    numOfErrors = new AtomicInteger( 0 );
    countDownLatch = new CountDownLatch( 1 );
    transformMockHelper = new TransformMockHelper<>( "SYSLOG_MESSAGE TEST", SyslogMessageMeta.class,
      SyslogMessageData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.iLogChannel );
    when( transformMockHelper.iTransformMeta.getServerName() ).thenReturn( "localhost" );
    when( transformMockHelper.iTransformMeta.getMessageFieldName() ).thenReturn( "message field" );
    when( transformMockHelper.iTransformMeta.getPort() ).thenReturn( "9988" );
    when( transformMockHelper.iTransformMeta.getPriority() ).thenReturn( "ERROR" );
  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void concurrentSyslogMessageTest() throws Exception {
    SyslogMessageTask syslogMessage = null;
    ExecutorService service = Executors.newFixedThreadPool( numOfTasks );
    for ( int i = 0; i < numOfTasks; i++ ) {
      syslogMessage = createSyslogMessageTask();
      service.execute( syslogMessage );
    }
    service.shutdown();
    countDownLatch.countDown();
    service.awaitTermination( 10000, TimeUnit.NANOSECONDS );
    Assert.assertTrue( numOfErrors.get() == 0 );
  }


  private class SyslogMessageTask extends SyslogMessage implements Runnable {

    SyslogMessageMeta syslogMessageMeta = null;

    public SyslogMessageTask( TransformMeta transformMeta, SyslogMessageMeta meta, SyslogMessageData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline, SyslogMessageMeta processRowsITransform ) {
      super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
      syslogMessageMeta = processRowsITransform;
    }

    @Override
    public void run() {
      try {
        countDownLatch.await();
       init();
      } catch ( Exception e ) {
        e.printStackTrace();
        numOfErrors.getAndIncrement();
      } finally {
        try {
          dispose();
        } catch ( Exception e ) {
          e.printStackTrace();
          numOfErrors.getAndIncrement();
        }
      }
    }

    @Override
    public void putRow( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
      Assert.assertNotNull( row );
      Assert.assertTrue( row.length == 1 );
      Assert.assertEquals( testMessage, row[ 0 ] );
    }

    @Override
    public Object[] getRow() throws HopException {
      return new Object[] { testMessage };
    }
  }

  private SyslogMessageTask createSyslogMessageTask() throws Exception {
    SyslogMessageData data = new SyslogMessageData();
    SyslogMessageMeta meta = new SyslogMessageMeta();
    IRowMeta inputRowMeta = mock( IRowMeta.class );
    when( inputRowMeta.indexOfValue( any() ) ).thenReturn( 0 );
    when( inputRowMeta.getString( any(), eq( 0 ) ) ).thenReturn( testMessage );
    SyslogMessageTask syslogMessage = new SyslogMessageTask( transformMockHelper.transformMeta, meta, data, 0, transformMockHelper.pipelineMeta,
      transformMockHelper.pipeline, transformMockHelper.iTransformMeta );
    syslogMessage.init();
    syslogMessage.setInputRowMeta( inputRowMeta );
    return syslogMessage;
  }
}

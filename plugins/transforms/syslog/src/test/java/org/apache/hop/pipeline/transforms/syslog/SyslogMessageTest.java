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

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.productivity.java.syslog4j.SyslogConfigIF;
import org.productivity.java.syslog4j.SyslogIF;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * User: Dzmitry Stsiapanau Date: 1/23/14 Time: 11:04 AM
 */
public class SyslogMessageTest {

  private TransformMockHelper<SyslogMessageMeta, SyslogMessageData> transformMockHelper;

  @Before
  public void setUp() throws Exception {
    transformMockHelper =
      new TransformMockHelper<>( "SYSLOG_MESSAGE TEST", SyslogMessageMeta.class,
        SyslogMessageData.class );
    when( transformMockHelper.logChannelFactory.create( any(), any( ILoggingObject.class ) ) ).thenReturn(
      transformMockHelper.iLogChannel );

  }

  @After
  public void cleanUp() {
    transformMockHelper.cleanUp();
  }

  @Test
  public void testDispose() throws Exception {
    SyslogMessageData data = new SyslogMessageData();
    SyslogIF syslog = mock( SyslogIF.class );
    SyslogConfigIF syslogConfigIF = mock( SyslogConfigIF.class, RETURNS_MOCKS );
    when( syslog.getConfig() ).thenReturn( syslogConfigIF );
    final Boolean[] initialized = new Boolean[] { Boolean.FALSE };
    doAnswer( (Answer<Object>) invocation -> {
      initialized[ 0 ] = true;
      return initialized;
    } ).when( syslog ).initialize( anyString(), (SyslogConfigIF) anyObject() );
    doAnswer( (Answer<Object>) invocation -> {
      if ( !initialized[ 0 ] ) {
        throw new NullPointerException( "this.socket is null" );
      } else {
        initialized[ 0 ] = false;
      }
      return initialized;
    } ).when( syslog ).shutdown();
    SyslogMessageMeta meta = new SyslogMessageMeta();
    SyslogMessage syslogMessage =
      new SyslogMessage( transformMockHelper.transformMeta, transformMockHelper.iTransformMeta, transformMockHelper.iTransformData, 0, transformMockHelper.pipelineMeta,
        transformMockHelper.pipeline );
    SyslogMessage sysLogMessageSpy = spy( syslogMessage );
    when( sysLogMessageSpy.getSyslog() ).thenReturn( syslog );
    meta.setServerName( "1" );
    meta.setMessageFieldName( "1" );
    sysLogMessageSpy.init();
    sysLogMessageSpy.dispose();
  }
}

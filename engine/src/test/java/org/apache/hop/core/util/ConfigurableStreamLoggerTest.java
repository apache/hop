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

package org.apache.hop.core.util;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;


public class ConfigurableStreamLoggerTest {

  public static String INPUT = "str1\nstr2";
  public static String PREFIX = "OUTPUT";
  public static String OUT1 = "OUTPUT str1";
  public static String OUT2 = "OUTPUT str2";


  private ConfigurableStreamLogger streamLogger;
  private ILogChannel log;
  private InputStream is;

  @Before
  public void init() throws Exception {
    log = Mockito.mock( LogChannel.class );
    is = new ByteArrayInputStream( INPUT.getBytes( "UTF-8" ) );
  }


  @Test
  public void testLogError() {
    streamLogger = new ConfigurableStreamLogger( log, is, LogLevel.ERROR, PREFIX );
    streamLogger.run();

    Mockito.verify( log ).logError( OUT1 );
    Mockito.verify( log ).logError( OUT2 );
  }

  @Test
  public void testLogMinimal() {
    streamLogger = new ConfigurableStreamLogger( log, is, LogLevel.MINIMAL, PREFIX );
    streamLogger.run();

    Mockito.verify( log ).logMinimal( OUT1 );
    Mockito.verify( log ).logMinimal( OUT2 );
  }

  @Test
  public void testLogBasic() {
    streamLogger = new ConfigurableStreamLogger( log, is, LogLevel.BASIC, PREFIX );
    streamLogger.run();

    Mockito.verify( log ).logBasic( OUT1 );
    Mockito.verify( log ).logBasic( OUT2 );
  }

  @Test
  public void testLogDetailed() {
    streamLogger = new ConfigurableStreamLogger( log, is, LogLevel.DETAILED, PREFIX );
    streamLogger.run();

    Mockito.verify( log ).logDetailed( OUT1 );
    Mockito.verify( log ).logDetailed( OUT2 );
  }

  @Test
  public void testLogDebug() {
    streamLogger = new ConfigurableStreamLogger( log, is, LogLevel.DEBUG, PREFIX );
    streamLogger.run();

    Mockito.verify( log ).logDebug( OUT1 );
    Mockito.verify( log ).logDebug( OUT2 );
  }

  @Test
  public void testLogRowlevel() {
    streamLogger = new ConfigurableStreamLogger( log, is, LogLevel.ROWLEVEL, PREFIX );
    streamLogger.run();

    Mockito.verify( log ).logRowlevel( OUT1 );
    Mockito.verify( log ).logRowlevel( OUT2 );
  }

}

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

package org.apache.hop.mongo.wrapper;

import org.apache.hop.core.logging.ILogChannel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Created by matt on 10/30/15. */
public class HopMongoUtilLoggerTest {

  @Mock ILogChannel logChannelInterface;
  @Mock Exception exception;
  HopMongoUtilLogger logger;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    logger = new HopMongoUtilLogger(logChannelInterface);
  }

  @Test
  public void testLoggingDelegates() throws Exception {
    logger.debug("log");
    verify(logChannelInterface).logDebug("log");
    logger.info("log");
    verify(logChannelInterface).logBasic("log");
    logger.warn("log", exception);
    logger.error("log", exception);
    // both warn and error are mapped to logError.
    verify(logChannelInterface, times(2)).logError("log", exception);
    logger.isDebugEnabled();
    verify(logChannelInterface).isDebug();
  }
}

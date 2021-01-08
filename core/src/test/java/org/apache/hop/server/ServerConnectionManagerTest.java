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

package org.apache.hop.server;

import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ServerConnectionManagerTest {

  private SSLContext defaultContext;

  @Before
  public void setUp() throws Exception {
    ServerConnectionManager.reset();
    defaultContext = SSLContext.getDefault();
  }

  @Test
  public void shouldOverrideDefaultSSLContextByDefault() throws Exception {
    System.clearProperty( "javax.net.ssl.keyStore" );
    ServerConnectionManager instance = ServerConnectionManager.getInstance();
    assertNotEquals( defaultContext, SSLContext.getDefault() );
  }

  @Test
  public void shouldNotOverrideDefaultSSLContextIfKeystoreIsSet() throws Exception {
    System.setProperty( "javax.net.ssl.keyStore", "NONE" );
    ServerConnectionManager instance = ServerConnectionManager.getInstance();
    assertEquals( defaultContext, SSLContext.getDefault() );
  }
}

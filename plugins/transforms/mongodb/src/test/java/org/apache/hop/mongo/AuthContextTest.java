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

package org.apache.hop.mongo;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AuthContextTest {

  @Test
  public void testDoPrivalegedActionAsCurrentUser() throws Exception {
    PrivilegedAction<Object> privAction = mock(PrivilegedAction.class);
    AuthContext authContext = new AuthContext(null);
    authContext.doAs(privAction);
    verify(privAction).run();
  }

  @Test
  public void testDoPrivalegedActionExceptionAsCurrentUser() throws Exception {
    PrivilegedExceptionAction<Object> privExcAction = mock(PrivilegedExceptionAction.class);
    AuthContext authContext = new AuthContext(null);
    authContext.doAs(privExcAction);
    verify(privExcAction).run();
  }

  @Test
  public void testDoPrivalegedActionExceptionThrowsAsCurrentUser() throws Exception {
    PrivilegedExceptionAction<Object> privExcAction = mock(PrivilegedExceptionAction.class);
    Exception mock = mock(RuntimeException.class);
    doThrow(mock).when(privExcAction).run();
    AuthContext authContext = new AuthContext(null);
    try {
      authContext.doAs(privExcAction);
      fail();
    } catch (Exception e) {
      assertThat(e, CoreMatchers.instanceOf(PrivilegedActionException.class));
    }
    verify(privExcAction).run();
  }
}

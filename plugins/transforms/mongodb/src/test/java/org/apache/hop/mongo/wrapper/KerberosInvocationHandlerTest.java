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

import org.apache.hop.mongo.AuthContext;
import org.apache.hop.mongo.MongoDbException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KerberosInvocationHandlerTest {
  @SuppressWarnings("unchecked")
  @Test
  public void testInvocationHandlerCallsDoAsWhichCallsDelegate()
      throws MongoDbException, PrivilegedActionException {
    final MongoClientWrapper wrapper = mock(MongoClientWrapper.class);
    AuthContext authContext = mock(AuthContext.class);
    MongoClientWrapper wrappedWrapper =
        KerberosInvocationHandler.wrap(MongoClientWrapper.class, authContext, wrapper);
    when(authContext.doAs(any(PrivilegedExceptionAction.class)))
        .thenAnswer(
            new Answer<Void>() {

              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                wrapper.dispose();
                return null;
              }
            });
    wrappedWrapper.dispose();
    verify(authContext, times(1)).doAs(any(PrivilegedExceptionAction.class));
    verify(wrapper, times(1)).dispose();
  }
}

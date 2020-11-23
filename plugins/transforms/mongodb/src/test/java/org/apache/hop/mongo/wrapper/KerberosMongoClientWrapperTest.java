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

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.hop.mongo.AuthContext;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoUtilLogger;
import org.apache.hop.mongo.wrapper.collection.MongoCollectionWrapper;
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

public class KerberosMongoClientWrapperTest {
  @SuppressWarnings("unchecked")
  @Test
  public void testWrapProperlyWrapsCollection() throws MongoDbException, PrivilegedActionException {
    MongoClient client = mock(MongoClient.class);
    AuthContext authContext = mock(AuthContext.class);
    MongoUtilLogger log = mock(MongoUtilLogger.class);
    final DBCollection dbCollection = mock(DBCollection.class);
    String username = "test";
    final KerberosMongoClientWrapper wrapper =
        new KerberosMongoClientWrapper(client, log, username, authContext);
    MongoCollectionWrapper mongoCollectionWrapper = wrapper.wrap(dbCollection);
    when(authContext.doAs(any(PrivilegedExceptionAction.class)))
        .thenAnswer(
            new Answer<Void>() {

              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                dbCollection.drop();
                return null;
              }
            });
    mongoCollectionWrapper.drop();
    verify(authContext, times(1)).doAs(any(PrivilegedExceptionAction.class));
    verify(dbCollection, times(1)).drop();
  }
}

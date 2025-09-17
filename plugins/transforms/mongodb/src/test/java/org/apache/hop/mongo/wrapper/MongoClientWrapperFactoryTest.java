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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MongoClientWrapperFactoryTest {

  @Mock DefaultMongoClientFactory mongoClientFactory;
  @Mock MongoUtilLogger logger;

  @BeforeEach
  void before() {
    MockitoAnnotations.openMocks(this);
    NoAuthMongoClientWrapper.clientFactory = mongoClientFactory;
  }

  @Test
  void testCreateMongoClientWrapper() throws Exception {
    MongoClientWrapper wrapper =
        MongoClientWrapperFactory.createMongoClientWrapper(
            new MongoProperties.Builder()
                .set(MongoProp.USERNAME, "user")
                .set(MongoProp.PASSWORD, "password")
                .set(MongoProp.DBNAME, "dbname")
                .build(),
            logger);
    assertTrue(wrapper instanceof UsernamePasswordMongoClientWrapper);

    wrapper =
        MongoClientWrapperFactory.createMongoClientWrapper(
            new MongoProperties.Builder().set(MongoProp.USE_KERBEROS, "false").build(), logger);
    assertTrue(wrapper instanceof NoAuthMongoClientWrapper);
  }
}

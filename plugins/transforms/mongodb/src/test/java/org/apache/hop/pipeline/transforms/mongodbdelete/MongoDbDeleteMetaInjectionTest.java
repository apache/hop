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
package org.apache.hop.pipeline.transforms.mongodbdelete;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMetaInjectionTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MongoDbDeleteMetaInjectionTest extends BaseMetadataInjectionTestJunit5<MongoDbDeleteMeta> {
  private ILogChannelFactory oldLogChannelInterfaceFactory;

  @BeforeEach
  void setup() throws Exception {
    oldLogChannelInterfaceFactory = HopLogStore.getLogChannelFactory();
    MongoDbOutputMetaInjectionTest.setHopLogFactoryWithMock();
    setup(new MongoDbDeleteMeta());
  }

  @AfterEach
  void tearDown() {
    HopLogStore.setLogChannelFactory(oldLogChannelInterfaceFactory);
  }

  @Test
  void test() throws Exception {
    check("CONNECTION", () -> meta.getConnectionName());
    check("COLLECTION", () -> meta.getCollection());
    check("RETRIES", () -> meta.nbRetries);
    check("WRITE_RETRIES", () -> meta.getWriteRetries());
    check("RETRY_DELAY", () -> meta.getWriteRetryDelay());
    check("USE_JSON_QUERY", () -> meta.isUseJsonQuery());
    check("JSON_QUERY", () -> meta.getJsonQuery());
    check("EXECUTE_FOR_EACH_ROW", () -> meta.isExecuteForEachIncomingRow());
    check("INCOMING_FIELD_1", () -> meta.getMongoFields().get(0).incomingField1);
    check("INCOMING_FIELD_2", () -> meta.getMongoFields().get(0).incomingField2);
    check("DOC_PATH", () -> meta.getMongoFields().get(0).mongoDocPath);
    check("COMPARATOR", () -> meta.getMongoFields().get(0).comparator);
  }
}

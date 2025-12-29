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
package org.apache.hop.pipeline.transforms.mongodboutput;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** MDI test for MongoDbOutput. */
public class MongoDbOutputMetaInjectionTest
    extends BaseMetadataInjectionTestJunit5<MongoDbOutputMeta> {
  private ILogChannelFactory oldLogChannelInterfaceFactory;

  @BeforeEach
  void setup() throws Exception {
    oldLogChannelInterfaceFactory = HopLogStore.getLogChannelFactory();
    setHopLogFactoryWithMock();
    setup(new MongoDbOutputMeta());
  }

  public static void setHopLogFactoryWithMock() {
    ILogChannelFactory logChannelInterfaceFactory = mock(ILogChannelFactory.class);
    ILogChannel logChannelInterface = mock(ILogChannel.class);
    when(logChannelInterfaceFactory.create(any())).thenReturn(logChannelInterface);
    HopLogStore.setLogChannelFactory(logChannelInterfaceFactory);
  }

  @AfterEach
  void tearDown() {
    HopLogStore.setLogChannelFactory(oldLogChannelInterfaceFactory);
  }

  @Test
  void test() throws Exception {
    check("TRUNCATE", (IBooleanGetter) () -> meta.truncate);
    check("UPDATE", (IBooleanGetter) () -> meta.update);
    check("UPSERT", (IBooleanGetter) () -> meta.upsert);
    check("MULTI", (IBooleanGetter) () -> meta.multi);
    check("MODIFIER_UPDATE", (IBooleanGetter) () -> meta.modifierUpdate);
    check("BATCH_INSERT_SIZE", (IStringGetter) () -> meta.batchInsertSize);
    check("RETRY_NUMBER", (IStringGetter) () -> meta.getWriteRetries());
    check("RETRY_DELAY", (IStringGetter) () -> meta.getWriteRetryDelay());
    check("CONNECTION", (IStringGetter) () -> meta.getConnectionName());
    check("COLLECTION", (IStringGetter) () -> meta.getCollection());
    check(
        "INCOMING_FIELD_NAME",
        (IStringGetter) () -> meta.getMongoFields().get(0).incomingFieldName);
    check("MONGO_DOCUMENT_PATH", (IStringGetter) () -> meta.getMongoFields().get(0).mongoDocPath);
    check(
        "INCOMING_AS_MONGO",
        (IBooleanGetter) () -> meta.getMongoFields().get(0).useIncomingFieldNameAsMongoFieldName);
    check(
        "UPDATE_MATCH_FIELD", (IBooleanGetter) () -> meta.getMongoFields().get(0).updateMatchField);
    check(
        "MODIFIER_OPERATION",
        (IStringGetter) () -> meta.getMongoFields().get(0).modifierUpdateOperation);
    check(
        "MODIFIER_POLICY",
        (IStringGetter) () -> meta.getMongoFields().get(0).modifierOperationApplyPolicy);
    check("INSERT_NULL", (IBooleanGetter) () -> meta.getMongoFields().get(0).insertNull);
    check("JSON", (IBooleanGetter) () -> meta.getMongoFields().get(0).inputJson);
    check("INDEX_FIELD", (IStringGetter) () -> meta.getMongoIndexes().get(0).pathToFields);
    check("DROP", (IBooleanGetter) () -> meta.getMongoIndexes().get(0).drop);
    check("UNIQUE", (IBooleanGetter) () -> meta.getMongoIndexes().get(0).unique);
    check("SPARSE", (IBooleanGetter) () -> meta.getMongoIndexes().get(0).sparse);
  }
}

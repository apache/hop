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

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** MDI test for MongoDbOutput. */
public class MongoDbOutputMetaInjectionTest extends BaseMetadataInjectionTest<MongoDbOutputMeta> {
  private ILogChannelFactory oldLogChannelInterfaceFactory;

  @Before
  public void setup() throws Exception {
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

  @After
  public void tearDown() {
    HopLogStore.setLogChannelFactory(oldLogChannelInterfaceFactory);
  }

  @Test
  public void test() throws Exception {
    check(
        "TRUNCATE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.truncate;
          }
        });
    check(
        "UPDATE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.update;
          }
        });
    check(
        "UPSERT",
        new IBooleanGetter() {
          public boolean get() {
            return meta.upsert;
          }
        });
    check(
        "MULTI",
        new IBooleanGetter() {
          public boolean get() {
            return meta.multi;
          }
        });
    check(
        "MODIFIER_UPDATE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.modifierUpdate;
          }
        });
    check(
        "BATCH_INSERT_SIZE",
        new IStringGetter() {
          public String get() {
            return meta.batchInsertSize;
          }
        });
    check(
        "RETRY_NUMBER",
        new IStringGetter() {
          public String get() {
            return meta.getWriteRetries();
          }
        });
    check(
        "RETRY_DELAY",
        new IStringGetter() {
          public String get() {
            return meta.getWriteRetryDelay();
          }
        });
    check(
        "CONNECTION",
        new IStringGetter() {
          public String get() {
            return meta.getConnectionName();
          }
        });
    check(
        "COLLECTION",
        new IStringGetter() {
          public String get() {
            return meta.getCollection();
          }
        });
    check(
        "INCOMING_FIELD_NAME",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).incomingFieldName;
          }
        });
    check(
        "MONGO_DOCUMENT_PATH",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).mongoDocPath;
          }
        });
    check(
        "INCOMING_AS_MONGO",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoFields().get(0).useIncomingFieldNameAsMongoFieldName;
          }
        });
    check(
        "UPDATE_MATCH_FIELD",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoFields().get(0).updateMatchField;
          }
        });
    check(
        "MODIFIER_OPERATION",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).modifierUpdateOperation;
          }
        });
    check(
        "MODIFIER_POLICY",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).modifierOperationApplyPolicy;
          }
        });
    check(
        "INSERT_NULL",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoFields().get(0).insertNull;
          }
        });
    check(
        "JSON",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoFields().get(0).inputJson;
          }
        });
    check(
        "INDEX_FIELD",
        new IStringGetter() {
          public String get() {
            return meta.getMongoIndexes().get(0).pathToFields;
          }
        });
    check(
        "DROP",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoIndexes().get(0).drop;
          }
        });
    check(
        "UNIQUE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoIndexes().get(0).unique;
          }
        });
    check(
        "SPARSE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoIndexes().get(0).sparse;
          }
        });
  }
}

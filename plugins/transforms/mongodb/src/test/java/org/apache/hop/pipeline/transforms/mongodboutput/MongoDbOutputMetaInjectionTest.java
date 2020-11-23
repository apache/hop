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
            return meta.m_truncate;
          }
        });
    check(
        "UPDATE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.m_update;
          }
        });
    check(
        "UPSERT",
        new IBooleanGetter() {
          public boolean get() {
            return meta.m_upsert;
          }
        });
    check(
        "MULTI",
        new IBooleanGetter() {
          public boolean get() {
            return meta.m_multi;
          }
        });
    check(
        "MODIFIER_UPDATE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.m_modifierUpdate;
          }
        });
    check(
        "BATCH_INSERT_SIZE",
        new IStringGetter() {
          public String get() {
            return meta.m_batchInsertSize;
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
        "HOSTNAME",
        new IStringGetter() {
          public String get() {
            return meta.getHostnames();
          }
        });
    check(
        "PORT",
        new IStringGetter() {
          public String get() {
            return meta.getPort();
          }
        });
    check(
        "DATABASE_NAME",
        new IStringGetter() {
          public String get() {
            return meta.getDbName();
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
        "AUTH_DATABASE",
        new IStringGetter() {
          public String get() {
            return meta.getAuthenticationDatabaseName();
          }
        });
    check(
        "AUTH_USERNAME",
        new IStringGetter() {
          public String get() {
            return meta.getAuthenticationUser();
          }
        });
    check(
        "AUTH_PASSWORD",
        new IStringGetter() {
          public String get() {
            return meta.getAuthenticationPassword();
          }
        });
    check(
        "AUTH_MECHANISM",
        new IStringGetter() {
          public String get() {
            return meta.getAuthenticationMechanism();
          }
        });
    check(
        "AUTH_KERBEROS",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getUseKerberosAuthentication();
          }
        });
    check(
        "TIMEOUT_CONNECTION",
        new IStringGetter() {
          public String get() {
            return meta.getConnectTimeout();
          }
        });
    check(
        "TIMEOUT_SOCKET",
        new IStringGetter() {
          public String get() {
            return meta.getSocketTimeout();
          }
        });
    check(
        "USE_SSL_SOCKET_FACTORY",
        new IBooleanGetter() {
          public boolean get() {
            return meta.isUseSSLSocketFactory();
          }
        });
    check(
        "READ_PREFERENCE",
        new IStringGetter() {
          public String get() {
            return meta.getReadPreference();
          }
        });
    check(
        "USE_ALL_REPLICA_SET_MEMBERS",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getUseAllReplicaSetMembers();
          }
        });
    check(
        "INCOMING_FIELD_NAME",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).m_incomingFieldName;
          }
        });
    check(
        "MONGO_DOCUMENT_PATH",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).m_mongoDocPath;
          }
        });
    check(
        "INCOMING_AS_MONGO",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoFields().get(0).m_useIncomingFieldNameAsMongoFieldName;
          }
        });
    check(
        "UPDATE_MATCH_FIELD",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoFields().get(0).m_updateMatchField;
          }
        });
    check(
        "MODIFIER_OPERATION",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).m_modifierUpdateOperation;
          }
        });
    check(
        "MODIFIER_POLICY",
        new IStringGetter() {
          public String get() {
            return meta.getMongoFields().get(0).m_modifierOperationApplyPolicy;
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
            return meta.getMongoFields().get(0).m_JSON;
          }
        });
    check(
        "INDEX_FIELD",
        new IStringGetter() {
          public String get() {
            return meta.getMongoIndexes().get(0).m_pathToFields;
          }
        });
    check(
        "DROP",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoIndexes().get(0).m_drop;
          }
        });
    check(
        "UNIQUE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoIndexes().get(0).m_unique;
          }
        });
    check(
        "SPARSE",
        new IBooleanGetter() {
          public boolean get() {
            return meta.getMongoIndexes().get(0).m_sparse;
          }
        });
    check(
        "TAG_SET",
        new IStringGetter() {
          public String get() {
            return meta.getReadPrefTagSets().get(0);
          }
        });
  }
}

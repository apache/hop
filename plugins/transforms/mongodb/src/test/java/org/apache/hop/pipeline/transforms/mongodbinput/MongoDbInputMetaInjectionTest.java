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
package org.apache.hop.pipeline.transforms.mongodbinput;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannelFactory;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMetaInjectionTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** MDI test for MongoDbInput. */
public class MongoDbInputMetaInjectionTest extends BaseMetadataInjectionTest<MongoDbInputMeta> {

  private ILogChannelFactory oldLogChannelInterfaceFactory;

  @Before
  public void setup() throws Exception {
    oldLogChannelInterfaceFactory = HopLogStore.getLogChannelFactory();
    MongoDbOutputMetaInjectionTest.setHopLogFactoryWithMock();
    setup(new MongoDbInputMeta());
  }

  @After
  public void tearDown() {
    HopLogStore.setLogChannelFactory(oldLogChannelInterfaceFactory);
  }

  @Test
  public void test() throws Exception {
    check("HOSTNAME", () -> meta.getHostnames());
    check("JSON_FIELD", () -> meta.getFieldsName());
    check("JSON_QUERY", () -> meta.getJsonQuery());
    check("PORT", () -> meta.getPort());
    check("DATABASE_NAME", () -> meta.getDbName());
    check("COLLECTION", () -> meta.getCollection());
    check("AUTH_DATABASE", () -> meta.getAuthenticationDatabaseName());
    check("AUTH_USERNAME", () -> meta.getAuthenticationUser());
    check("AUTH_PASSWORD", () -> meta.getAuthenticationPassword());
    check("AUTH_MECHANISM", () -> meta.getAuthenticationMechanism());
    check("AUTH_KERBEROS", () -> meta.getUseKerberosAuthentication());
    check("TIMEOUT_CONNECTION", () -> meta.getConnectTimeout());
    check("TIMEOUT_SOCKET", () -> meta.getSocketTimeout());
    check("USE_SSL_SOCKET_FACTORY", () -> meta.isUseSSLSocketFactory());
    check("READ_PREFERENCE", () -> meta.getReadPreference());
    check("USE_ALL_REPLICA_SET_MEMBERS", () -> meta.getUseAllReplicaSetMembers());
    check("TAG_SET", () -> meta.getReadPrefTagSets().get(0));
    check("JSON_OUTPUT_FIELD", () -> meta.getJsonFieldName());
    check("AGG_PIPELINE", () -> meta.isQueryIsPipeline());
    check("OUTPUT_JSON", () -> meta.isOutputJson());
    check("EXECUTE_FOR_EACH_ROW", () -> meta.getExecuteForEachIncomingRow());
    check("FIELD_NAME", () -> meta.getMongoFields().get(0).fieldName);
    check("FIELD_PATH", () -> meta.getMongoFields().get(0).fieldPath);
    check("FIELD_TYPE", () -> meta.getMongoFields().get(0).hopType);
    check("FIELD_INDEXED", () -> meta.getMongoFields().get(0).indexedValues.get(0));
    check("FIELD_ARRAY_INDEX", () -> meta.getMongoFields().get(0).arrayIndexInfo);
    check("FIELD_PERCENTAGE", () -> meta.getMongoFields().get(0).percentageOfSample);
    check("FIELD_DISPARATE_TYPES", () -> meta.getMongoFields().get(0).disparateTypes);
  }
}

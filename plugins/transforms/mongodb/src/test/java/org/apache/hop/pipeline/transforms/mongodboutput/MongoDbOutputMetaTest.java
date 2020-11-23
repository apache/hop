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

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMeta.MongoField;
import org.apache.hop.pipeline.transforms.mongodboutput.MongoDbOutputMeta.MongoIndex;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDbOutputMetaTest {
  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  public void testRoundTrips() throws HopException {
    List<String> commonFields =
        Arrays.asList(
            "mongo_host",
            "mongo_port",
            "use_all_replica_members",
            "mongo_user",
            "mongo_password",
            "auth_kerberos",
            "mongo_db",
            "mongo_collection",
            "batch_insert_size",
            "connect_timeout",
            "socket_timeout",
            "read_preference",
            "write_concern",
            "w_timeout",
            "journaled_writes",
            "truncate",
            "update",
            "upsert",
            "multi",
            "modifier_update",
            "write_retries",
            "write_retry_delay",
            "mongo_fields",
            "mongo_indexes");
    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("mongo_host", "getHostnames");
    getterMap.put("mongo_port", "getPort");
    getterMap.put("use_all_replica_members", "getUseAllReplicaSetMembers");
    getterMap.put("mongo_user", "getAuthenticationUser");
    getterMap.put("mongo_password", "getAuthenticationPassword");
    getterMap.put("auth_kerberos", "getUseKerberosAuthentication");
    getterMap.put("mongo_db", "getDbName");
    getterMap.put("mongo_collection", "getCollection");
    getterMap.put("journaled_writes", "getJournal");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("mongo_host", "setHostnames");
    setterMap.put("mongo_port", "setPort");
    setterMap.put("use_all_replica_members", "setUseAllReplicaSetMembers");
    setterMap.put("mongo_user", "setAuthenticationUser");
    setterMap.put("mongo_password", "setAuthenticationPassword");
    setterMap.put("auth_kerberos", "setUseKerberosAuthentication");
    setterMap.put("mongo_db", "setDbName");
    setterMap.put("mongo_collection", "setCollection");
    setterMap.put("batch_insert_size", "setBatchInsertSize");
    setterMap.put("journaled_writes", "setJournal");

    LoadSaveTester tester =
        new LoadSaveTester(MongoDbOutputMeta.class, commonFields, getterMap, setterMap);

    IFieldLoadSaveValidatorFactory validatorFactory = tester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, MongoField.class),
        new ListLoadSaveValidator<>(
            new ObjectValidator<>(
                validatorFactory,
                MongoField.class,
                Arrays.asList(
                    "m_incomingFieldName",
                    "m_mongoDocPath",
                    "m_useIncomingFieldNameAsMongoFieldName",
                    "m_updateMatchField",
                    "m_modifierUpdateOperation",
                    "m_modifierOperationApplyPolicy",
                    "m_JSON",
                    "insertNull"))));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, MongoIndex.class),
        new ListLoadSaveValidator<>(
            new ObjectValidator<>(
                validatorFactory,
                MongoIndex.class,
                Arrays.asList("m_pathToFields", "m_drop", "m_unique", "m_sparse"))));

    tester.testXmlRoundTrip();
  }
}

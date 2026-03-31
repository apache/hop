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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class MongoDbDeleteMetaTest {

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testRoundTrips() throws HopException {
    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("execute_for_each_row", "isExecuteForEachIncomingRow");
    getterMap.put("fields", "getMongoFields");

    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("execute_for_each_row", "setExecuteForEachIncomingRow");
    setterMap.put("fields", "setMongoFields");

    LoadSaveTester tester =
        new LoadSaveTester(
            MongoDbDeleteMeta.class,
            Arrays.asList(
                "collection",
                "connectionName",
                "nbRetries",
                "writeRetries",
                "writeRetryDelay",
                "useJsonQuery",
                "jsonQuery",
                "executeForEachIncomingRow",
                "mongoFields"),
            getterMap,
            setterMap);

    IFieldLoadSaveValidatorFactory validatorFactory = tester.getFieldLoadSaveValidatorFactory();
    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, MongoDbDeleteField.class),
        new ListLoadSaveValidator<>(
            new ObjectValidator<>(
                validatorFactory,
                MongoDbDeleteField.class,
                Arrays.asList("incomingField1", "incomingField2", "mongoDocPath", "comparator"))));

    tester.testXmlRoundTrip();
  }
}

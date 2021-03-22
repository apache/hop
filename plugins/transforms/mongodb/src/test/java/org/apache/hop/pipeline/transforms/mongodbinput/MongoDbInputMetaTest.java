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

import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.mongo.wrapper.field.MongoField;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MongoDbInputMetaTest {
  @BeforeClass
  public static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  public void testRoundTrips() throws HopException, SecurityException {
    Map<String, String> getterMap = new HashMap<>();
    getterMap.put("execute_for_each_row", "getExecuteForEachIncomingRow");
    getterMap.put("mongo_fields", "getMongoFields");
    getterMap.put("tag_sets", "getReadPrefTagSets");
    Map<String, String> setterMap = new HashMap<>();
    setterMap.put("execute_for_each_row", "setExecuteForEachIncomingRow");
    setterMap.put("mongo_fields", "setMongoFields");

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorTypeMap = new HashMap<>();

    LoadSaveTester tester =
        new LoadSaveTester(
            MongoDbInputMeta.class,
            Arrays.asList(
                "fields_name",
                "collection",
                "json_field_name",
                "json_query",
                "output_json",
                "query_is_pipeline",
                "execute_for_each_row",
                "mongo_fields"),
            getterMap,
            setterMap,
            fieldLoadSaveValidatorAttributeMap,
            fieldLoadSaveValidatorTypeMap);

    IFieldLoadSaveValidatorFactory validatorFactory = tester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, MongoField.class),
        new ListLoadSaveValidator<>(
            new ObjectValidator<>(
                validatorFactory,
                MongoField.class,
                Arrays.asList("fieldName", "fieldPath", "hopType", "indexedValues"))));

    tester.testXmlRoundTrip();
  }
}

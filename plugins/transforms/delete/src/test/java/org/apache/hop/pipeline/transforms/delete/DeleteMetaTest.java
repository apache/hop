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

package org.apache.hop.pipeline.transforms.delete;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeleteMetaTest implements IInitializer<ITransformMeta> {
  LoadSaveTester loadSaveTester;
  Class<DeleteMeta> testMetaClass = DeleteMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    PluginRegistry.init(false);
    List<String> attributes = Arrays.asList("commit", "connection", "lookup");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("commit", "getCommitSize");
            put("connection", "getDatabaseMeta");
            put("lookup", "getLookup");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
          {
            put("commit", "setCommitSize");
            put("connection", "setDatabaseMeta");
            put("lookup", "setLookup");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "key", new ListLoadSaveValidator<>(new DeleteKeyFieldInputFieldLoadSaveValidator(), 5));
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            testMetaClass,
            attributes,
            new ArrayList<>(),
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(DeleteLookupField.class),
        new ObjectValidator<DeleteLookupField>(
            validatorFactory,
            DeleteLookupField.class,
            Arrays.asList("schema", "table", "key"),
            new HashMap<String, String>() {
              {
                put("table", "getTableName");
                put("schema", "getSchemaName");
                put("key", "getFields");
              }
            },
            new HashMap<String, String>() {
              {
                put("table", "setTableName");
                put("schema", "setSchemaName");
                put("key", "setFields");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, DeleteKeyField.class),
        new ListLoadSaveValidator<DeleteKeyField>(new DeleteKeyFieldInputFieldLoadSaveValidator()));
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {
    if (someMeta instanceof DeleteMeta) {
      DeleteLookupField dlf = ((DeleteMeta) someMeta).getLookup();
      dlf.getFields().clear();
      dlf.getFields()
          .addAll(
              Arrays.asList(
                  new DeleteKeyField("StreamField1", "=", "1", null),
                  new DeleteKeyField("StreamField2", "<>", "20", null),
                  new DeleteKeyField("StreamField3", "BETWEEN", "1", "10"),
                  new DeleteKeyField("StreamField4", "<=", "3", null),
                  new DeleteKeyField("StreamField5", ">=", "40", null)));
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  private TransformMeta transformMeta;
  private Delete del;
  private DeleteData data;
  private DeleteMeta meta;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("delete1");

    meta = new DeleteMeta();
    data = new DeleteData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId(TransformPluginType.class, meta);

    transformMeta = new TransformMeta(deletePid, "delete", meta);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    Map<String, String> vars = new HashMap<>();
    vars.put("max.sz", "10");
    pipeline.setVariables(vars);

    pipelineMeta.addTransform(transformMeta);
    del = new Delete(transformMeta, meta, data, 1, pipelineMeta, pipeline);
  }

  @Test
  public void testCommitCountFixed() {
    meta.setCommitSize("100");
    assertTrue(meta.getCommitSize(del) == 100);
  }

  @Test
  public void testCommitCountVar() {
    meta.setCommitSize("${max.sz}");
    assertTrue(meta.getCommitSize(del) == 10);
  }

  @Test
  public void testCommitCountMissedVar() {
    meta.setCommitSize("missed-var");
    try {
      meta.getCommitSize(del);
      fail();
    } catch (Exception ex) {
    }
  }

  public class DeleteLookupKeyFieldInputFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<DeleteLookupField> {
    final Random rand = new Random();

    @Override
    public DeleteLookupField getTestObject() {

      DeleteLookupField field =
          new DeleteLookupField(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              new ArrayList<DeleteKeyField>());

      return field;
    }

    @Override
    public boolean validateTestObject(DeleteLookupField testObject, Object actual) {
      if (!(actual instanceof DeleteLookupField)) {
        return false;
      }
      DeleteLookupField another = (DeleteLookupField) actual;
      return new EqualsBuilder()
          .append(testObject.getSchemaName(), another.getSchemaName())
          .append(testObject.getTableName(), another.getTableName())
          .append(testObject.getFields(), another.getFields())
          .isEquals();
    }
  }

  public class DeleteKeyFieldInputFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<DeleteKeyField> {
    final Random rand = new Random();

    @Override
    public DeleteKeyField getTestObject() {

      DeleteKeyField field =
          new DeleteKeyField(
              UUID.randomUUID().toString(),
              "=",
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString());

      return field;
    }

    @Override
    public boolean validateTestObject(DeleteKeyField testObject, Object actual) {
      if (!(actual instanceof DeleteKeyField)) {
        return false;
      }
      DeleteKeyField another = (DeleteKeyField) actual;
      return new EqualsBuilder()
          .append(testObject.getKeyLookup(), another.getKeyLookup())
          .append(testObject.getKeyCondition(), another.getKeyCondition())
          .append(testObject.getKeyStream(), another.getKeyStream())
          .append(testObject.getKeyStream2(), another.getKeyStream2())
          .isEquals();
    }
  }
}

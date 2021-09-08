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

package org.apache.hop.pipeline.transforms.update;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
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
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UpdateMetaTest implements IInitializer<ITransformMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private TransformMeta transformMeta;
  private Update upd;
  private UpdateData ud;
  private UpdateMeta umi;
  LoadSaveTester loadSaveTester;
  Class<UpdateMeta> testMetaClass = UpdateMeta.class;
  private TransformMockHelper<UpdateMeta, UpdateData> mockHelper;

  @Before
  public void setUp() throws HopException {
    HopEnvironment.init();
    PluginRegistry.init(false);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("delete1");

    Map<String, String> vars = new HashMap<>();
    vars.put("max.sz", "10");

    umi = new UpdateMeta();
    ud = new UpdateData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId(TransformPluginType.class, umi);

    transformMeta = new TransformMeta(deletePid, "delete", umi);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setVariables(vars);
    pipelineMeta.addTransform(transformMeta);
    mockHelper = new TransformMockHelper<>("Update", UpdateMeta.class, UpdateData.class);
    Mockito.when(
            mockHelper.logChannelFactory.create(Mockito.any(), Mockito.any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);

    upd = new Update(transformMeta, umi, ud, 1, pipelineMeta, pipeline);

    List<String> attributes =
        Arrays.asList(
            "connection",
            "lookup",
            "commit",
            "error_ignored",
            "ignore_flag_field",
            "skip_lookup",
            "use_batch");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("connection", "getConnection");
            put("lookup", "getLookupField");
            put("commit", "getCommitSize");
            put("error_ignored", "isErrorIgnored");
            put("ignore_flag_field", "getIgnoreFlagField");
            put("skip_lookup", "isSkipLookup");
            put("use_batch", "isUseBatchUpdate");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
          {
            put("connection", "setConnection");
            put("lookup", "setLookupField");
            put("commit", "setCommitSize");
            put("error_ignored", "setErrorIgnored");
            put("ignore_flag_field", "setIgnoreFlagField");
            put("skip_lookup", "setSkipLookup");
            put("use_batch", "setUseBatchUpdate");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
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
            validatorFactory.getName(UpdateLookupField.class),
            new ObjectValidator<UpdateLookupField>(
                    validatorFactory,
                    UpdateLookupField.class,
                    Arrays.asList("schema", "table", "key", "value"),
                    new HashMap<String, String>() {
                      {
                        put("schema", "getSchemaName");
                        put("table", "getTableName");
                        put("key", "getLookupKeys");
                        put("value", "getUpdateFields");
                      }
                    },
                    new HashMap<String, String>() {
                      {
                        put("schema", "setSchemaName");
                        put("table", "setTableName");
                        put("key", "setLookupKeys");
                        put("value", "setUpdateFields");
                      }
                    }));

    validatorFactory.registerValidator(
            validatorFactory.getName(List.class, UpdateLookupField.class),
            new ListLoadSaveValidator<UpdateLookupField>(new UpdateLookupFieldLoadSaveValidator()));



    validatorFactory.registerValidator(
        validatorFactory.getName(UpdateKeyField.class),
        new ObjectValidator<UpdateKeyField>(
            validatorFactory,
            UpdateKeyField.class,
            Arrays.asList("name", "field", "condition", "name2"),
            new HashMap<String, String>() {
              {
                put("name", "getKeyStream");
                put("field", "getKeyLookup");
                put("condition", "getKeyCondition");
                put("name2", "getKeyStream2");
              }
            },
            new HashMap<String, String>() {
              {
                put("name", "setKeyStream");
                put("field", "setKeyLookup");
                put("condition", "setKeyCondition");
                put("name2", "setKeyStream2");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, UpdateKeyField.class),
        new ListLoadSaveValidator<UpdateKeyField>(new UpdateKeyFieldLoadSaveValidator()));

    validatorFactory.registerValidator(
        validatorFactory.getName(UpdateField.class),
        new ObjectValidator<UpdateField>(
            validatorFactory,
            UpdateField.class,
            Arrays.asList("name", "rename"),
            new HashMap<String, String>() {
              {
                put("name", "getUpdateLookup");
                put("rename", "getUpdateStream");
              }
            },
            new HashMap<String, String>() {
              {
                put("name", "setUpdateLookup");
                put("rename", "setUpdateStream");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, UpdateField.class),
        new ListLoadSaveValidator<UpdateField>(new UpdateFieldLoadSaveValidator()));
  }

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void testCommitCountFixed() {
    umi.setCommitSize("100");
    assertTrue(umi.getCommitSize(upd) == 100);
  }

  @Test
  public void testCommitCountVar() {
    umi.setCommitSize("${max.sz}");
    assertTrue(umi.getCommitSize(upd) == 10);
  }

  @Test
  public void testCommitCountMissedVar() {
    umi.setCommitSize("missed-var");
    try {
      umi.getCommitSize(upd);
      fail();
    } catch (Exception ex) {
    }
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ITransformMeta someMeta) {

    if (someMeta instanceof UpdateMeta) {
      UpdateLookupField dlf = ((UpdateMeta) someMeta).getLookupField();
      dlf.getLookupKeys().clear();
      dlf.getLookupKeys()
          .addAll(
              Arrays.asList(
                  new UpdateKeyField("StreamField1", "=", "1", null),
                  new UpdateKeyField("StreamField2", "<>", "20", null),
                  new UpdateKeyField("StreamField3", "BETWEEN", "1", "10"),
                  new UpdateKeyField("StreamField4", "<=", "3", null),
                  new UpdateKeyField("StreamField5", ">=", "40", null)));
      dlf.getUpdateFields()
          .addAll(
              Arrays.asList(
                  new UpdateField("TableField1", "StreamField1"),
                  new UpdateField("TableField2", "StreamField2"),
                  new UpdateField("TableField3", "StreamField3"),
                  new UpdateField("TableField4", "StreamField4"),
                  new UpdateField("TableField5", "StreamField5")));
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }


  public class UpdateLookupFieldLoadSaveValidator
          implements IFieldLoadSaveValidator<UpdateLookupField> {
    final Random rand = new Random();

    @Override
    public UpdateLookupField getTestObject() {

      UpdateLookupField field =
              new UpdateLookupField(
                      UUID.randomUUID().toString(),
                      UUID.randomUUID().toString(),
                      new ArrayList<UpdateKeyField>(),
                      new ArrayList<UpdateField>());

      return field;
    }

    @Override
    public boolean validateTestObject(UpdateLookupField testObject, Object actual) {
      if (!(actual instanceof UpdateLookupField)) {
        return false;
      }
      UpdateLookupField another = (UpdateLookupField) actual;
      return new EqualsBuilder()
          .append(testObject.getSchemaName(), another.getSchemaName())
          .append(testObject.getTableName(), another.getTableName())
          .append(testObject.getLookupKeys(), another.getLookupKeys())
          .append(testObject.getUpdateFields(), another.getUpdateFields())
          .isEquals();
    }
  }

  public class UpdateFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<UpdateField> {
    final Random rand = new Random();

    @Override
    public UpdateField getTestObject() {

      UpdateField field =
          new UpdateField(UUID.randomUUID().toString(), UUID.randomUUID().toString());

      return field;
    }

    @Override
    public boolean validateTestObject(UpdateField testObject, Object actual) {
      if (!(actual instanceof UpdateField)) {
        return false;
      }
      UpdateField another = (UpdateField) actual;
      return new EqualsBuilder()
          .append(testObject.getUpdateLookup(), another.getUpdateLookup())
          .append(testObject.getUpdateStream(), another.getUpdateStream())
          .isEquals();
    }
  }

  public class UpdateKeyFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<UpdateKeyField> {
    final Random rand = new Random();

    @Override
    public UpdateKeyField getTestObject() {

      UpdateKeyField field =
          new UpdateKeyField(
              UUID.randomUUID().toString(),
              "=",
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString());

      return field;
    }

    @Override
    public boolean validateTestObject(UpdateKeyField testObject, Object actual) {
      if (!(actual instanceof UpdateKeyField)) {
        return false;
      }
      UpdateKeyField another = (UpdateKeyField) actual;
      return new EqualsBuilder()
          .append(testObject.getKeyLookup(), another.getKeyLookup())
          .append(testObject.getKeyCondition(), another.getKeyCondition())
          .append(testObject.getKeyStream(), another.getKeyStream())
          .append(testObject.getKeyStream2(), another.getKeyStream2())
          .isEquals();
    }
  }
}

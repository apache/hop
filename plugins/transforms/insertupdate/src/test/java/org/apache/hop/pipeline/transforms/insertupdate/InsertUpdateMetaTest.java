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

package org.apache.hop.pipeline.transforms.insertupdate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class InsertUpdateMetaTest {
  LoadSaveTester loadSaveTester;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private IVariables variables;
  private TransformMeta transformMeta;
  private InsertUpdate upd;
  private InsertUpdateData ud;
  private InsertUpdateMeta umi;
  private TransformMockHelper<InsertUpdateMeta, InsertUpdateData> mockHelper;

  @BeforeAll
  static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    variables = new Variables();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("delete1");

    umi = new InsertUpdateMeta();
    ud = new InsertUpdateData();

    PluginRegistry plugReg = PluginRegistry.getInstance();
    String deletePid = plugReg.getPluginId(TransformPluginType.class, umi);

    transformMeta = new TransformMeta(deletePid, "delete", umi);
    Pipeline pipeline = new LocalPipelineEngine(pipelineMeta);

    Map<String, String> vars = new HashMap<>();
    vars.put("max.sz", "10");
    pipeline.setVariables(vars);

    pipelineMeta.addTransform(transformMeta);
    upd = new InsertUpdate(transformMeta, umi, ud, 1, pipelineMeta, pipeline);

    mockHelper =
        new TransformMockHelper<>("insertUpdate", InsertUpdateMeta.class, InsertUpdateData.class);
    Mockito.when(
            mockHelper.logChannelFactory.create(Mockito.any(), Mockito.any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    Mockito.when(mockHelper.transformMeta.getTransform()).thenReturn(new InsertUpdateMeta());
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  void testCommitCountFixed() {
    umi.setCommitSize("100");
    assertEquals(100, umi.getCommitSizeVar(upd));
  }

  @Test
  void testCommitCountVar() {
    umi.setCommitSize("${max.sz}");
    assertEquals(10, umi.getCommitSizeVar(upd));
  }

  @Test
  void testCommitCountMissedVar() {
    umi.setCommitSize("missed-var");
    try {
      umi.getCommitSizeVar(upd);
      fail();
    } catch (Exception ex) {
    }
  }

  @BeforeEach
  void setUpLoadSave() throws Exception {

    List<String> attributes = Arrays.asList("connection", "lookup", "commit", "update_bypassed");

    Map<String, String> getterMap =
        new HashMap<>() {
          {
            put("connection", "getConnection");
            put("lookup", "getInsertUpdateLookupField");
            put("commit", "getCommitSize");
            put("update_bypassed", "isUpdateBypassed");
          }
        };
    Map<String, String> setterMap =
        new HashMap<>() {
          {
            put("connection", "setConnection");
            put("lookup", "setInsertUpdateLookupField");
            put("commit", "setCommitSize");
            put("update_bypassed", "setUpdateBypassed");
          }
        };

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester(
            InsertUpdateMeta.class,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator(
        validatorFactory.getName(InsertUpdateLookupField.class),
        new ObjectValidator<>(
            validatorFactory,
            InsertUpdateLookupField.class,
            Arrays.asList("schema", "table", "key", "value"),
            new HashMap<>() {
              {
                put("schema", "getSchemaName");
                put("table", "getTableName");
                put("key", "getLookupKeys");
                put("value", "getValueFields");
              }
            },
            new HashMap<>() {
              {
                put("schema", "setSchemaName");
                put("table", "setTableName");
                put("key", "setLookupKeys");
                put("value", "setValueFields");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, InsertUpdateLookupField.class),
        new ListLoadSaveValidator<>(new InsertUpdateLookupFieldLoadSaveValidator()));

    validatorFactory.registerValidator(
        validatorFactory.getName(InsertUpdateKeyField.class),
        new ObjectValidator<>(
            validatorFactory,
            InsertUpdateKeyField.class,
            Arrays.asList("name", "field", "condition", "name2"),
            new HashMap<>() {
              {
                put("name", "getKeyStream");
                put("field", "getKeyLookup");
                put("condition", "getKeyCondition");
                put("name2", "getKeyStream2");
              }
            },
            new HashMap<>() {
              {
                put("name", "setKeyStream");
                put("field", "setKeyLookup");
                put("condition", "setKeyCondition");
                put("name2", "setKeyStream2");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, InsertUpdateKeyField.class),
        new ListLoadSaveValidator<>(new InsertUpdateKeyFieldLoadSaveValidator()));

    validatorFactory.registerValidator(
        validatorFactory.getName(InsertUpdateValue.class),
        new ObjectValidator<>(
            validatorFactory,
            InsertUpdateValue.class,
            Arrays.asList("name", "rename"),
            new HashMap<>() {
              {
                put("name", "getUpdateLookup");
                put("rename", "getUpdateStream");
              }
            },
            new HashMap<>() {
              {
                put("name", "setUpdateLookup");
                put("rename", "setUpdateStream");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, InsertUpdateValue.class),
        new ListLoadSaveValidator<>(new InsertUpdateValueLoadSaveValidator()));
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  void testErrorProcessRow() throws HopException {
    Mockito.when(
            mockHelper.logChannelFactory.create(Mockito.any(), Mockito.any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    Mockito.when(mockHelper.transformMeta.getTransform()).thenReturn(new InsertUpdateMeta());

    InsertUpdate insertUpdateTransform =
        new InsertUpdate(
            mockHelper.transformMeta,
            mockHelper.iTransformMeta,
            mockHelper.iTransformData,
            0,
            mockHelper.pipelineMeta,
            mockHelper.pipeline);
    insertUpdateTransform = Mockito.spy(insertUpdateTransform);

    Mockito.doReturn(new Object[] {}).when(insertUpdateTransform).getRow();
    insertUpdateTransform.first = false;
    mockHelper.iTransformData.lookupParameterRowMeta = Mockito.mock(IRowMeta.class);
    mockHelper.iTransformData.keynrs = new int[] {};
    mockHelper.iTransformData.db = Mockito.mock(Database.class);
    mockHelper.iTransformData.valuenrs = new int[] {};
    Mockito.doThrow(new HopTransformException("Test exception"))
        .when(insertUpdateTransform)
        .putRow(Mockito.any(), Mockito.any());

    boolean result = insertUpdateTransform.processRow();
    assertFalse(result);
  }

  public class InsertUpdateLookupFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<InsertUpdateLookupField> {
    final Random rand = new Random();

    @Override
    public InsertUpdateLookupField getTestObject() {
      return new InsertUpdateLookupField(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          new ArrayList<>(),
          new ArrayList<>());
    }

    @Override
    public boolean validateTestObject(InsertUpdateLookupField testObject, Object actual) {
      if (!(actual instanceof InsertUpdateLookupField)) {
        return false;
      }
      InsertUpdateLookupField another = (InsertUpdateLookupField) actual;
      return new EqualsBuilder()
          .append(testObject.getSchemaName(), another.getSchemaName())
          .append(testObject.getTableName(), another.getTableName())
          .append(testObject.getLookupKeys(), another.getLookupKeys())
          .append(testObject.getValueFields(), another.getValueFields())
          .isEquals();
    }
  }

  public class InsertUpdateValueLoadSaveValidator
      implements IFieldLoadSaveValidator<InsertUpdateValue> {
    final Random rand = new Random();

    @Override
    public InsertUpdateValue getTestObject() {
      return new InsertUpdateValue(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(InsertUpdateValue testObject, Object actual) {
      if (!(actual instanceof InsertUpdateValue)) {
        return false;
      }
      InsertUpdateValue another = (InsertUpdateValue) actual;
      return new EqualsBuilder()
          .append(testObject.getUpdateLookup(), another.getUpdateLookup())
          .append(testObject.getUpdateStream(), another.getUpdateStream())
          .isEquals();
    }
  }

  public class InsertUpdateKeyFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<InsertUpdateKeyField> {
    final Random rand = new Random();

    @Override
    public InsertUpdateKeyField getTestObject() {
      return new InsertUpdateKeyField(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          "=",
          UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(InsertUpdateKeyField testObject, Object actual) {
      if (!(actual instanceof InsertUpdateKeyField)) {
        return false;
      }
      InsertUpdateKeyField another = (InsertUpdateKeyField) actual;
      return new EqualsBuilder()
          .append(testObject.getKeyStream(), another.getKeyStream())
          .append(testObject.getKeyLookup(), another.getKeyLookup())
          .append(testObject.getKeyCondition(), another.getKeyCondition())
          .append(testObject.getKeyStream2(), another.getKeyStream2())
          .isEquals();
    }
  }
}

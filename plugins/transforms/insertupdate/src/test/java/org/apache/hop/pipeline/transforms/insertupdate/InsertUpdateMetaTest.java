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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
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
import org.junit.*;
import org.mockito.Mockito;

import java.util.*;

public class InsertUpdateMetaTest {
  LoadSaveTester loadSaveTester;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private IVariables variables;
  private TransformMeta transformMeta;
  private InsertUpdate upd;
  private InsertUpdateData ud;
  private InsertUpdateMeta umi;
  private TransformMockHelper<InsertUpdateMeta, InsertUpdateData> mockHelper;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @Before
  public void setUp() {
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

  @After
  public void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  public void testCommitCountFixed() {
    umi.setCommitSize("100");
    Assert.assertTrue(umi.getCommitSizeVar(upd) == 100);
  }

  @Test
  public void testCommitCountVar() {
    umi.setCommitSize("${max.sz}");
    Assert.assertTrue(umi.getCommitSizeVar(upd) == 10);
  }

  @Test
  public void testProvidesModeler() throws Exception {
    InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();

    insertUpdateMeta
        .getInsertUpdateLookupField()
        .getValueFields()
        .add(new InsertUpdateValue("f1", "s4"));
    insertUpdateMeta
        .getInsertUpdateLookupField()
        .getValueFields()
        .add(new InsertUpdateValue("f2", "s5"));
    insertUpdateMeta
        .getInsertUpdateLookupField()
        .getValueFields()
        .add(new InsertUpdateValue("f3", "s6"));

    InsertUpdateData tableOutputData = new InsertUpdateData();
    tableOutputData.insertRowMeta = Mockito.mock(RowMeta.class);
    Assert.assertEquals(
        tableOutputData.insertRowMeta, insertUpdateMeta.getRowMeta(variables, tableOutputData));
    Assert.assertEquals(3, insertUpdateMeta.getDatabaseFields().size());
    Assert.assertEquals("f1", insertUpdateMeta.getDatabaseFields().get(0));
    Assert.assertEquals("f2", insertUpdateMeta.getDatabaseFields().get(1));
    Assert.assertEquals("f3", insertUpdateMeta.getDatabaseFields().get(2));
    Assert.assertEquals(3, insertUpdateMeta.getStreamFields().size());
    Assert.assertEquals("s4", insertUpdateMeta.getStreamFields().get(0));
    Assert.assertEquals("s5", insertUpdateMeta.getStreamFields().get(1));
    Assert.assertEquals("s6", insertUpdateMeta.getStreamFields().get(2));
  }

  @Test
  public void testCommitCountMissedVar() {
    umi.setCommitSize("missed-var");
    try {
      umi.getCommitSizeVar(upd);
      Assert.fail();
    } catch (Exception ex) {
    }
  }

  @Before
  public void setUpLoadSave() throws Exception {

    List<String> attributes = Arrays.asList("connection", "lookup", "commit", "update_bypassed");

    Map<String, String> getterMap =
        new HashMap<String, String>() {
          {
            put("connection", "getConnection");
            put("lookup", "getInsertUpdateLookupField");
            put("commit", "getCommitSize");
            put("update_bypassed", "isUpdateBypassed");
          }
        };
    Map<String, String> setterMap =
        new HashMap<String, String>() {
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
        new ObjectValidator<InsertUpdateLookupField>(
            validatorFactory,
            InsertUpdateLookupField.class,
            Arrays.asList("schema", "table", "key", "value"),
            new HashMap<String, String>() {
              {
                put("schema", "getSchemaName");
                put("table", "getTableName");
                put("key", "getLookupKeys");
                put("value", "getValueFields");
              }
            },
            new HashMap<String, String>() {
              {
                put("schema", "setSchemaName");
                put("table", "setTableName");
                put("key", "setLookupKeys");
                put("value", "setValueFields");
              }
            }));

    validatorFactory.registerValidator(
        validatorFactory.getName(List.class, InsertUpdateLookupField.class),
        new ListLoadSaveValidator<InsertUpdateLookupField>(
            new InsertUpdateLookupFieldLoadSaveValidator()));

    validatorFactory.registerValidator(
        validatorFactory.getName(InsertUpdateKeyField.class),
        new ObjectValidator<InsertUpdateKeyField>(
            validatorFactory,
            InsertUpdateKeyField.class,
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
        validatorFactory.getName(List.class, InsertUpdateKeyField.class),
        new ListLoadSaveValidator<InsertUpdateKeyField>(
            new InsertUpdateKeyFieldLoadSaveValidator()));

    validatorFactory.registerValidator(
        validatorFactory.getName(InsertUpdateValue.class),
        new ObjectValidator<InsertUpdateValue>(
            validatorFactory,
            InsertUpdateValue.class,
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
        validatorFactory.getName(List.class, InsertUpdateValue.class),
        new ListLoadSaveValidator<InsertUpdateValue>(new InsertUpdateValueLoadSaveValidator()));
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testErrorProcessRow() throws HopException {
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
    Assert.assertFalse(result);
  }

  public class InsertUpdateLookupFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<InsertUpdateLookupField> {
    final Random rand = new Random();

    @Override
    public InsertUpdateLookupField getTestObject() {

      InsertUpdateLookupField field =
          new InsertUpdateLookupField(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              new ArrayList<InsertUpdateKeyField>(),
              new ArrayList<InsertUpdateValue>());

      return field;
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

      InsertUpdateValue field =
          new InsertUpdateValue(UUID.randomUUID().toString(), UUID.randomUUID().toString());

      return field;
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

      InsertUpdateKeyField field =
          new InsertUpdateKeyField(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              "=",
              UUID.randomUUID().toString());

      return field;
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

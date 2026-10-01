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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.IInitializer;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ObjectValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ConstantMetaTest implements IInitializer<ConstantMeta> {
  LoadSaveTester<ConstantMeta> loadSaveTester;
  Class<ConstantMeta> testMetaClass = ConstantMeta.class;

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
    List<String> attributes = new ArrayList<>();

    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();

    Map<String, IFieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<>();
    attrValidatorMap.put(
        "fields", new ListLoadSaveValidator<>(new ConstantFieldLoadSaveValidator(), 5));

    Map<String, IFieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<>();

    loadSaveTester =
        new LoadSaveTester<>(
            testMetaClass,
            attributes,
            getterMap,
            setterMap,
            attrValidatorMap,
            typeValidatorMap,
            this);

    IFieldLoadSaveValidatorFactory validatorFactory =
        loadSaveTester.getFieldLoadSaveValidatorFactory();
    validatorFactory.registerValidator(
        validatorFactory.getName(ConstantField.class),
        new ObjectValidator<>(
            validatorFactory,
            ConstantField.class,
            Arrays.asList(
                "name",
                "type",
                "format",
                "length",
                "precision",
                "set_empty_string",
                "nullif",
                "group",
                "decimal",
                "currency"),
            new HashMap<>() {
              {
                put("name", "getFieldName");
                put("type", "getFieldType");
                put("format", "getFieldFormat");
                put("length", "getFieldLength");
                put("precision", "getFieldPrecision");
                put("set_empty_string", "isEmptyString");
                put("nullif", "getValue");
                put("group", "getGroup");
                put("decimal", "getDecimal");
                put("currency", "getCurrency");
              }
            },
            new HashMap<>() {
              {
                put("name", "setFieldName");
                put("type", "setFieldType");
                put("format", "setFieldFormat");
                put("length", "setFieldLength");
                put("precision", "setFieldPrecision");
                put("set_empty_string", "setEmptyString");
                put("nullif", "setValue");
                put("group", "setGroup");
                put("decimal", "setDecimal");
                put("currency", "setCurrency");
              }
            }));
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify(ConstantMeta someMeta) {
    if (someMeta instanceof ConstantMeta) {
      ((ConstantMeta) someMeta).getFields().clear();
      ((ConstantMeta) someMeta)
          .getFields()
          .addAll(
              Arrays.asList(
                  new ConstantField("InField1", "String", "Value1"),
                  new ConstantField("InField2", "String", "Value2"),
                  new ConstantField("InField3", "String", "Value3"),
                  new ConstantField("InField4", "String", "Value4"),
                  new ConstantField("InField5", "String", "Value5")));
    }
  }

  @Test
  void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  void testGetFieldsAddsAValueMetaPerField() throws Exception {
    ConstantMeta meta = new ConstantMeta();
    ConstantField field = new ConstantField("amount", "Number", "1");
    field.setFieldLength(9);
    field.setFieldPrecision(2);
    field.setFieldFormat("#.##");
    meta.getFields().add(field);

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "the transform", null, null, new Variables(), null);

    assertEquals(1, rowMeta.size());
    IValueMeta valueMeta = rowMeta.getValueMeta(0);
    assertEquals("amount", valueMeta.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, valueMeta.getType());
    assertEquals(9, valueMeta.getLength());
    assertEquals(2, valueMeta.getPrecision());
    assertEquals("#.##", valueMeta.getConversionMask());
    assertEquals("the transform", valueMeta.getOrigin());
  }

  /** Unnamed fields are placeholders in the dialog's grid and must not reach the output row. */
  @Test
  void testGetFieldsSkipsFieldsWithoutAName() throws Exception {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField(null, "String", "no name"));
    meta.getFields().add(new ConstantField("", "String", "empty name"));
    meta.getFields().add(new ConstantField("kept", "String", "value"));

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "the transform", null, null, new Variables(), null);

    assertEquals(1, rowMeta.size());
    assertEquals("kept", rowMeta.getValueMeta(0).getName());
  }

  /** A field whose type was never chosen still produces a column, typed as String. */
  @Test
  void testGetFieldsDefaultsAnUnsetTypeToString() throws Exception {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("untyped", "", "value"));

    IRowMeta rowMeta = new RowMeta();
    meta.getFields(rowMeta, "the transform", null, null, new Variables(), null);

    assertEquals(IValueMeta.TYPE_STRING, rowMeta.getValueMeta(0).getType());
  }

  @Test
  void testCheckReportsReceivedFields() {
    List<ICheckResult> remarks = new ArrayList<>();
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("incoming"));

    check(remarks, prev, new ConstantField("string", "String", "value"));

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_OK, remarks.get(0).getType());
  }

  @Test
  void testCheckReportsMissingInputFields() {
    List<ICheckResult> remarks = new ArrayList<>();

    check(remarks, new RowMeta(), new ConstantField("string", "String", "value"));

    assertEquals(1, remarks.size());
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(0).getType());
  }

  /** check() also surfaces the per-field problems that would otherwise only fail at runtime. */
  @Test
  void testCheckReportsUnbuildableFields() {
    List<ICheckResult> remarks = new ArrayList<>();
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("incoming"));

    check(remarks, prev, new ConstantField("integer", "Integer", "not a number"));

    assertEquals(2, remarks.size(), "the fields-received remark plus the unparsable field");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, remarks.get(1).getType());
  }

  @Test
  void testCloneCopiesTheFields() {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("string", "String", "value"));

    ConstantMeta clone = (ConstantMeta) meta.clone();

    assertNotSame(meta, clone);
    assertEquals(meta.getFields(), clone.getFields());
  }

  private static void check(List<ICheckResult> remarks, IRowMeta prev, ConstantField... fields) {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().addAll(Arrays.asList(fields));
    TransformMeta transformMeta = new TransformMeta("Constant", "constant", meta);
    meta.check(
        remarks, new PipelineMeta(), transformMeta, prev, null, null, null, new Variables(), null);
  }

  public class ConstantFieldLoadSaveValidator implements IFieldLoadSaveValidator<ConstantField> {
    final Random rand = new Random();

    @Override
    public ConstantField getTestObject() {
      return new ConstantField(
          UUID.randomUUID().toString(), "String", UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(ConstantField testObject, Object actual) {
      if (!(actual instanceof ConstantField)) {
        return false;
      }
      ConstantField another = (ConstantField) actual;
      return new EqualsBuilder()
          .append(testObject.getFieldName(), another.getFieldName())
          .append(testObject.getFieldType(), another.getFieldType())
          .append(testObject.getValue(), another.getValue())
          .isEquals();
    }
  }
}

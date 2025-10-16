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

package org.apache.hop.pipeline.transforms.salesforceinsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceMetaTest;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SalesforceInsertMetaTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @Test
  void testErrorHandling() {
    SalesforceTransformMeta meta = new SalesforceInsertMeta();
    assertTrue(meta.supportsErrorHandling());
  }

  @Test
  void testBatchSize() {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setBatchSize("20");
    assertEquals("20", meta.getBatchSize());
    assertEquals(20, meta.getBatchSizeInt());

    // Pass invalid batch size, should get default value of 10
    meta.setBatchSize("unknown");
    assertEquals("unknown", meta.getBatchSize());
    assertEquals(10, meta.getBatchSizeInt());
  }

  @Test
  void testCheck() {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setDefault();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, null, null, null, null, null, null, null);
    boolean hasError = false;
    for (ICheckResult cr : remarks) {
      if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        hasError = true;
      }
    }
    assertFalse(remarks.isEmpty());
    assertTrue(hasError);

    remarks.clear();
    meta.setDefault();
    meta.setUsername("user");
    List<SalesforceInsertField> fields = new ArrayList<>();
    fields.add(new SalesforceInsertField());
    //    SalesforceInsertField field = new SalesforceInsertField();
    //    field.setUpdateLookup("SalesforceField");
    //    field.setUpdateStream("StreamField");
    //    field.setUseExternalId(false);
    //    fields.add(field);
    meta.setFields(fields);
    //    meta.setUpdateLookup(new String[] {"SalesforceField"});
    //    meta.setUpdateStream(new String[] {"StreamField"});
    //    meta.setUseExternalId(new Boolean[] {false});
    meta.check(remarks, null, null, null, null, null, null, null, null);
    hasError = false;
    for (ICheckResult cr : remarks) {
      if (cr.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        hasError = true;
      }
    }
    assertFalse(remarks.isEmpty());
    assertFalse(hasError);
  }

  @Test
  void testGetFields() throws HopTransformException {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setDefault();
    IRowMeta r = new RowMeta();
    meta.getFields(r, "thisTransform", null, null, new Variables(), null);
    assertEquals(1, r.size());
    assertEquals("Id", r.getFieldNames()[0]);

    meta.setSalesforceIDFieldName("id_field");
    r.clear();
    meta.getFields(r, "thisTransform", null, null, new Variables(), null);
    assertEquals(1, r.size());
    assertEquals("id_field", r.getFieldNames()[0]);
  }

  @Test
  @Disabled
  void testSalesforceInsertMeta() throws Exception {
    List<String> attributes = new ArrayList<>();
    attributes.addAll(SalesforceMetaTest.getDefaultAttributes());
    attributes.addAll(
        Arrays.asList(
            "fields",
            "batchSize",
            "salesforceIDFieldName",
            //            "updateLookup",
            //            "updateStream",
            //            "useExternalId",
            "rollbackAllChangesOnError"));
    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidators = new HashMap<>();

    Class<SalesforceInsertMeta> testMetaClass = SalesforceInsertMeta.class;
    LoadSaveTester<SalesforceInsertMeta> tester =
        new LoadSaveTester<>(testMetaClass, attributes, getterMap, setterMap);

    IFieldLoadSaveValidatorFactory factory = tester.getFieldLoadSaveValidatorFactory();
    factory.registerValidator(
        SalesforceInsertMeta.class.getDeclaredField("fields").getGenericType().toString(),
        //        new SkipFieldsValidator());
        new ListLoadSaveValidator<>(
            new SalesforceInsertMetaTest.SalesforceInsertFieldLoadSaveValidator(), 3));

    tester.testSerialization();
    //    tester.testXmlRoundTrip();

    //    IFieldLoadSaveValidatorFactory factory =
    // tester.getFieldLoadSaveValidatorFactory();
    //    factory.registerValidator(
    //        SalesforceInsertMeta.class.getDeclaredField("fields").getGenericType().toString(),
    //        new IFieldLoadSaveValidator<List<SalesforceInsertField>>() {
    //          @Override
    //          public List<SalesforceInsertField> getTestObject() {
    //            // Return an empty list so we don't test fields
    //            return new ArrayList<>();
    //          }
    //
    //          @Override
    //          public boolean validateTestObject(List<SalesforceInsertField> testObject, Object
    // actual) {
    //            // Always return true to skip validation
    //            return true;
    //          }
    //        });
    //    IFieldLoadSaveValidatorFactory factory = tester.getFieldLoadSaveValidatorFactory();
    //    factory.registerValidator(
    //        SalesforceInsertMeta.class.getDeclaredField("fields").getGenericType().toString(),
    //        new ListLoadSaveValidator<>(
    //            new SalesforceInsertMetaTest.SalesforceInsertFieldLoadSaveValidator(), 3));

  }

  public static class SalesforceInsertFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<SalesforceInsertField> {
    static final Random rnd = new Random();

    @Override
    public SalesforceInsertField getTestObject() {
      SalesforceInsertField retval = new SalesforceInsertField();
      retval.setUseExternalId(rnd.nextBoolean());
      retval.setUpdateLookup(UUID.randomUUID().toString());
      retval.setUpdateStream(UUID.randomUUID().toString());
      return retval;
    }

    @Override
    public boolean validateTestObject(SalesforceInsertField testObject, Object actual)
        throws HopException {
      if (!(actual instanceof SalesforceInsertField)) {
        return false;
      }

      SalesforceInsertField sfActual = (SalesforceInsertField) actual;

      if (sfActual.isUseExternalId() != testObject.isUseExternalId()
          || !sfActual.getUpdateStream().equals(testObject.getUpdateStream())
          || !sfActual.getUpdateLookup().equals(testObject.getUpdateLookup())) {
        return false;
      }

      //      if (sfActual.isUseExternalId() != testObject.isUseExternalId()) {
      //        return false;
      //      }
      //
      //      String actualLookup = sfActual.getUpdateLookup();
      //      String testLookup = testObject.getUpdateLookup();
      //      if ((actualLookup == null) != (testLookup == null)
      //          || (actualLookup != null && !actualLookup.equals(testLookup))) {
      //        return false;
      //      }
      //
      //      String actualStream = sfActual.getUpdateStream();
      //      String testStream = testObject.getUpdateStream();
      //      if ((actualStream == null) != (testStream == null)
      //          || (actualStream != null && !actualStream.equals(testStream))) {
      //        return false;
      //      }

      // Null-safe comparison
      //      boolean actualUseExternalId = sfActual.isUseExternalId();
      //      boolean testUseExternalId = testObject.isUseExternalId();
      //
      //      if (!Objects.equals(actualUseExternalId, testUseExternalId)) {
      //        return false;
      //      }
      //      if (sfActual.isUseExternalId() != testObject.isUseExternalId()) {
      //        return false;
      //      }

      //      String actualLookup = sfActual.getUpdateLookup();
      //      String testLookup = testObject.getUpdateLookup();
      //      if ((actualLookup == null) != (testLookup == null)
      //          || (actualLookup != null && !actualLookup.equals(testLookup))) {
      //        return false;
      //      }

      //      String actualStream = sfActual.getUpdateStream();
      //      String testStream = testObject.getUpdateStream();
      //      if ((actualStream == null) != (testStream == null)
      //          || (actualStream != null && !actualStream.equals(testStream))) {
      //        return false;
      //      }

      //      if (sfActual.isUseExternalId() != testObject.isUseExternalId()
      //          || !sfActual.getUpdateLookup().equals(testObject.getUpdateLookup())
      //          || !sfActual.getUpdateStream().equals(testObject.getUpdateStream())) {
      //        return false;
      //      }
      return true;
    }
  }

  public class SkipFieldsValidator implements IFieldLoadSaveValidator<List<SalesforceInsertField>> {

    @Override
    public List<SalesforceInsertField> getTestObject() {
      // Return an empty list so we don't test fields
      return new ArrayList<>();
    }

    @Override
    public boolean validateTestObject(List<SalesforceInsertField> testObject, Object actual)
        throws HopException {
      // Always return true to skip validation
      return true;
    }
  }
}

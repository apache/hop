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
package org.apache.hop.pipeline.transforms.databasevaluevalidation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DatabaseValueValidationMetaTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void testDefaultAndClone() throws Exception {
    DatabaseValueValidationMeta meta = new DatabaseValueValidationMeta();
    meta.setDefault();
    assertTrue(meta.isFailIfRequiredColumnsUnmapped());
    assertFalse(meta.isOmitValues());
    assertEquals("; ", meta.getConcatenationSeparator());
    assertTrue(meta.supportsErrorHandling());

    meta.getFields().add(new DatabaseValueValidationField("col", "field"));
    DatabaseValueValidationMeta clone = (DatabaseValueValidationMeta) meta.clone();
    assertNotSame(clone, meta);
    assertEquals(clone.getXml(), meta.getXml());
    assertNotSame(clone.getFields(), meta.getFields());
  }

  @Test
  void testSerialization() throws Exception {
    LoadSaveTester<DatabaseValueValidationMeta> tester =
        new LoadSaveTester<>(DatabaseValueValidationMeta.class);
    IFieldLoadSaveValidatorFactory factory = tester.getFieldLoadSaveValidatorFactory();
    factory.registerValidator(
        DatabaseValueValidationMeta.class.getDeclaredField("fields").getGenericType().toString(),
        new ListLoadSaveValidator<>(new FieldValidator()));
    tester.testSerialization();
  }

  static final class FieldValidator
      implements IFieldLoadSaveValidator<DatabaseValueValidationField> {

    @Override
    public DatabaseValueValidationField getTestObject() {
      return new DatabaseValueValidationField(
          UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(DatabaseValueValidationField test, Object actual) {
      if (!(actual instanceof DatabaseValueValidationField)) {
        return false;
      }
      DatabaseValueValidationField another = (DatabaseValueValidationField) actual;
      return new EqualsBuilder()
          .append(test.getFieldStream(), another.getFieldStream())
          .append(test.getFieldDatabase(), another.getFieldDatabase())
          .isEquals();
    }
  }
}

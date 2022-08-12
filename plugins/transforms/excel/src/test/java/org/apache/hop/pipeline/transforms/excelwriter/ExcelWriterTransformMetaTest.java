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

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidatorFactory;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

public class ExcelWriterTransformMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testRoundTrip() throws Exception {

    LoadSaveTester<ExcelWriterTransformMeta> tester =
        new LoadSaveTester<>(ExcelWriterTransformMeta.class);

    IFieldLoadSaveValidatorFactory validatorFactory = tester.getFieldLoadSaveValidatorFactory();
    validatorFactory.registerValidator(
        ExcelWriterFileField.class.getName(),
        new ExcelWriterFileFieldValidator());
    validatorFactory.registerValidator(
            ExcelWriterTemplateField.class.getName(),
            new ExcelWriterTemplateFieldValidator());
    validatorFactory.registerValidator(
            ExcelWriterTransformMeta.class.getDeclaredField("outputFields").getGenericType().toString(),
            new ListLoadSaveValidator<>(new ExcelWriterOutputFieldValidator()));

    tester.testSerialization();
  }

  public static final class ExcelWriterOutputFieldValidator
      implements IFieldLoadSaveValidator<ExcelWriterOutputField> {

    @Override
    public ExcelWriterOutputField getTestObject() {
      return new ExcelWriterOutputField(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          new Random().nextBoolean(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(ExcelWriterOutputField testObject, Object actual) {
      return testObject.equalsAll(actual);
    }
  }

  public static final class ExcelWriterFileFieldValidator
      implements IFieldLoadSaveValidator<ExcelWriterFileField> {

    @Override
    public ExcelWriterFileField getTestObject() {
      return new ExcelWriterFileField(
          UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(ExcelWriterFileField testObject, Object actual) {
      if (!(actual instanceof ExcelWriterFileField)) {
        return false;
      }
      ExcelWriterFileField actualObject = (ExcelWriterFileField) actual;
      return testObject.getFileName().equals(actualObject.getFileName())
          && testObject.getExtension().equals(actualObject.getExtension())
          && testObject.getSheetname().equals(actualObject.getSheetname());
    }
  }

  public static final class ExcelWriterTemplateFieldValidator implements IFieldLoadSaveValidator<ExcelWriterTemplateField> {

    @Override
    public ExcelWriterTemplateField getTestObject() {
      return new ExcelWriterTemplateField(
              new Random().nextBoolean(),
              new Random().nextBoolean(),
              new Random().nextBoolean(),
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString()
      );
    }

    @Override
    public boolean validateTestObject(ExcelWriterTemplateField testObject, Object actual) {
      return testObject.equals(actual);
    }
  }
}

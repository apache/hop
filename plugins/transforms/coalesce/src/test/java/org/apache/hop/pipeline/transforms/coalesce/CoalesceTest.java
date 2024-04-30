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

package org.apache.hop.pipeline.transforms.coalesce;

import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class CoalesceTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testLoadSave() throws Exception {

    LoadSaveTester<CoalesceMeta> loadSaveTester = new LoadSaveTester<>(CoalesceMeta.class);

    loadSaveTester
        .getFieldLoadSaveValidatorFactory()
        .registerValidator(
            CoalesceMeta.class.getDeclaredField("fields").getGenericType().toString(),
            new ListLoadSaveValidator<>(new CoalesceFieldLoadSaveValidator()));

    loadSaveTester.testSerialization();
  }

  private final class CoalesceFieldLoadSaveValidator
      implements IFieldLoadSaveValidator<CoalesceField> {

    @Override
    public CoalesceField getTestObject() {
      return new CoalesceField(
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          new Random().nextBoolean(),
          UUID.randomUUID().toString(),
          Collections.emptyList());
    }

    @Override
    public boolean validateTestObject(CoalesceField testObject, Object actual) {
      if (!(actual instanceof CoalesceField)) {
        return false;
      }
      CoalesceField actualObject = (CoalesceField) actual;
      return testObject.getName().equals(actualObject.getName())
          && testObject.getType().equals(actualObject.getType())
          && testObject.isRemoveFields() == actualObject.isRemoveFields()
          && testObject.getInputFields().equals(actualObject.getInputFields());
    }
  }
}

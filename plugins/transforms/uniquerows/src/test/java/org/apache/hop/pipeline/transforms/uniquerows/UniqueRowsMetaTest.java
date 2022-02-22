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

package org.apache.hop.pipeline.transforms.uniquerows;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.validator.*;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

public class UniqueRowsMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Test
  public void testRoundTrip() throws HopException {

    Map<String, IFieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();

    // Arrays need to be consistent length
    IFieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
        new ArrayLoadSaveValidator<>(new StringLoadSaveValidator(), 25);
    IFieldLoadSaveValidator<boolean[]> booleanArrayLoadSaveValidator =
        new PrimitiveBooleanArrayLoadSaveValidator(new BooleanLoadSaveValidator(), 25);

    fieldLoadSaveValidatorAttributeMap.put("name", stringArrayLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put("case_insensitive", booleanArrayLoadSaveValidator);
    fieldLoadSaveValidatorAttributeMap.put(
        "compareFields", new ListLoadSaveValidator<>(new UniqueFieldLoadSaveTester()));

    LoadSaveTester loadSaveTester =
        new LoadSaveTester(
            UniqueRowsMeta.class,
            new ArrayList<>(),
            new HashMap<>(),
            new HashMap<>(),
            fieldLoadSaveValidatorAttributeMap,
            new HashMap<>());

    loadSaveTester.testSerialization();
  }

  private static final class UniqueFieldLoadSaveTester
      implements IFieldLoadSaveValidator<UniqueField> {

    @Override
    public UniqueField getTestObject() {
      return new UniqueField(UUID.randomUUID().toString(), new Random().nextBoolean());
    }

    @Override
    public boolean validateTestObject(UniqueField testObject, Object actual) {
      if (!(actual instanceof UniqueField)) {
        return false;
      }
      UniqueField actualObject = (UniqueField) actual;
      return testObject.getName().equals(actualObject.getName())
          && testObject.isCaseInsensitive() == actualObject.isCaseInsensitive();
    }
  }
}

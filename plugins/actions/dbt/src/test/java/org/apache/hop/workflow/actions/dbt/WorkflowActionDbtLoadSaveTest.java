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

package org.apache.hop.workflow.actions.dbt;

import java.util.UUID;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.jupiter.api.BeforeEach;

class WorkflowActionDbtLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionDbt> {

  @Override
  protected Class<ActionDbt> getActionClass() {
    return ActionDbt.class;
  }

  @BeforeEach
  void setup() throws Exception {
    for (String field : new String[] {"vars", "envVars"}) {
      tester
          .getFieldLoadSaveValidatorFactory()
          .registerValidator(
              getActionClass().getDeclaredField(field).getGenericType().toString(),
              new ListLoadSaveValidator<>(new DbtNameValueLoadSaveValidator()));
    }
  }

  private static class DbtNameValueLoadSaveValidator
      implements IFieldLoadSaveValidator<DbtNameValue> {
    @Override
    public DbtNameValue getTestObject() {
      return new DbtNameValue(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(DbtNameValue testObject, Object actual) {
      if (!(actual instanceof DbtNameValue actualObject)) {
        return false;
      }
      return testObject.getName().equals(actualObject.getName())
          && testObject.getValue().equals(actualObject.getValue());
    }
  }
}

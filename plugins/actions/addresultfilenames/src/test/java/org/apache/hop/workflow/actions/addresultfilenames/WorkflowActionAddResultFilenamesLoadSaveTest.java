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
 *
 */
package org.apache.hop.workflow.actions.addresultfilenames;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.ListLoadSaveValidator;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class WorkflowActionAddResultFilenamesLoadSaveTest
    extends WorkflowActionLoadSaveTestSupport<ActionAddResultFilenames> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionAddResultFilenames> getActionClass() {
    return ActionAddResultFilenames.class;
  }

  @Before
  public void setup() throws Exception {
    tester
        .getFieldLoadSaveValidatorFactory()
        .registerValidator(
            getActionClass().getDeclaredField("arguments").getGenericType().toString(),
            new ListLoadSaveValidator<>(new ArgumentLoadSaveValidator()));
  }

  private static class ArgumentLoadSaveValidator implements IFieldLoadSaveValidator<Argument> {
    @Override
    public Argument getTestObject() {
      return new Argument(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @Override
    public boolean validateTestObject(Argument testObject, Object actual) {
      if (!(actual instanceof Argument)) {
        return false;
      }
      Argument actualObject = (Argument) actual;
      return testObject.getArgument().equalsIgnoreCase(actualObject.getArgument())
          && testObject.getMask().equalsIgnoreCase(actualObject.getMask());
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.ui.testing.WithoutSwt;
import org.junit.jupiter.api.Test;

/**
 * The variable registry loads the class that declares a variable in every Hop process, also in runs
 * without SWT: the dialog variables must load without it.
 */
class BaseDialogVariablesHeadlessTest {

  @Test
  void theDialogVariablesLoadWithoutSwt() throws Exception {
    try (WithoutSwt withoutSwt = new WithoutSwt()) {
      Class<?> variables = Class.forName(BaseDialogVariables.class.getName(), true, withoutSwt);
      Field field = variables.getDeclaredField("HOP_USE_NATIVE_FILE_DIALOG");
      @SuppressWarnings("unchecked")
      Class<Variable> annotation =
          (Class<Variable>) Class.forName(Variable.class.getName(), true, withoutSwt);
      assertNotNull(field.getAnnotation(annotation));
      assertEquals("HOP_USE_NATIVE_FILE_DIALOG", field.get(null));
    }
  }

  @Test
  void theDialogKeepsTheSameConstant() {
    assertEquals(
        BaseDialogVariables.HOP_USE_NATIVE_FILE_DIALOG, BaseDialog.HOP_USE_NATIVE_FILE_DIALOG);
  }
}

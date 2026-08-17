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

package org.apache.hop.ui.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.Test;

/**
 * Plugins compiled against older Hop resolve {@code wTransformName} / {@code wName} as SWT {@link
 * Text}. Changing those field types causes {@link NoSuchFieldError} at dialog open.
 */
class BaseTransformDialogBinaryCompatTest {

  @Test
  void wTransformNameStaysSwtText() throws Exception {
    Field field = BaseTransformDialog.class.getDeclaredField("wTransformName");
    assertEquals(Text.class, field.getType());
  }

  @Test
  void wNameStaysSwtText() throws Exception {
    Field field = ActionDialog.class.getDeclaredField("wName");
    assertEquals(Text.class, field.getType());
  }
}

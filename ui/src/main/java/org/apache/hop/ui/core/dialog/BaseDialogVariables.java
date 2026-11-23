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

import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.variables.VariableScope;

/**
 * The variables of Hop's dialogs. They live here rather than in {@link BaseDialog}: the variable
 * registry loads the class that declares a variable, also in runs without a GUI, and BaseDialog
 * needs SWT to load.
 */
public final class BaseDialogVariables {

  @Variable(
      scope = VariableScope.APPLICATION,
      value = "N",
      description =
          "Set this value to 'Y' if you want to use the system file open/save dialog when browsing files.")
  public static final String HOP_USE_NATIVE_FILE_DIALOG = "HOP_USE_NATIVE_FILE_DIALOG";

  private BaseDialogVariables() {}
}

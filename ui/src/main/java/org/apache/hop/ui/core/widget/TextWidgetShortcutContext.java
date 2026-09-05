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

package org.apache.hop.ui.core.widget;

import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Getter;
import org.apache.hop.core.variables.IVariables;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;

/** Context passed to {@link ITextWidgetShortcut#apply(TextWidgetShortcutContext)}. */
@Getter
@Builder
public class TextWidgetShortcutContext {

  private final Control control;
  private final IVariables variables;
  private final Supplier<String> getText;
  private final Consumer<String> setText;
  private final String namingSchemeType;
  private final boolean variablesEnabled;

  public Shell getShell() {
    if (control == null || control.isDisposed()) {
      return null;
    }
    return control.getShell();
  }
}

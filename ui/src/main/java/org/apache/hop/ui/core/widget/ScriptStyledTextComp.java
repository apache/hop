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

package org.apache.hop.ui.core.widget;

import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.widget.highlight.GenericCodeHighlight;
import org.apache.hop.ui.core.widget.highlight.ScriptEngine;
import org.eclipse.swt.widgets.Composite;

public class ScriptStyledTextComp extends StyledTextVar {

  private ScriptEngine scriptEngine;

  public ScriptStyledTextComp(IVariables variables, Composite parent, int style) {
    this(variables, parent, style, true, false);
  }

  public ScriptStyledTextComp(
      IVariables variables, Composite parent, int style, boolean varsSensitive) {
    this(variables, parent, style, varsSensitive, false);
  }

  public ScriptStyledTextComp(
      IVariables variables,
      Composite parent,
      int style,
      boolean varsSensitive,
      boolean variableIconOnTop) {

    super(variables, parent, style, varsSensitive, variableIconOnTop);
  }

  @Override
  public void addLineStyleListener() {
    throw new UnsupportedOperationException("Not supported for this ScriptStyledTextComp");
  }

  @Override
  public void addLineStyleListener(List<String> keywords) {
    throw new UnsupportedOperationException("Not supported for this ScriptStyledTextComp");
  }

  @Override
  public void addLineStyleListener(String scriptEngine) {
    this.scriptEngine = ScriptEngine.fromString(scriptEngine);
    if (this.scriptEngine == null) {
      this.scriptEngine = ScriptEngine.GROOVY;
    }
    addLineStyleListener(new GenericCodeHighlight(this.scriptEngine));
  }
}

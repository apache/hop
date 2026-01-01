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

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.widget.highlight.LogHighlight;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.widgets.Composite;

public class LogStyledTextComp extends StyledTextVar {
  public LogStyledTextComp(IVariables variables, Composite parent, int style) {
    super(variables, parent, style, false, false);
  }

  @Override
  public void addLineStyleListener() {
    // Only add highlighting in SWT mode (RWT doesn't have StyledText)
    if (!EnvironmentUtils.getInstance().isWeb()) {
      addLineStyleListener(new LogHighlight());
    }
  }

  @Override
  public void addLineStyleListener(java.util.List<String> keywords) {
    // Log highlighting doesn't use keywords, just use default
    addLineStyleListener();
  }
}

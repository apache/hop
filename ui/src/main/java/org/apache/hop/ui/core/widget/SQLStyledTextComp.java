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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.highlight.SqlHighlight;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;

public class SQLStyledTextComp extends StyledTextVar {
  public SQLStyledTextComp(IVariables variables, Composite parent, int style) {
    this(variables, parent, style, true, false);
  }

  public SQLStyledTextComp(
      IVariables variables, Composite parent, int style, boolean varsSensitive) {
    this(variables, parent, style, varsSensitive, false);
  }

  public SQLStyledTextComp(
      IVariables variables,
      Composite parent,
      int style,
      boolean varsSensitive,
      boolean variableIconOnTop) {

    super(variables, parent, style, varsSensitive, variableIconOnTop);
  }

  @Override
  public void addLineStyleListener() {
    addLineStyleListener(new SqlHighlight(List.of()));
  }

  @Override
  public void addLineStyleListener(List<String> keywords) {
    addLineStyleListener(new SqlHighlight(keywords));
  }
}

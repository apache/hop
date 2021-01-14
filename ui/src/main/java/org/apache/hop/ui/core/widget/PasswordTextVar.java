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

import org.apache.hop.core.variables.IVariables;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;

public class PasswordTextVar extends TextVar {

  public PasswordTextVar( IVariables variables, Composite composite, int flags ) {
    super( variables, composite, flags | SWT.PASSWORD, null, null, null );
  }

  public PasswordTextVar( IVariables variables, Composite composite, int flags, String toolTipText ) {
    super( variables, composite, flags | SWT.PASSWORD, toolTipText, null, null );
  }

  public PasswordTextVar( IVariables variables, Composite composite, int flags,
                          IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface ) {
    super( variables, composite, flags | SWT.PASSWORD, null, getCaretPositionInterface, insertTextInterface );
  }

  public PasswordTextVar( IVariables variables, Composite composite, int flags, String toolTipText,
                          IGetCaretPosition getCaretPositionInterface, IInsertText insertTextInterface ) {
    super( variables, composite, flags | SWT.PASSWORD, toolTipText, getCaretPositionInterface, insertTextInterface );
  }

  @Override
  protected ModifyListener getModifyListenerTooltipText( final Text textField ) {
    return e -> textField.setToolTipText( toolTipText );
  }
}

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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** A simple dialog with a message and button that closes the dialog. */
public class BaseMessageDialog extends BaseDialog {
  private static final Class<?> PKG = BaseMessageDialog.class; // For Translator

  private String message;

  public BaseMessageDialog(final Shell shell, final String title, final String message) {
    this(shell, title, message, BaseMessages.getString(PKG, "System.Button.OK"), -1);
  }

  public BaseMessageDialog(
      final Shell shell, final String title, final String message, final int width) {
    this(shell, title, message, BaseMessages.getString(PKG, "System.Button.OK"), width);
  }

  public BaseMessageDialog(
      final Shell shell,
      final String title,
      final String message,
      final String buttonLabel,
      final int width) {
    super(shell, title, width);
    this.message = message;
    this.buttons.put(
        buttonLabel,
        event -> {
          dispose();
        });
  }

  @Override
  protected Control buildBody() {
    final Label message = new Label(shell, SWT.WRAP | SWT.LEFT);
    message.setText(this.message);
    props.setLook(message);
    message.setLayoutData(new FormDataBuilder().top().left().right(100, 0).result());
    return message;
  }
}

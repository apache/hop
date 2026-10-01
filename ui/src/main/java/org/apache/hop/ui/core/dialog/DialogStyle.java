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

import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;

/**
 * The shell style of the big dialogs: transforms, actions, metadata editors and the like.
 *
 * <ul>
 *   <li><b>Hop Web (RAP):</b> {@code DIALOG_TRIM | RESIZE}, without the minimize and maximize
 *       buttons that misbehave in a browser.
 *   <li><b>macOS:</b> a modeless child window ({@code DIALOG_TRIM | RESIZE | MAX}), the style Hop
 *       had before 2.16: it follows the Hop window and stays above it. With the "Allow dialogs to
 *       open on any screen" option on, a modal window instead ({@code PRIMARY_MODAL | CLOSE | TITLE
 *       | RESIZE}) that can be moved to another screen. No minimize button in either case: a
 *       minimized child window cannot be brought back on macOS.
 *   <li><b>Other desktop platforms:</b> {@code DIALOG_TRIM | RESIZE | MAX | MIN}.
 * </ul>
 *
 * <p>{@code SWT.DIALOG_TRIM} includes {@code SWT.TITLE}, {@code SWT.CLOSE} and {@code SWT.BORDER}.
 */
public final class DialogStyle {

  private DialogStyle() {
    // utility
  }

  /** The style for the environment Hop runs in and the user's options. */
  public static int forEnvironment() {
    return of(
        EnvironmentUtils.getInstance().isWeb(),
        OsHelper.isMac(),
        PropsUi.getInstance().isDialogsOnAnyScreenEnabled());
  }

  static int of(boolean web, boolean mac, boolean dialogsOnAnyScreen) {
    if (web) {
      return SWT.DIALOG_TRIM | SWT.RESIZE;
    }
    if (mac) {
      return dialogsOnAnyScreen
          ? SWT.PRIMARY_MODAL | SWT.CLOSE | SWT.TITLE | SWT.RESIZE
          : SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX;
    }
    return SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN;
  }
}

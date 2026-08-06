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

package org.apache.hop.ui.hopgui;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.eclipse.swt.SWT;

/** Desktop: log-off is a Hop Web concept. */
public class HopWebLogoutFacadeImpl extends HopWebLogoutFacade {

  private static final Class<?> PKG = HopGui.class;

  @Override
  void logOffInternal() {
    MessageBox box = new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_INFORMATION | SWT.OK);
    box.setText(BaseMessages.getString(PKG, "HopGui.LogOff.Desktop.Title"));
    box.setMessage(BaseMessages.getString(PKG, "HopGui.LogOff.Desktop.Message"));
    box.open();
  }
}

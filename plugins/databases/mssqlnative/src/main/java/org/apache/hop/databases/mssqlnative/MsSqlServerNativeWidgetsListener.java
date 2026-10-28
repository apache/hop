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

package org.apache.hop.databases.mssqlnative;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;

/**
 * Disables the user name and password when integrated security is used, in the connection dialog.
 */
public class MsSqlServerNativeWidgetsListener implements IGuiPluginCompositeWidgetsListener {

  private final MsSqlServerNativeDatabaseMeta database;

  public MsSqlServerNativeWidgetsListener(IDatabase database) {
    this.database = (MsSqlServerNativeDatabaseMeta) database;
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // not needed
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    enableField(compositeWidgets);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    enableField(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    // not needed
  }

  private void enableField(GuiCompositeWidgets compositeWidgets) {
    List<Control> controls = new ArrayList<>();
    String[] ids =
        new String[] {
          BaseDatabaseMeta.ID_USERNAME_LABEL,
          BaseDatabaseMeta.ID_USERNAME_WIDGET,
          BaseDatabaseMeta.ID_PASSWORD_LABEL,
          BaseDatabaseMeta.ID_PASSWORD_WIDGET,
        };
    for (String id : ids) {
      Control control = compositeWidgets.getWidgetsMap().get(id);
      if (control != null) {
        controls.add(control);
      }
    }
    Button wIntegratedSecurity =
        (Button)
            compositeWidgets
                .getWidgetsMap()
                .get(MsSqlServerNativeDatabaseMeta.ID_INTEGRATED_SECURITY_WIDGET);

    // The checkbox is absent when it is switched off in disabledGuiElements.xml. There is nothing
    // to read the setting from then, so we go by what is on the metadata itself.
    //
    boolean enable =
        wIntegratedSecurity == null
            ? !database.isUsingIntegratedSecurity()
            : !wIntegratedSecurity.getSelection();
    for (Control control : controls) {
      control.setEnabled(enable);
    }
  }
}

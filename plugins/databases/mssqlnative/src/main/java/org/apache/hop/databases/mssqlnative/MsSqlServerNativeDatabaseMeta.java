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

import org.apache.hop.core.Const;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.mssql.MsSqlServerDatabaseMeta;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;

import java.util.ArrayList;
import java.util.List;

@DatabaseMetaPlugin(
        type = "MSSQLNATIVE",
        typeDescription = "MS SQL Server (Native)"
)
@GuiPlugin(id = "GUI-MSSQLServerNativeDatabaseMeta")
public class MsSqlServerNativeDatabaseMeta extends MsSqlServerDatabaseMeta implements IGuiPluginCompositeWidgetsListener {

    public static final String ID_INTEGRATED_SECURITY_WIDGET = "usingIntegratedSecurity";

    @GuiWidgetElement(
            id = ID_INTEGRATED_SECURITY_WIDGET,
            order = "21",
            parentId = DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            type = GuiElementType.CHECKBOX,
            label = "i18n:org.apache.hop.ui.core.database:DatabaseDialog.label.UseIntegratedSecurity"
    )
    @HopMetadataProperty
    private boolean usingIntegratedSecurity;

    @Override
    public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    }

    @Override
    public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
        enableField(compositeWidgets);
    }

    @Override
    public void widgetModified( GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId ) {
        enableField(compositeWidgets);
    }

    @Override public void persistContents( GuiCompositeWidgets compositeWidgets ) {

    }

    private void enableField( GuiCompositeWidgets compositeWidgets) {
        List<Control> controls = new ArrayList<>();
        String[] ids = new String[]{
                BaseDatabaseMeta.ID_USERNAME_LABEL,
                BaseDatabaseMeta.ID_USERNAME_WIDGET,
                BaseDatabaseMeta.ID_PASSWORD_LABEL,
                BaseDatabaseMeta.ID_PASSWORD_WIDGET,
        };
        for (String id : ids) {
            // During creation and so on we get widgets which aren't there. TODO: fix this
            Control control = compositeWidgets.getWidgetsMap().get(id);
            if (control != null) {
                controls.add(control);
            }
        }
        Button wIntegratedSecurity = (Button) compositeWidgets.getWidgetsMap().get(ID_INTEGRATED_SECURITY_WIDGET);

        boolean enable = !wIntegratedSecurity.getSelection();
        for (Control control : controls) {
            control.setEnabled(enable);
        }
    }

    /**
     * Gets usingIntegratedSecurity
     *
     * @return value of usingIntegratedSecurity
     */
    public boolean isUsingIntegratedSecurity() {
        return this.usingIntegratedSecurity;
    }

    /**
     * @param usingIntegratedSecurity The usingIntegratedSecurity to set
     */
    public void setUsingIntegratedSecurity(boolean usingIntegratedSecurity) {
        this.usingIntegratedSecurity = usingIntegratedSecurity;
    }

    @Override
    public String getDriverClass() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getURL(String serverName, String port, String databaseName) {

        StringBuilder sb = new StringBuilder("jdbc:sqlserver://");

        sb.append(serverName);

        // When specifying the location of the SQL Server instance, one normally provides serverName\instanceName or serverName:portNumber
        // If both a portNumber and instanceName are used, the portNumber will take precedence and the instanceName will be ignored.
        if (!Utils.isEmpty(port) && Const.toInt(port, -1) > 0) {
            sb.append(':');
            sb.append(port);
        } else if (!Utils.isEmpty(this.getInstanceName())) {
            sb.append('\\');
            sb.append(this.getInstanceName());
        }

        if (!Utils.isEmpty(databaseName)) {
            sb.append(";databaseName=");
            sb.append(databaseName);
        }

        if (this.usingIntegratedSecurity) {
            sb.append(";integratedSecurity=");
            sb.append(String.valueOf(this.usingIntegratedSecurity));
        }

        return sb.toString();

    }

    @Override
    public boolean supportsGetBlob() {
        return false;
    }

    @Override
    public boolean isMsSqlServerNativeVariant() {
        return true;
    }

    @Override
    public String getStartQuote() {
        return "";
    }

    @Override
    public String getEndQuote() {
        return "";
    }

    @Override
    public String getSchemaTableCombination(String schemaName, String tablePart) {
        // Something special for MSSQL
        //
        if (isUsingDoubleDecimalAsSchemaTableSeparator()) {
            return schemaName + ".." + tablePart;
        } else {
            return '[' + schemaName + ']' + "." + tablePart;
        }
    }
}

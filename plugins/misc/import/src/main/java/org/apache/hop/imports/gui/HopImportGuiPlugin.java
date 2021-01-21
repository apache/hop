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

package org.apache.hop.imports.gui;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.imports.kettle.KettleImport;
import org.apache.hop.imports.kettle.KettleImportDialog;
import org.apache.hop.ui.hopgui.HopGui;

@GuiPlugin
public class HopImportGuiPlugin {

    public static final String ID_MAIN_MENU_FILE_IMPORT = "10060-menu-tools-import";

    public static HopImportGuiPlugin instance;

    public static HopImportGuiPlugin getInstance() {
        if(instance == null){
            instance = new HopImportGuiPlugin();
        }
        return instance;
    }

    @GuiMenuElement(
            root = HopGui.ID_MAIN_MENU,
            id = ID_MAIN_MENU_FILE_IMPORT,
            label = "Import from Kettle/PDI",
//            image = "kettle-logo.svg",
            parentId = HopGui.ID_MAIN_MENU_FILE,
            separator = true
    )
    @GuiKeyboardShortcut(control = true, key = 'i')
    @GuiOsxKeyboardShortcut(command = true, key = 'i')
    public void menuToolsImport(){
        HopGui hopGui = HopGui.getInstance();

        KettleImport kettleImport = new KettleImport();
        KettleImportDialog dialog = new KettleImportDialog(hopGui.getShell(), hopGui.getVariables(), kettleImport);
        dialog.open();

    }
}

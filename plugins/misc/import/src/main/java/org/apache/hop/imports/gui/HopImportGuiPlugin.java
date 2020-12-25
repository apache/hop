package org.apache.hop.imports.gui;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.imports.kettle.KettleImport;
import org.apache.hop.imports.kettle.KettleImportDialog;
import org.apache.hop.ui.hopgui.HopGui;

@GuiPlugin
public class HopImportGuiPlugin {

    public static final String ID_MAIN_MENU_TOOLS_IMPORT = "40300-menu-tools-import";

    public static HopImportGuiPlugin instance;

    public static HopImportGuiPlugin getInstance() {
        if(instance == null){
            instance = new HopImportGuiPlugin();
        }
        return instance;
    }

    @GuiMenuElement(
            root = HopGui.ID_MAIN_MENU,
            id = ID_MAIN_MENU_TOOLS_IMPORT,
            label = "Import Kettle",
            parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
            separator = true
    )
    public void menuToolsImport(){
        HopGui hopGui = HopGui.getInstance();

        KettleImport kettleImport = new KettleImport();
        KettleImportDialog dialog = new KettleImportDialog(hopGui.getShell(), hopGui.getVariables(), kettleImport);
        dialog.open();

    }

/*
    public static final List<String> findKettleFilenames(){
        Set<File> kettleFiles = new HashSet<>();
        kettleFiles.addAll(FileUtils.listFiles(new File("/home/bart/Projects/ABN AMRO"), new String[]{"ktr", "kjb"}, true));
        List<String> kettleFilenames = new ArrayList<>();
        kettleFiles.forEach(file -> kettleFilenames.add(file.toString()));
        return kettleFilenames;
    }
*/
}

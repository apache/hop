package org.apache.hop.ui.core.gui;

import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.tab.GuiTabItem;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.widgets.Composite;

import java.util.*;

public class GuiTabWidgets extends BaseGuiWidgets{

    private HopGui hopGui;
    private List<GuiTabItem> guiTabItems;

    public GuiTabWidgets() {
        super(UUID.randomUUID().toString());
        hopGui = HopGui.getInstance();
        guiTabItems = new ArrayList<>();
    }

    public void createTabWidgets(Composite guiTab, String parent) {
        guiTabItems = GuiRegistry.getInstance().findGuiTabItems(parent);
    }

    public IHopPerspective getPerspective(String parent){
        List<IHopPerspective> perspectiveList = hopGui.getPerspectiveManager().getPerspectives();

        for (GuiTabItem guiTabItem : guiTabItems) {
            for (IHopPerspective perspective : perspectiveList) {
                if (perspective.getId().equals(parent)) {
                    return perspective;
                }
            }
        }
        return null;
    }
}

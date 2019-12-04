package org.apache.hop.core.gui.plugin;

import java.util.HashMap;
import java.util.Map;

/**
 * This singleton keeps track of the various GUI elements that are made plug-able by the developers.
 * For example, a general menu with a certain ID is added by someone and then other developers can then add menu items into it wherever they like.
 * This registry keeps track of everything so that menus can be built dynamically as needed.
 */
public class GuiRegistry {

  private static GuiRegistry guiRegistry;

  private Map<String, Map<String, GuiElements>> dataElementsMap;

  private GuiRegistry() {
    dataElementsMap = new HashMap<>();
  }

  public static final GuiRegistry getInstance() {
    if (guiRegistry==null) {
      guiRegistry = new GuiRegistry();
    }
    return guiRegistry;
  }

  /**
   * Add a bunch of GUI elements under a particular data class name (example: PostgresDatabaseMeta)
   * under a particular parent GUI element ID (the ID of the specific postgres database options)
   *
   * @param dataClassName
   * @param parentGuiElementId
   * @param guiElements
   */
  public void putGuiElements(String dataClassName, String parentGuiElementId, GuiElements guiElements) {
    Map<String, GuiElements> elementsMap = dataElementsMap.get( dataClassName );
    if (elementsMap==null) {
      elementsMap=new HashMap<>(  );
      dataElementsMap.put(dataClassName, elementsMap);
    }
    elementsMap.put(parentGuiElementId, guiElements);
  }

  /**
   * Get the GUI elements for the given data class and parent GUI element ID.
   *
   * @param dataClassName
   * @param parentGuiElementId
   * @return The GUI elements or null if the data class name or parent ID can not be found.
   */
  public GuiElements findGuiElements( String dataClassName, String parentGuiElementId ) {
    Map<String, GuiElements> elementsMap = dataElementsMap.get( dataClassName );
    if (elementsMap==null) {
      return null;
    }
    return elementsMap.get(parentGuiElementId);
  }
}

package org.apache.hop.core.gui.plugin;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

  /**
   * Add a GUI element to the registry.
   * If there is no elements objects for the parent ID under which the element belongs, one will be added.
   *
   * @param dataClassName
   * @param guiElement
   * @param fieldName
   * @param fieldClass
   */
  public void addGuiElement( String dataClassName, GuiElement guiElement, String fieldName, Class<?> fieldClass ) {
    GuiElements guiElements = findGuiElements( dataClassName, guiElement.parentId() );
    if (guiElements==null) {
      guiElements= new GuiElements();
      putGuiElements( dataClassName, guiElement.parentId(), guiElements );
    }
    GuiElements child = new GuiElements( guiElement, fieldName, fieldClass );

    guiElements.getChildren().add(child);
  }

  /**
   * Sort all the GUI elements in all data classes for all parent IDs
   * You typically call this only once after loading all the GUI Plugins or when adding more plugins
   */
  public void sortAllElements() {
    Set<String> dataClassNames = dataElementsMap.keySet();
    for (String dataClassName : dataClassNames) {
      Map<String, GuiElements> guiElementsMap = dataElementsMap.get( dataClassName );
      Set<String> parentIds = guiElementsMap.keySet();
      for (String parentId : parentIds) {
        GuiElements guiElements = guiElementsMap.get( parentId );
        guiElements.sortChildren();
      }
    }
  }
}

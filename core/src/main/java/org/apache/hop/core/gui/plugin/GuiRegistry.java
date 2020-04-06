package org.apache.hop.core.gui.plugin;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreElementType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
  private Map<String, List<KeyboardShortcut>> shortCutsMap;
  private Map<String, List<GuiAction>> contextActionsMap;
  private Map<Class<? extends IHopMetaStoreElement>, GuiMetaStoreElement> metaStoreTypeMap;
  private Map<Class<? extends IHopMetaStoreElement>, ClassLoader> metaStoreClassLoaderMap;

  private GuiRegistry() {
    dataElementsMap = new HashMap<>();
    shortCutsMap = new HashMap<>();
    contextActionsMap = new HashMap<>();
    metaStoreTypeMap = new HashMap<>(  );
    metaStoreClassLoaderMap = new HashMap<>(  );
  }

  public static final GuiRegistry getInstance() {
    if ( guiRegistry == null ) {
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
  public void putGuiElements( String dataClassName, String parentGuiElementId, GuiElements guiElements ) {
    Map<String, GuiElements> elementsMap = dataElementsMap.get( dataClassName );
    if ( elementsMap == null ) {
      elementsMap = new HashMap<>();
      dataElementsMap.put( dataClassName, elementsMap );
    }
    elementsMap.put( parentGuiElementId, guiElements );
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
    if ( elementsMap == null ) {
      return null;
    }
    GuiElements guiElements = elementsMap.get( parentGuiElementId );
    if ( guiElements == null ) {
      for ( GuiElements elements : elementsMap.values() ) {
        GuiElements found = findChildGuiElementsById( elements, parentGuiElementId );
        if ( found != null ) {
          return found;
        }
      }
    }
    return guiElements;
  }

  /**
   * Look at the given {@link GuiElements} object its children and see if the element with the given ID is found.
   *
   * @param guiElements The element and its children to examine
   * @param id          The element ID to look for
   * @return The GuiElement if any is found or null if nothing is found.
   */
  public GuiElements findChildGuiElementsById( GuiElements guiElements, String id ) {
    if ( guiElements.getId() != null && guiElements.getId().equals( id ) ) {
      return guiElements;
    }
    for ( GuiElements child : guiElements.getChildren() ) {
      GuiElements found = findChildGuiElementsById( child, id );
      if ( found != null ) {
        return found;
      }
    }
    return null;
  }

  /**
   * Add a GUI element to the registry.
   * If there is no elements objects for the parent ID under which the element belongs, one will be added.
   *
   * @param dataClassName
   * @param guiElement
   * @param field
   */
  public void addGuiElement( String dataClassName, GuiWidgetElement guiElement, Field field ) {
    GuiElements guiElements = findGuiElements( dataClassName, guiElement.parentId() );
    if ( guiElements == null ) {
      guiElements = new GuiElements();
      putGuiElements( dataClassName, guiElement.parentId(), guiElements );
    }
    GuiElements child = new GuiElements( guiElement, field );

    // See if we need to disable something of if something is disabled already...
    // In those scenarios we ignore the GuiWidgetElement
    //
    GuiElements existing = guiElements.findChild( guiElement.id() );
    if ( existing != null && existing.isIgnored() ) {
      return;
    }
    if ( existing != null && child.isIgnored() ) {
      existing.setIgnored( true );
      return;
    }

    guiElements.getChildren().add( child );
  }

  /**
   * Add a GUI menu element to the registry.
   * If there is no elements objects for the parent ID under which the element belongs, one will be added.
   *
   * @param dataClassName
   * @param guiElement
   * @param method
   */
  public void addMethodElement( String dataClassName, GuiMenuElement guiElement, Method method ) {
    GuiElements guiElements = findGuiElements( dataClassName, guiElement.parentId() );
    if ( guiElements == null ) {
      guiElements = new GuiElements();
      putGuiElements( dataClassName, guiElement.parentId(), guiElements );
    }
    GuiElements child = new GuiElements( guiElement, method );

    // See if we need to disable something of if something is disabled already...
    // In those scenarios we ignore the GuiWidgetElement
    //
    GuiElements existing = guiElements.findChild( guiElement.id() );
    if ( existing != null && existing.isIgnored() ) {
      return;
    }
    if ( existing != null && child.isIgnored() ) {
      existing.setIgnored( true );
      return;
    }

    guiElements.getChildren().add( child );
  }

  /**
   * Add a GUI element to the registry.
   * If there is no elements objects for the parent ID under which the element belongs, one will be added.
   *
   * @param parentClassName The parent under which the widgets are stored
   * @param dataClass       The data class (singleton) of the method
   * @param guiElement
   * @param method
   */
  public void addMethodElement( String parentClassName, Class<?> dataClass, GuiToolbarElement guiElement, Method method ) {
    GuiElements guiElements = findGuiElements( parentClassName, guiElement.parentId() );
    if ( guiElements == null ) {
      guiElements = new GuiElements();
      putGuiElements( parentClassName, guiElement.parentId(), guiElements );
    }
    GuiElements child = new GuiElements( guiElement, dataClass, method );

    // See if we need to disable something of if something is disabled already...
    // In those scenarios we ignore the GuiWidgetElement
    //
    GuiElements existing = guiElements.findChild( guiElement.id() );
    if ( existing != null && existing.isIgnored() ) {
      return;
    }
    if ( existing != null && child.isIgnored() ) {
      existing.setIgnored( true );
      return;
    }

    guiElements.getChildren().add( child );
  }

  /**
   * Sort all the GUI elements in all data classes for all parent IDs
   * You typically call this only once after loading all the GUI Plugins or when adding more plugins
   */
  public void sortAllElements() {
    Set<String> dataClassNames = dataElementsMap.keySet();
    for ( String dataClassName : dataClassNames ) {
      Map<String, GuiElements> guiElementsMap = dataElementsMap.get( dataClassName );
      Set<String> parentIds = guiElementsMap.keySet();
      for ( String parentId : parentIds ) {
        GuiElements guiElements = guiElementsMap.get( parentId );
        guiElements.sortChildren();
      }
    }
  }

  public void addKeyboardShortcut( String parentClassName, Method parentMethod, GuiKeyboardShortcut shortcut ) {
    List<KeyboardShortcut> shortcuts = shortCutsMap.get( parentClassName );
    if ( shortcuts == null ) {
      shortcuts = new ArrayList<>();
      shortCutsMap.put( parentClassName, shortcuts );
    }
    shortcuts.add( new KeyboardShortcut( shortcut, parentMethod ) );
  }

  public void addKeyboardShortcut( String parentClassName, Method parentMethod, GuiOSXKeyboardShortcut shortcut ) {
    List<KeyboardShortcut> shortcuts = shortCutsMap.get( parentClassName );
    if ( shortcuts == null ) {
      shortcuts = new ArrayList<>();
      shortCutsMap.put( parentClassName, shortcuts );
    }
    shortcuts.add( new KeyboardShortcut( shortcut, parentMethod ) );
  }

  public List<KeyboardShortcut> getKeyboardShortcuts( String parentClassName ) {
    List<KeyboardShortcut> shortcuts = shortCutsMap.get( parentClassName );
    return shortcuts;
  }

  public KeyboardShortcut findKeyboardShortcut( String parentClassName, String methodName, boolean osx ) {
    List<KeyboardShortcut> shortcuts = getKeyboardShortcuts( parentClassName );
    if ( shortcuts != null ) {
      for ( KeyboardShortcut shortcut : shortcuts ) {
        if ( shortcut.getParentMethodName().equals( methodName ) ) {
          if ( shortcut.isOsx() == osx ) {
            return shortcut;
          }
        }
      }
    }
    return null;
  }

  /**
   * Add a GUI context action for the given method and its annotation.
   * Also provide a classloader which can be used to load resources later.
   *
   * @param method
   * @param ca
   * @param classLoader
   */
  public void addGuiContextAction( Method method, GuiContextAction ca, ClassLoader classLoader ) {
    GuiAction action = new GuiAction( ca.id(), ca.type(), ca.name(), ca.tooltip(), ca.image(), method.getName() );
    action.setClassLoader( classLoader );

    List<GuiAction> actions = contextActionsMap.get( ca.parentId() );
    if ( actions == null ) {
      actions = new ArrayList<>();
      contextActionsMap.put( ca.parentId(), actions );
    }
    actions.add( action );
  }

  public List<GuiAction> getGuiContextActions( String parentContextId ) {
    return contextActionsMap.get( parentContextId );
  }

  public void addMetaStoreElementType( Class<? extends IHopMetaStoreElement> elementClass, GuiMetaStoreElement guiMetaStoreElement, ClassLoader classLoader ) {
    metaStoreTypeMap.put( elementClass, guiMetaStoreElement );
    metaStoreClassLoaderMap.put( elementClass, classLoader );

  }


  /**
   * Gets dataElementsMap
   *
   * @return value of dataElementsMap
   */
  public Map<String, Map<String, GuiElements>> getDataElementsMap() {
    return dataElementsMap;
  }

  /**
   * @param dataElementsMap The dataElementsMap to set
   */
  public void setDataElementsMap( Map<String, Map<String, GuiElements>> dataElementsMap ) {
    this.dataElementsMap = dataElementsMap;
  }

  /**
   * Gets shortCutsMap
   *
   * @return value of shortCutsMap
   */
  public Map<String, List<KeyboardShortcut>> getShortCutsMap() {
    return shortCutsMap;
  }

  /**
   * @param shortCutsMap The shortCutsMap to set
   */
  public void setShortCutsMap( Map<String, List<KeyboardShortcut>> shortCutsMap ) {
    this.shortCutsMap = shortCutsMap;
  }

  /**
   * Gets contextActionsMap
   *
   * @return value of contextActionsMap
   */
  public Map<String, List<GuiAction>> getContextActionsMap() {
    return contextActionsMap;
  }

  /**
   * @param contextActionsMap The contextActionsMap to set
   */
  public void setContextActionsMap( Map<String, List<GuiAction>> contextActionsMap ) {
    this.contextActionsMap = contextActionsMap;
  }

  /**
   * Gets metaStoreTypeMap
   *
   * @return value of metaStoreTypeMap
   */
  public Map<Class<? extends IHopMetaStoreElement>, GuiMetaStoreElement> getMetaStoreTypeMap() {
    return metaStoreTypeMap;
  }

  /**
   * @param metaStoreTypeMap The metaStoreTypeMap to set
   */
  public void setMetaStoreTypeMap( Map<Class<? extends IHopMetaStoreElement>, GuiMetaStoreElement> metaStoreTypeMap ) {
    this.metaStoreTypeMap = metaStoreTypeMap;
  }

  /**
   * Gets metaStoreClassLoaderMap
   *
   * @return value of metaStoreClassLoaderMap
   */
  public Map<Class<? extends IHopMetaStoreElement>, ClassLoader> getMetaStoreClassLoaderMap() {
    return metaStoreClassLoaderMap;
  }

  /**
   * @param metaStoreClassLoaderMap The metaStoreClassLoaderMap to set
   */
  public void setMetaStoreClassLoaderMap( Map<Class<? extends IHopMetaStoreElement>, ClassLoader> metaStoreClassLoaderMap ) {
    this.metaStoreClassLoaderMap = metaStoreClassLoaderMap;
  }
}

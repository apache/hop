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

package org.apache.hop.core.gui.plugin;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.menu.GuiMenuItem;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarItem;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

  /**
   * this map links the GUI class to the menu elements information.
   * For example, it would contain the root ID of the HopGui class at the top of the map.
   * For the HopGui main menu we would have a menu elements stored per ID.
   */
  private Map<String, Map<String, GuiMenuItem>> guiMenuMap;
  private Map<String, Map<String, GuiToolbarItem>> guiToolbarMap;
  private Map<String, Map<String, GuiElements>> dataElementsMap;
  private Map<String, List<KeyboardShortcut>> shortCutsMap;
  private Map<String, List<GuiAction>> contextActionsMap;

  /**
   * The first entry in this map is the HopGui ID
   * Then the maps found are GuiPlugin class names and their instances.  It's used to get the methods and fields for toolbars, components, ...
   */
  private Map<String, Map<String, Map<String, Object>>> guiPluginObjectsMap;

  private GuiRegistry() {
    guiMenuMap = new HashMap<>();
    guiToolbarMap = new HashMap<>();
    dataElementsMap = new HashMap<>();
    shortCutsMap = new HashMap<>();
    contextActionsMap = new HashMap<>();
    guiPluginObjectsMap = new HashMap<>();
  }

  public static final GuiRegistry getInstance() {
    if ( guiRegistry == null ) {
      guiRegistry = new GuiRegistry();
    }
    return guiRegistry;
  }

  /**
   * Add GUI Menu elements under a particular gui root (example: HopGui-MainMenu)
   * under a particular parent element ID
   *
   * @param root
   * @param guiMenuItem
   */
  public void addGuiMenuItem( String root, GuiMenuItem guiMenuItem ) {
    Map<String, GuiMenuItem> menuMap = guiMenuMap.get( root );
    if ( menuMap == null ) {
      menuMap = new HashMap<>();
      guiMenuMap.put( root, menuMap );
    }
    menuMap.put( guiMenuItem.getId(), guiMenuItem );
  }

  /**
   * Get the GUI Menu Item for the given root and the given ID.
   *
   * @param root
   * @param id
   * @return The GUI Menu elements or null if the gui class name or ID can not be found.
   */
  public GuiMenuItem findGuiMenuItem( String root, String id ) {
    Map<String, GuiMenuItem> menuMap = guiMenuMap.get( root );
    if ( menuMap == null ) {
      return null;
    }
    return menuMap.get( id );
  }

  /**
   * Find the root menu item for a certain GUI root (HopGui for example).
   *
   * @param root The menu root ID
   * @return An empty list if the root could not be found. The parent menu items or an empty list if nothing was found.
   */
  public List<GuiMenuItem> findChildGuiMenuItems( String root, String parentId ) {
    Map<String, GuiMenuItem> menuMap = guiMenuMap.get( root );
    if ( menuMap == null ) {
      return Collections.emptyList();
    }
    List<GuiMenuItem> items = new ArrayList<>();
    for ( GuiMenuItem item : menuMap.values() ) {
      if ( item.getParentId().equals( parentId ) ) {
        items.add( item );
      }
    }
    return items;
  }


  /**
   * Add a GUI Toolbar element under a particular gui root (example: HopGui-MainMenu)
   * under a particular parent element ID
   *
   * @param root
   * @param guiToolbarItem
   */
  public void addGuiToolbarItem( String root, GuiToolbarItem guiToolbarItem ) {
    Map<String, GuiToolbarItem> toolbarMap = guiToolbarMap.get( root );
    if ( toolbarMap == null ) {
      toolbarMap = new HashMap<>();
      guiToolbarMap.put( root, toolbarMap );
    }
    toolbarMap.put( guiToolbarItem.getId(), guiToolbarItem );
  }

  /**
   * Get the GUI Menu Item for the given root and the given ID.
   *
   * @param root
   * @param id
   * @return The GUI Menu elements or null if the gui class name or ID can not be found.
   */
  public GuiToolbarItem findGuiToolbarItem( String root, String id ) {
    Map<String, GuiToolbarItem> toolbarMap = guiToolbarMap.get( root );
    if ( toolbarMap == null ) {
      return null;
    }
    return toolbarMap.get( id );
  }

  /**
   * Find the root menu item for a certain GUI Toolbar root.
   *
   * @param root The toolbar root ID
   * @return Returns either: an empty list if the root could not be found, the toolbar items or an empty list if nothing was found.
   */
  public List<GuiToolbarItem> findGuiToolbarItems( String root ) {
    Map<String, GuiToolbarItem> menuMap = guiToolbarMap.get( root );
    if ( menuMap == null ) {
      return Collections.emptyList();
    }
    List<GuiToolbarItem> items = new ArrayList<>();
    for ( GuiToolbarItem item : menuMap.values() ) {
      items.add( item );
    }
    return items;
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
  public void addGuiWidgetElement( String dataClassName, GuiWidgetElement guiElement, Field field ) {
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
   * @param guiPluginClassName   Class in which we paint the GUI element
   * @param guiElement
   * @param guiPluginClassMethod
   */
  public void addGuiWidgetElement( String guiPluginClassName, GuiMenuElement guiElement, Method guiPluginClassMethod, ClassLoader classLoader ) {

    // Extract all the information we need from the available data at boot time
    //
    GuiMenuItem guiMenuItem = new GuiMenuItem( guiElement, guiPluginClassMethod, guiPluginClassName, classLoader );

    // Store the element under the specified root
    // This holds together a menu
    //
    addGuiMenuItem( guiElement.root(), guiMenuItem );
  }

  /**
   * Add a GUI element to the registry.
   * If there is no elements objects for the parent ID under which the element belongs, one will be added.
   *
   * @param guiPluginClassName The parent under which the widgets are stored
   * @param toolbarElement
   * @param method
   * @param classLoader
   */
  public void addGuiToolbarElement( String guiPluginClassName, GuiToolbarElement toolbarElement, Method method, ClassLoader classLoader ) {

    // Convert it to a class so we can work with it more easily compared to an annotation
    //
    GuiToolbarItem toolbarItem = new GuiToolbarItem( toolbarElement, guiPluginClassName, method, classLoader );

    // Store the toolbar item under its root
    //
    addGuiToolbarItem( toolbarElement.root(), toolbarItem );
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

  public void addKeyboardShortcut( String guiPluginClassName, Method method, GuiKeyboardShortcut shortcut ) {
    List<KeyboardShortcut> shortcuts = shortCutsMap.get( guiPluginClassName );
    if ( shortcuts == null ) {
      shortcuts = new ArrayList<>();
      shortCutsMap.put( guiPluginClassName, shortcuts );
    }
    KeyboardShortcut keyboardShortCut = new KeyboardShortcut( shortcut, method );
    shortcuts.add( keyboardShortCut );
  }

  public void addKeyboardShortcut( String parentClassName, Method parentMethod, GuiOsxKeyboardShortcut shortcut ) {
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

  // Shortcuts are pretty much global so we'll look everywhere...
  //
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
   * @param guiPluginClassName
   * @param method
   * @param ca
   * @param classLoader
   */
  public void addGuiContextAction( String guiPluginClassName, Method method, GuiContextAction ca, ClassLoader classLoader ) {
    GuiAction action = new GuiAction( ca.id(), ca.type(), ca.name(), ca.tooltip(), ca.image(), guiPluginClassName, method.getName() );
    action.setCategory( StringUtils.isEmpty(ca.category()) ? null : ca.category() );
    action.setCategoryOrder( StringUtils.isEmpty(ca.categoryOrder()) ? null : ca.categoryOrder() );
    action.setKeywords( Arrays.asList(ca.keywords()) );
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

  /**
   * @param hopGuiId           The HopGui ID
   * @param guiPluginClassname
   * @param instanceId
   * @param guiPluginObject
   */
  public void registerGuiPluginObject( String hopGuiId, String guiPluginClassname, String instanceId, Object guiPluginObject ) {
    Map<String, Map<String, Object>> instanceObjectsMap = guiPluginObjectsMap.get( hopGuiId );
    if ( instanceObjectsMap == null ) {
      instanceObjectsMap = new HashMap<>();
      guiPluginObjectsMap.put( hopGuiId, instanceObjectsMap );
    }
    Map<String, Object> objectsMap = instanceObjectsMap.get( instanceId );
    if ( objectsMap == null ) {
      objectsMap = new HashMap<>();
      instanceObjectsMap.put( instanceId, objectsMap );
    }
    objectsMap.put( guiPluginClassname, guiPluginObject );
  }

  /**
   * @param hopGuiId           The HopGui ID
   * @param guiPluginClassname
   * @param instanceId
   * @return
   */
  public Object findGuiPluginObject( String hopGuiId, String guiPluginClassname, String instanceId ) {

    Map<String, Map<String, Object>> instanceObjectsMap = guiPluginObjectsMap.get( hopGuiId );
    if ( instanceObjectsMap == null ) {
      return null;
    }
    Map<String, Object> objectsMap = instanceObjectsMap.get( instanceId );
    if ( objectsMap == null ) {
      return null;
    }
    return objectsMap.get( guiPluginClassname );
  }

  /**
   * Remove the GuiPlugin object once it's disposed.
   *
   * @param hopGuiId
   * @param guiPluginClassname
   * @param instanceId
   */
  public void removeGuiPluginObject( String hopGuiId, String guiPluginClassname, String instanceId ) {
    Map<String, Map<String, Object>> instanceObjectsMap = guiPluginObjectsMap.get( hopGuiId );
    if ( instanceObjectsMap == null ) {
      return;
    }
    Map<String, Object> objectsMap = instanceObjectsMap.get( instanceId );
    if ( objectsMap == null ) {
      return;
    }
    objectsMap.remove( guiPluginClassname );
  }

  /**
   * Remove all objects with the given instanceId
   *
   * @param hopGuiId
   * @param instanceId
   */
  public void removeGuiPluginObjects( String hopGuiId, String instanceId ) {
    Map<String, Map<String, Object>> instanceObjectsMap = guiPluginObjectsMap.get( hopGuiId );
    if ( instanceObjectsMap == null ) {
      return;
    }
    instanceObjectsMap.remove( instanceId );
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
   * Gets guiMenuMap
   *
   * @return value of guiMenuMap
   */
  public Map<String, Map<String, GuiMenuItem>> getGuiMenuMap() {
    return guiMenuMap;
  }

  /**
   * @param guiMenuMap The guiMenuMap to set
   */
  public void setGuiMenuMap( Map<String, Map<String, GuiMenuItem>> guiMenuMap ) {
    this.guiMenuMap = guiMenuMap;
  }

  /**
   * Gets guiToolbarMap
   *
   * @return value of guiToolbarMap
   */
  public Map<String, Map<String, GuiToolbarItem>> getGuiToolbarMap() {
    return guiToolbarMap;
  }

  /**
   * @param guiToolbarMap The guiToolbarMap to set
   */
  public void setGuiToolbarMap( Map<String, Map<String, GuiToolbarItem>> guiToolbarMap ) {
    this.guiToolbarMap = guiToolbarMap;
  }

  /**
   * Gets guiPluginObjectsMap
   *
   * @return value of guiPluginObjectsMap
   */
  public Map<String, Map<String, Map<String, Object>>> getGuiPluginObjectsMap() {
    return guiPluginObjectsMap;
  }

  /**
   * @param guiPluginObjectsMap The guiPluginObjectsMap to set
   */
  public void setGuiPluginObjectsMap( Map<String, Map<String, Map<String, Object>>> guiPluginObjectsMap ) {
    this.guiPluginObjectsMap = guiPluginObjectsMap;
  }

}

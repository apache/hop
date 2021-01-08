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

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.SearchableAnalyserPluginType;
import org.apache.hop.ui.hopgui.file.HopFileTypePluginType;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class HopGuiEnvironment extends HopClientEnvironment {

  public static void init() throws HopException {
    init( Arrays.asList(
      GuiPluginType.getInstance(),
      HopPerspectivePluginType.getInstance(),
      HopFileTypePluginType.getInstance(),
      SearchableAnalyserPluginType.getInstance()
    ) );
  }

  public static void init( List<IPluginType> pluginTypes ) throws HopException {
    pluginTypes.forEach( PluginRegistry::addPluginType );

    for ( IPluginType pluginType : pluginTypes ) {
      pluginType.searchPlugins();
    }

    initGuiPlugins();
  }

  /**
   * Look for GuiWidgetElement annotated fields in all the GuiPlugins.
   * Put them in the Gui registry
   *
   * @throws HopException
   */
  public static void initGuiPlugins() throws HopException {

    try {
      GuiRegistry guiRegistry = GuiRegistry.getInstance();
      PluginRegistry pluginRegistry = PluginRegistry.getInstance();

      List<IPlugin> guiPlugins = pluginRegistry.getPlugins( GuiPluginType.class );
      for ( IPlugin guiPlugin : guiPlugins ) {
        ClassLoader classLoader = pluginRegistry.getClassLoader( guiPlugin );
        Class<?>[] typeClasses = guiPlugin.getClassMap().keySet().toArray( new Class<?>[ 0 ] );
        String guiPluginClassName = guiPlugin.getClassMap().get( typeClasses[ 0 ] );
        Class<?> guiPluginClass = classLoader.loadClass( guiPluginClassName );

        // Component widgets are defined on fields
        //
        List<Field> fields = findDeclaredFields( guiPluginClass );

        for ( Field field : fields ) {
          GuiWidgetElement guiElement = field.getAnnotation( GuiWidgetElement.class );
          if ( guiElement != null ) {
            // Add the GUI Element to the registry...
            //
            guiRegistry.addGuiWidgetElement( guiPluginClassName, guiElement, field );
          }
        }

        // Menu and toolbar items are defined on methods
        //
        List<Method> methods = findDeclaredMethods( guiPluginClass );
        for ( Method method : methods ) {
          GuiMenuElement menuElement = method.getAnnotation( GuiMenuElement.class );
          if ( menuElement != null ) {
            guiRegistry.addGuiWidgetElement( guiPluginClassName, menuElement, method, classLoader );
          }
          GuiToolbarElement toolbarElement = method.getAnnotation( GuiToolbarElement.class );
          if ( toolbarElement != null ) {
            guiRegistry.addGuiToolbarElement( guiPluginClassName, toolbarElement, method, classLoader );
          }
          GuiKeyboardShortcut shortcut = method.getAnnotation( GuiKeyboardShortcut.class );
          if ( shortcut != null ) {
            // RAP does not support ESC as a shortcut key.
            if (EnvironmentUtils.getInstance().isWeb() && shortcut.key() == SWT.ESC) {
              continue;
            }
            guiRegistry.addKeyboardShortcut( guiPluginClassName, method, shortcut );
          }
          GuiOsxKeyboardShortcut osxShortcut = method.getAnnotation( GuiOsxKeyboardShortcut.class );
          if ( osxShortcut != null ) {
            // RAP does not support ESC as a shortcut key.
            if (EnvironmentUtils.getInstance().isWeb() && osxShortcut.key() == SWT.ESC) {
              continue;
            }
            guiRegistry.addKeyboardShortcut( guiPluginClassName, method, osxShortcut );
          }
          GuiContextAction contextAction = method.getAnnotation( GuiContextAction.class );
          if ( contextAction != null ) {
            guiRegistry.addGuiContextAction( guiPluginClassName, method, contextAction, classLoader );
          }
        }
      }

      // Sort all GUI elements once.
      //
      guiRegistry.sortAllElements();

      // Now populate the HopFileTypeRegistry
      //
      // Get all the file handler plugins
      //
      PluginRegistry registry = PluginRegistry.getInstance();
      List<IPlugin> plugins = registry.getPlugins( HopFileTypePluginType.class );
      for ( IPlugin plugin : plugins ) {
        try {
          IHopFileType hopFileTypeInterface = registry.loadClass( plugin, IHopFileType.class );
          HopFileTypeRegistry.getInstance().registerHopFile( hopFileTypeInterface );
        } catch ( HopPluginException e ) {
          throw new HopException( "Unable to load plugin with ID '" + plugin.getIds()[ 0 ] + "' and type : " + plugin.getPluginType().getName(), e );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error looking for Elements in GUI Plugins ", e );
    }
  }
}

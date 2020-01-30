package org.apache.hop.ui.hopgui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.GuiElementMethod;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiMenuElement;
import org.apache.hop.core.gui.plugin.GuiOSXKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypePluginType;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HopGuiEnvironment extends HopClientEnvironment {

  public static void init() throws HopException {
    init( Arrays.asList(
      GuiPluginType.getInstance(),
      HopPerspectivePluginType.getInstance(),
      HopFileTypePluginType.getInstance()
    ));
  }

  public static void init(List<PluginTypeInterface> pluginTypes) throws HopException {
    pluginTypes.forEach( PluginRegistry::addPluginType );

    for (PluginTypeInterface pluginType : pluginTypes) {
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

      List<PluginInterface> guiPlugins = pluginRegistry.getPlugins( GuiPluginType.class );
      for ( PluginInterface guiPlugin : guiPlugins ) {
        ClassLoader classLoader = pluginRegistry.getClassLoader( guiPlugin );
        Class<?>[] typeClasses = guiPlugin.getClassMap().keySet().toArray( new Class<?>[ 0 ] );
        String parentClassName = guiPlugin.getClassMap().get( typeClasses[ 0 ] );
        Class<?> parentClass = classLoader.loadClass( parentClassName );

        // Component widgets are defined on fields
        //
        List<Field> fields = findDeclaredFields( parentClass );

        for ( Field field : fields ) {
          GuiWidgetElement guiElement = field.getAnnotation( GuiWidgetElement.class );
          if ( guiElement != null ) {
            // Add the GUI Element to the registry...
            //
            guiRegistry.addGuiElement( parentClassName, guiElement, field );
          }
        }

        // Menu and toolbar items are defined on methods
        //
        List<GuiElementMethod> menuItems = new ArrayList<>(  );
        List<GuiElementMethod> toolBarItems = new ArrayList<>(  );

        List<Method> methods = findDeclaredMethods( parentClass );
        for ( Method method : methods ) {
          GuiMenuElement menuElement = method.getAnnotation( GuiMenuElement.class );
          if ( menuElement != null ) {
            menuItems.add( new GuiElementMethod( menuElement, method ) );
          }
          GuiToolbarElement toolbarElement = method.getAnnotation( GuiToolbarElement.class );
          if ( toolbarElement != null ) {
            toolBarItems.add( new GuiElementMethod( toolbarElement, method ) );
          }
          GuiKeyboardShortcut shortcut = method.getAnnotation( GuiKeyboardShortcut.class );
          if ( shortcut != null ) {
            guiRegistry.addKeyboardShortcut( parentClassName, method, shortcut );
          }
          GuiOSXKeyboardShortcut osxShortcut = method.getAnnotation( GuiOSXKeyboardShortcut.class );
          if ( osxShortcut != null ) {
            guiRegistry.addKeyboardShortcut( parentClassName, method, osxShortcut );
          }
        }

        Collections.sort( menuItems, new Comparator<GuiElementMethod>() {
          @Override public int compare( GuiElementMethod o1, GuiElementMethod o2 ) {
            if ( StringUtils.isEmpty(o1.menuElement.order()) || StringUtils.isEmpty( o2.menuElement.order() )) {
              return o1.menuElement.id().compareTo( o2.menuElement.id() );
            } else {
              return o1.menuElement.order().compareTo( o2.menuElement.order() );
            }
          }
        } );
        Collections.sort( toolBarItems, new Comparator<GuiElementMethod>() {
          @Override public int compare( GuiElementMethod o1, GuiElementMethod o2 ) {
            if ( StringUtils.isEmpty(o1.toolBarElement.order()) || StringUtils.isEmpty( o2.toolBarElement.order() )) {
              return o1.toolBarElement.id().compareTo( o2.toolBarElement.id() );
            } else {
              return o1.toolBarElement.order().compareTo( o2.toolBarElement.order() );
            }
          }
        } );


        for (GuiElementMethod item : menuItems) {
          guiRegistry.addMethodElement( parentClassName, item.menuElement, item.method );
        }
        for (GuiElementMethod item : toolBarItems) {
          if (StringUtils.isEmpty( item.toolBarElement.parent())) {
            guiRegistry.addMethodElement( parentClassName, parentClass, item.toolBarElement, item.method );
          } else {
            guiRegistry.addMethodElement( item.toolBarElement.parent(), parentClass, item.toolBarElement, item.method );
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
      List<PluginInterface> plugins = registry.getPlugins( HopFileTypePluginType.class );
      for (PluginInterface plugin : plugins) {
        try {
          HopFileTypeInterface hopFileTypeInterface = registry.loadClass( plugin, HopFileTypeInterface.class );
          HopFileTypeRegistry.getInstance().registerHopFile( hopFileTypeInterface );
        } catch ( HopPluginException e ) {
          throw new HopException( "Unable to load plugin with ID '"+plugin.getIds()[0]+"' and type : "+plugin.getPluginType().getName(), e );
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error looking for Elements in GUI Plugins ", e );
    }
  }
}

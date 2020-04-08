/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.core.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.KeyboardShortcut;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the widgets for Menu Bars
 */
public class GuiMenuWidgets {

  private IVariables variables;
  private Map<String, MenuItem> menuItemMap;
  private Map<String, KeyboardShortcut> shortcutMap;

  public GuiMenuWidgets( IVariables variables ) {
    this.variables = variables;
    this.menuItemMap = new HashMap<>();
    this.shortcutMap = new HashMap<>();
  }

  public void createMenuWidgets( Object sourceData, Shell shell, Menu parent, String parentGuiElementId ) {
    // Find the GUI Elements for the given class...
    //
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements = registry.findGuiElements( sourceData.getClass().getName(), parentGuiElementId );
    if ( guiElements == null ) {
      System.err.println( "Create menu widgets: no GUI elements found for class: " + sourceData.getClass().getName() + ", parent ID: " + parentGuiElementId );
      return;
    }

    // Loop over the GUI elements and create menus all the way down...
    //
    addMenuWidgets( sourceData, shell, parent, guiElements );
  }

  private void addMenuWidgets( Object sourceData, Shell shell, Menu parentMenu, GuiElements guiElements ) {

    if ( guiElements.isIgnored() ) {
      return;
    }

    PropsUI props = PropsUI.getInstance();

    MenuItem menuItem;

    // With children mean: drop-down menu item
    //
    if ( guiElements.getChildren().isEmpty() ) {

      if ( guiElements.isAddingSeparator() ) {
        new MenuItem( parentMenu, SWT.SEPARATOR );
      }

      menuItem = new MenuItem( parentMenu, SWT.PUSH );
      menuItem.setText( guiElements.getLabel() );
      if ( StringUtils.isNotEmpty( guiElements.getImage() ) ) {
        menuItem.setImage( GUIResource.getInstance().getImage( guiElements.getImage(), ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE ) );
      }

      setMenuItemKeyboardShortcut( menuItem, guiElements, sourceData.getClass().getName() );
      if ( StringUtils.isNotEmpty( guiElements.getToolTip() ) ) {
        menuItem.setToolTipText( guiElements.getToolTip() );
      }

      // Call the method to which the GuiWidgetElement annotation belongs.
      //
      menuItem.addListener( SWT.Selection, e -> {
        try {
          Method menuMethod = sourceData.getClass().getMethod( guiElements.getListenerMethod() );
          if ( menuMethod == null ) {
            throw new HopException( "Unable to find method " + guiElements.getListenerMethod() + " in class " + sourceData.getClass().getName() );
          }
          menuMethod.invoke( sourceData );
        } catch ( Exception ex ) {
          System.err.println( "Unable to call method " + guiElements.getListenerMethod() + " in class " + sourceData.getClass().getName() + " : " + ex.getMessage() );
          ex.printStackTrace( System.err );
        }
      } );

      menuItemMap.put( guiElements.getId(), menuItem );

    } else {
      // We have a bunch of children so we want to create a new drop-down menu in the parent menu
      //
      Menu menu = parentMenu;
      if ( guiElements.getId() != null ) {
        menuItem = new MenuItem( parentMenu, SWT.CASCADE );
        menuItem.setText( Const.NVL( guiElements.getLabel(), "" ) );
        setMenuItemKeyboardShortcut( menuItem, guiElements, sourceData.getClass().getName() );
        if ( StringUtils.isNotEmpty( guiElements.getToolTip() ) ) {
          menuItem.setToolTipText( guiElements.getToolTip() );
        }
        menu = new Menu( shell, SWT.DROP_DOWN );
        menuItem.setMenu( menu );
        menuItemMap.put( guiElements.getId(), menuItem );
      }

      // Add the children to this menu...
      //
      for ( GuiElements child : guiElements.getChildren() ) {
        addMenuWidgets( sourceData, shell, menu, child );
      }
    }
  }

  private void setMenuItemKeyboardShortcut( MenuItem menuItem, GuiElements guiElements, String parentClassName ) {

    // See if there's a shortcut worth mentioning...
    //
    KeyboardShortcut shortcut = GuiRegistry.getInstance().findKeyboardShortcut( parentClassName, guiElements.getListenerMethod(), Const.isOSX() );
    if ( shortcut != null ) {
      appendShortCut( menuItem, shortcut );
      menuItem.setAccelerator( getAccelerator( shortcut ) );
      shortcutMap.put( guiElements.getId(), shortcut );
    }
  }

  public static void appendShortCut( MenuItem menuItem, KeyboardShortcut shortcut ) {
    menuItem.setText( menuItem.getText() + "\t" + getShortcutString( shortcut ) );
  }

  public static int getAccelerator( KeyboardShortcut shortcut ) {
    int a = 0;
    a += shortcut.getKeyCode();
    if ( shortcut.isControl() ) {
      a += SWT.CONTROL;
    }
    if ( shortcut.isShift() ) {
      a += SWT.SHIFT;
    }
    if ( shortcut.isAlt() ) {
      a += SWT.ALT;
    }
    if ( shortcut.isCommand() ) {
      a += SWT.COMMAND;
    }
    return a;
  }

  public static String getShortcutString( KeyboardShortcut shortcut ) {
    String s = shortcut.toString();
    if ( StringUtils.isEmpty( s ) || s.endsWith( "-" ) ) {
      // Unknown characters from the SWT library
      // We'll handle the special cases here.
      //
      int keyCode = shortcut.getKeyCode();
      if ( keyCode == SWT.BS ) {
        return s + "Backspace";
      }
      if ( keyCode == SWT.ESC ) {
        return s + "Esc";
      }
      if ( keyCode == SWT.ARROW_LEFT ) {
        return s + "LEFT";
      }
      if ( keyCode == SWT.ARROW_RIGHT ) {
        return s + "RIGHT";
      }
      if ( keyCode == SWT.ARROW_UP ) {
        return s + "UP";
      }
      if ( keyCode == SWT.ARROW_DOWN ) {
        return s + "DOWN";
      }
      if ( keyCode == SWT.HOME ) {
        return s + "HOME";
      }
      if ( keyCode == SWT.F1 ) {
        return s + "F1";
      }
      if ( keyCode == SWT.F2 ) {
        return s + "F2";
      }
      if ( keyCode == SWT.F3 ) {
        return s + "F3";
      }
      if ( keyCode == SWT.F4 ) {
        return s + "F4";
      }
      if ( keyCode == SWT.F5 ) {
        return s + "F5";
      }
      if ( keyCode == SWT.F6 ) {
        return s + "F6";
      }
      if ( keyCode == SWT.F7 ) {
        return s + "F7";
      }
      if ( keyCode == SWT.F8 ) {
        return s + "F8";
      }
      if ( keyCode == SWT.F9 ) {
        return s + "F9";
      }
      if ( keyCode == SWT.F10 ) {
        return s + "F10";
      }
      if ( keyCode == SWT.F11 ) {
        return s + "F11";
      }
      if ( keyCode == SWT.F12 ) {
        return s + "F12";
      }
    }
    return s;
  }

  /**
   * Find the menu item with the given ID
   *
   * @param id The ID to look for
   * @return The menu item or null if nothing is found
   */
  public MenuItem findMenuItem( String id ) {
    return menuItemMap.get( id );
  }

  public KeyboardShortcut findKeyboardShortcut( String id ) {
    return shortcutMap.get( id );
  }


  /**
   * Find the menu item with the given ID.
   * If we find it we enable or disable it.
   *
   * @param id      The ID to look for
   * @param enabled true if the item needs to be enabled.
   * @return The menu item or null if nothing is found
   */
  public MenuItem enableMenuItem( String id, boolean enabled ) {
    MenuItem menuItem = menuItemMap.get( id );
    if ( menuItem == null ) {
      return null;
    }
    menuItem.setEnabled( enabled );
    return menuItem;
  }

  /**
   * Find the menu item with the given ID.
   * Check the capability in the given file type
   * Enable or disable accordingly.
   *
   * @param fileType
   * @param id         The ID of the widget to look for
   * @param permission
   * @return The menu item or null if nothing is found
   */
  public MenuItem enableMenuItem( IHopFileType fileType, String id, String permission ) {
    MenuItem menuItem = menuItemMap.get( id );
    if ( menuItem == null ) {
      return null;
    }
    boolean hasCapability = fileType.hasCapability( permission );
    menuItem.setEnabled( hasCapability );
    return menuItem;
  }

  /**
   * Gets space
   *
   * @return value of space
   */
  public IVariables getSpace() {
    return variables;
  }

  /**
   * @param variables The space to set
   */
  public void setSpace( IVariables variables ) {
    this.variables = variables;
  }

  /**
   * Gets menuItemMap
   *
   * @return value of menuItemMap
   */
  public Map<String, MenuItem> getMenuItemMap() {
    return menuItemMap;
  }

  /**
   * @param menuItemMap The menuItemMap to set
   */
  public void setMenuItemMap( Map<String, MenuItem> menuItemMap ) {
    this.menuItemMap = menuItemMap;
  }
}

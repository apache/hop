package org.apache.hop.ui.core.gui;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.ui.core.PropsUI;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Method;

/**
 * This class contains the widgets for Menu Bars
 */
public class GuiMenuWidgets {

  private VariableSpace space;

  public GuiMenuWidgets( VariableSpace space ) {
    this.space = space;
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
      menuItem = new MenuItem( parentMenu, SWT.PUSH );
      menuItem.setText( guiElements.getLabel() );
      if ( StringUtils.isNotEmpty( guiElements.getToolTip() ) ) {
        menuItem.setToolTipText( guiElements.getToolTip() );
      }

      // Call the method to which the GuiWidgetElement annotation belongs.
      //
      menuItem.addListener( SWT.Selection, e->{
        try {
          Method menuMethod = sourceData.getClass().getMethod( guiElements.getListenerMethod() );
          if (menuMethod==null) {
            throw new HopException( "Unable to find method "+guiElements.getListenerMethod()+" in class "+sourceData.getClass().getName() );
          }
          menuMethod.invoke( sourceData );
        } catch(Exception ex) {
          System.err.println( "Unable to call method "+guiElements.getListenerMethod()+" in class "+sourceData.getClass().getName()+" : "+ex.getMessage());
          ex.printStackTrace(System.err);
        }
      } );

    } else {
      // We have a bunch of children so we want to create a new drop-down menu in the parent menu
      //
      menuItem = new MenuItem( parentMenu, SWT.CASCADE );
      menuItem.setText( guiElements.getLabel() );
      if ( StringUtils.isNotEmpty( guiElements.getToolTip() ) ) {
        menuItem.setToolTipText( guiElements.getToolTip() );
      }
      Menu menu = new Menu( shell, SWT.DROP_DOWN );
      menuItem.setMenu( menu );

      // Add the children to this menu...
      //
      for ( GuiElements child : guiElements.getChildren() ) {
        addMenuWidgets( sourceData, shell, menu, child );
      }
    }
  }

  /**
   * Gets space
   *
   * @return value of space
   */
  public VariableSpace getSpace() {
    return space;
  }

  /**
   * @param space The space to set
   */
  public void setSpace( VariableSpace space ) {
    this.space = space;
  }
}

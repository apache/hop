package org.apache.hop.ui.core.gui;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the widgets for the GUI elements of a GUI Plugin
 */
public class GuiElementWidgets {

  private Map<String, Control> widgetsMap;

  public GuiElementWidgets() {
    widgetsMap = new HashMap<>(  );
  }

  public GuiElementWidgets( Map<String, Control> widgetsMap ) {
    this.widgetsMap = widgetsMap;
  }

  public void createWidgets( Object sourceData, Composite parent, String parentGuiElementId ) {
    // Find the GUI Elements for the given class...
    //
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements = registry.findGuiElements(sourceData.getClass().getName(), parentGuiElementId);
    if (guiElements==null) {
      return;
    }

    // Loop over the GUI elements, create and remember the widgets...
    //
    addWidgets(sourceData, parent, guiElements, null);

  }

  private Control addWidgets( Object sourceData, Composite parent, GuiElements guiElements, Control lastControl ) {

    VariableSpace space;
    if (sourceData instanceof VariableSpace ) {
      space = (VariableSpace) sourceData;
    } else {
      space = Variables.getADefaultVariableSpace();
    }
    PropsUI props = PropsUI.getInstance();

    // Do we add the element or the children?
    //
    if (guiElements.getId()!=null) {

      // Add the label on the left hand side...
      //
      Label label = new Label( parent, SWT.RIGHT | SWT.SINGLE );
      props.setLook( label );
      label.setText( Const.NVL(guiElements.getLabel(), "") );
      FormData fdLabel = new FormData(  );
      fdLabel.left = new FormAttachment( 0, 0 );
      if (lastControl==null) {
        fdLabel.top = new FormAttachment( 0, 0 );
      } else {
        fdLabel.top = new FormAttachment( lastControl, Const.MARGIN );
      }
      fdLabel.right = new FormAttachment( Const.MIDDLE_PCT, 0 );
      label.setLayoutData( fdLabel );

      // Add the GUI element
      //
      Control control = null;
      switch(guiElements.getType()) {
        case TEXT:
          if (guiElements.isVariablesEnabled()) {
            TextVar textVar = new TextVar( space, parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT );
            props.setLook( textVar );
            // TODO: get data from the sourceData using the getter
            control = textVar;
          } else {
            Text text = new Text( parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT );
            props.setLook( text );
            // TODO: get data from the sourceData using the getter
            control = text;
          }
          break;
        case MENU_ITEM:
          break;
        case TOOLBAR_ICON:
          break;
        default:
          break;
      }

      if (control!=null) {
        FormData fdControl = new FormData();
        fdControl.left = new FormAttachment( Const.MIDDLE_PCT, Const.MARGIN );
        fdControl.top = new FormAttachment( label, SWT.CENTER );
        fdControl.right = new FormAttachment( 100, 0 );
        control.setLayoutData( fdControl );
        return control;
      } else {
        return lastControl;
      }

    } else {
      // Add the children
      //
      Control previousControl = lastControl;
      for (GuiElements child : guiElements.getChildren()) {
        previousControl = addWidgets( sourceData, parent, child, previousControl );
      }
      return previousControl;
    }

  }



  /**
   * Gets widgetsMap
   *
   * @return value of widgetsMap
   */
  public Map<String, Control> getWidgetsMap() {
    return widgetsMap;
  }

  /**
   * @param widgetsMap The widgetsMap to set
   */
  public void setWidgetsMap( Map<String, Control> widgetsMap ) {
    this.widgetsMap = widgetsMap;
  }
}

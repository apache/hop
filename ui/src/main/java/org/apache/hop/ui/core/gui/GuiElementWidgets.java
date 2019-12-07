package org.apache.hop.ui.core.gui;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseInterface;
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

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the widgets for the GUI elements of a GUI Plugin
 */
public class GuiElementWidgets {

  private VariableSpace space;
  private Map<GuiElements, Control> widgetsMap;

  public GuiElementWidgets(VariableSpace space) {
    this.space = space;
    widgetsMap = new HashMap<>();
  }

  public GuiElementWidgets( VariableSpace space, Map<GuiElements, Control> widgetsMap ) {
    this(space);
    this.widgetsMap = widgetsMap;
  }

  public void createWidgets( Object sourceData, Composite parent, String parentGuiElementId, Control lastControl ) {
    // Find the GUI Elements for the given class...
    //
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements = registry.findGuiElements( sourceData.getClass().getName(), parentGuiElementId );
    if ( guiElements == null ) {
      System.out.println( "No GUI elements found for class: " + sourceData.getClass().getName() + ", parent ID: " + parentGuiElementId );
      return;
    }

    // Loop over the GUI elements, create and remember the widgets...
    //
    addWidgets( sourceData, parent, guiElements, lastControl );

    // Force re-layout
    //
    parent.layout( true, true );
  }

  private Control addWidgets( Object sourceData, Composite parent, GuiElements guiElements, Control lastControl ) {

    System.out.println( "addWidgets START" );

    if (guiElements.isIgnored()) {
      return lastControl;
    }

    PropsUI props = PropsUI.getInstance();

    // Do we add the element or the children?
    //
    if ( guiElements.getId() != null ) {

      System.out.println( "addWidgets Adding widget for id: " + guiElements.getId() );

      // Add the label on the left hand side...
      //
      Label label = new Label( parent, SWT.RIGHT | SWT.SINGLE );
      props.setLook( label );
      label.setText( Const.NVL( guiElements.getLabel(), "" ) );
      FormData fdLabel = new FormData();
      fdLabel.left = new FormAttachment( 0, 0 );
      if ( lastControl == null ) {
        fdLabel.top = new FormAttachment( 0, 0 );
      } else {
        fdLabel.top = new FormAttachment( lastControl, Const.MARGIN );
      }
      fdLabel.right = new FormAttachment( Const.MIDDLE_PCT, 0 );
      label.setLayoutData( fdLabel );

      // Add the GUI element
      //
      Control control = null;
      switch ( guiElements.getType() ) {
        case TEXT:
          if ( guiElements.isVariablesEnabled() ) {
            TextVar textVar = new TextVar( space, parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT );
            props.setLook( textVar );
            if (guiElements.isPassword()) {
              textVar.setEchoChar( '*' );
            }
            widgetsMap.put( guiElements, textVar );
            control = textVar;
          } else {
            Text text = new Text( parent, SWT.BORDER | SWT.SINGLE | SWT.LEFT );
            props.setLook( text );
            if (guiElements.isPassword()) {
              text.setEchoChar( '*' );
            }
            widgetsMap.put( guiElements, text );
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

      if ( control != null ) {
        FormData fdControl = new FormData();
        fdControl.left = new FormAttachment( Const.MIDDLE_PCT, Const.MARGIN );
        fdControl.right = new FormAttachment( 100, 0 );
        fdControl.top = new FormAttachment( label, 0, SWT.CENTER );
        control.setLayoutData( fdControl );
        return control;
      } else {
        return lastControl;
      }
    } else {
      System.out.println( "Adding " + guiElements.getChildren().size() + " children" );
      // Add the children
      //
      Control previousControl = lastControl;
      for ( GuiElements child : guiElements.getChildren() ) {
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
  public Map<GuiElements, Control> getWidgetsMap() {
    return widgetsMap;
  }

  /**
   * @param widgetsMap The widgetsMap to set
   */
  public void setWidgetsMap( Map<GuiElements, Control> widgetsMap ) {
    this.widgetsMap = widgetsMap;
  }

  public void setWidgetsContents( Object sourceData, Composite parentComposite, String parentGuiElementId ) {

    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements = registry.findGuiElements( sourceData.getClass().getName(), parentGuiElementId );
    if ( guiElements == null ) {
      System.out.println( "No GUI elements found for class: " + sourceData.getClass().getName() + ", parent ID: " + parentGuiElementId );
      return;
    }

    setWidgetsData( sourceData, guiElements );

    parentComposite.layout( true, true );
  }

  private void setWidgetsData( Object sourceData, GuiElements guiElements ) {

    System.out.println( "setWidgetsData START" );

    if (guiElements.isIgnored()) {
      return;
    }

    // Do we add the element or the children?
    //
    if ( guiElements.getId() != null ) {

      System.out.println( "setting value on widget with id: " + guiElements.getId() );

      Control control = widgetsMap.get( guiElements );
      if ( control != null ) {

        // What's the value?
        //
        Object value = null;
        try {
          value = new PropertyDescriptor( guiElements.getFieldName(), sourceData.getClass() )
            .getReadMethod()
            .invoke( sourceData )
          ;
        } catch ( Exception e ) {
          System.out.println( "Unable to get value for field: '" + guiElements.getFieldName() + "' : " + e.getMessage() );
          e.printStackTrace();
        }
        String stringValue = value == null ? "" : Const.NVL( value.toString(), "" );

        switch ( guiElements.getType() ) {
          case TEXT:
            if ( guiElements.isVariablesEnabled() ) {
              TextVar textVar = (TextVar) control;
              textVar.setText( stringValue );
            } else {
              Text text = (Text) control;
              text.setText( stringValue );
            }
            break;
          case COMBO:
          case CHECKBOX:
          default:
            System.out.println( "WARNING: setting data on widget with ID " + guiElements.getId() + " : not implemented type " + guiElements.getType() + " yet." );
            break;
        }

      } else {
        System.out.println( "Widget not found to set value on for id: " + guiElements );
      }
    } else {

      System.out.println( "Adding " + guiElements.getChildren().size() + " children" );
      // Add the children
      //
      for ( GuiElements child : guiElements.getChildren() ) {
        setWidgetsData( sourceData, child );
      }
    }
  }

  public void getWidgetsContents( Object sourceData, String parentGuiElementId ) {
    GuiRegistry registry = GuiRegistry.getInstance();
    GuiElements guiElements = registry.findGuiElements( sourceData.getClass().getName(), parentGuiElementId );
    if ( guiElements == null ) {
      System.out.println( "No GUI elements found for class: " + sourceData.getClass().getName() + ", parent ID: " + parentGuiElementId );
      return;
    }

    getWidgetsData(sourceData, guiElements);
  }

  private void getWidgetsData( Object sourceData, GuiElements guiElements ) {
    if (guiElements.isIgnored()) {
      return;
    }

    // Do we add the element or the children?
    //
    if ( guiElements.getId() != null ) {

      System.out.println( "setting value on widget with id: " + guiElements.getId() );

      Control control = widgetsMap.get( guiElements );
      if ( control != null ) {

        // What's the value?
        //
        Object value = null;

        switch ( guiElements.getType() ) {
          case TEXT:
            if ( guiElements.isVariablesEnabled() ) {
              TextVar textVar = (TextVar) control;
              value = textVar.getText();
            } else {
              Text text = (Text) control;
              value = text.getText();
            }
            break;
          case COMBO:
          case CHECKBOX:
          default:
            System.out.println( "WARNING: getting data from widget with ID " + guiElements.getId() + " : not implemented type " + guiElements.getType() + " yet." );
            break;
        }

        // Set the value on the source data object
        //
        try {
          new PropertyDescriptor( guiElements.getFieldName(), sourceData.getClass() )
            .getWriteMethod()
            .invoke( sourceData, value )
          ;
        } catch ( Exception e ) {
          System.out.println( "Unable to set value '"+value+"'on field: '" + guiElements.getFieldName() + "' : " + e.getMessage() );
          e.printStackTrace();
        }

      } else {
        System.out.println( "Widget not found to set value on for id: " + guiElements );
      }
    } else {

      System.out.println( "Adding " + guiElements.getChildren().size() + " children" );
      // Add the children
      //
      for ( GuiElements child : guiElements.getChildren() ) {
        getWidgetsData( sourceData, child );
      }
    }
  }
}

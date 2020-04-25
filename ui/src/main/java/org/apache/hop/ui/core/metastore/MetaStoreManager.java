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

package org.apache.hop.ui.core.metastore;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.metastore.HopMetaStoreGuiPluginDetails;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.dialog.IMetaStoreDialog;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.dialog.MetaStoreExplorerDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This is a utility class which allows you to create or edit metastore elements in a generic fashion
 *
 * @author Matt
 * @since 2020-01-21
 */
public class MetaStoreManager<T extends IHopMetaStoreElement> {

  private IMetaStore metaStore;
  private IVariables variables;
  private ClassLoader classLoader;

  private Class<T> managedClass;
  private PropsUi props;

  public MetaStoreManager( IVariables variables, IMetaStore metaStore, Class<T> managedClass ) {
    this.variables = variables;
    this.classLoader = managedClass.getClassLoader();
    this.metaStore = metaStore;
    this.managedClass = managedClass;
    this.props = PropsUi.getInstance();
  }

  public void openMetaStoreExplorer() {
    MetaStoreExplorerDialog dialog = new MetaStoreExplorerDialog( HopGui.getInstance().getShell(), metaStore );
    dialog.open();
  }

  /**
   * edit an element
   *
   * @return True if anything was changed
   */
  public boolean editMetadata() {
    HopGui hopGui = HopGui.getInstance();
    try {
      List<String> names = getNames();

      // Is there a GuiPlugin available for T?
      //
      HopMetaStoreGuiPluginDetails details = GuiRegistry.getInstance().getMetaStoreTypeMap().get( managedClass );
      if (details!=null) {
        // Show an action dialog...
        //
        List<GuiAction> actions = new ArrayList<>(  );
        for (final String name : names) {
          GuiAction action = new GuiAction( name, GuiActionType.Modify, name, name + " : " + details.getDescription(), details.getIconImage(),
            ( shiftAction, controlAction, t ) -> editMetadata( name ) );
          action.setClassLoader( getClassLoader() );
          actions.add(action);
        }
        return GuiContextUtil.handleActionSelection( hopGui.getShell(), "Select the "+details.getName()+" to edit", actions );
      } else {
        // The regular selection dialog
        //
        EnterSelectionDialog dialog = new EnterSelectionDialog( hopGui.getShell(), names.toArray( new String[ 0 ] ), "Select the element", "Select the element to edit" );
        String name = dialog.open();
        if ( name != null ) {
          return editMetadata( name );
        }
        return false;
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error editing metadata", e );
      return false;
    }
  }

  /**
   * delete an element
   *
   * @return True if anything was changed
   */
  public boolean deleteMetadata() {
    HopGui hopGui = HopGui.getInstance();
    try {
      List<String> names = getNames();

      // Is there a GuiPlugin available for T?
      //
      HopMetaStoreGuiPluginDetails details = GuiRegistry.getInstance().getMetaStoreTypeMap().get( managedClass );
      if (details!=null) {
        // Show an action dialog...
        //
        List<GuiAction> actions = new ArrayList<>();
        for ( final String name : names ) {
          GuiAction action = new GuiAction( name, GuiActionType.Delete, name, name + " : " + details.getDescription(), details.getIconImage(),
            ( shiftAction, controlAction, t ) -> deleteMetadata( name ) );
          action.setClassLoader( getClassLoader() );
          actions.add( action );
        }
        return GuiContextUtil.handleActionSelection( hopGui.getShell(), "Select the " + details.getName() + " to delete after confirmation", actions );
      } else {
        // Good old selection dialog
        //
        EnterSelectionDialog dialog = new EnterSelectionDialog( hopGui.getShell(), names.toArray( new String[ 0 ] ), "Delete an element", "Select the element to delete" );
        String name = dialog.open();
        if ( name != null ) {
          return deleteMetadata( name );
        }
        return false;
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error deleting metadata", e );
      return false;
    }
  }

  /**
   * We look at the managed class name, add Dialog to it and then simply us that class to edit the dialog.
   *
   * @param elementName The name of the element to edit
   * @return True if anything was changed
   */
  public boolean editMetadata( String elementName ) {

    if ( StringUtils.isEmpty( elementName ) ) {
      return false;
    }

    try {
      MetaStoreFactory<T> factory = getFactory();

      // Load the metadata element from the metastore
      //
      T element = factory.loadElement( elementName );
      if ( element == null ) {
        // Something removed or renamed the element in the background
        //
        throw new HopException( "Unable to find element '" + elementName + "' in the metastore" );
      }

      initializeElementVariables(element);

      return openMetaDialog( element, factory );

    } catch ( Exception e ) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error editing metadata", e );
      return false;
    }
  }

  private void initializeElementVariables( T element ) {
    if (element instanceof IVariables) {
      ((IVariables)element).initializeVariablesFrom( variables );
    }
  }

  /**
   * delete an element
   *
   * @param elementName The name of the element to delete
   * @return True if anything was deleted
   */
  public boolean deleteMetadata( String elementName ) {

    if ( StringUtils.isEmpty( elementName ) ) {
      return false;
    }

    MessageBox confirmBox = new MessageBox( HopGui.getInstance().getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO );
    confirmBox.setText( "Delete?" );
    confirmBox.setMessage( "Are you sure you want to delete element " + elementName + "?" );
    int anwser = confirmBox.open();
    if ((anwser&SWT.YES)==0) {
      return false;
    }

    try {
      MetaStoreFactory<T> factory = getFactory();

      // delete the metadata element from the metastore
      //
      T element = factory.deleteElement( elementName );

      // Just to be precise.
      //
      initializeElementVariables( element );

      ExtensionPointHandler.callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopGuiMetaStoreElementDeleted.id, element );

      return true;

    } catch ( Exception e ) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error deleting metadata element "+elementName, e );
      return false;
    }
  }

  public MetaStoreFactory<T> getFactory() throws IllegalAccessException, InstantiationException {
    // T implements getFactory so let's create a new empty instance and get the factory...
    //
    return managedClass.newInstance().getFactory( metaStore );
  }

  public boolean openMetaDialog( T element, MetaStoreFactory<T> factory ) throws Exception {
    if ( element == null ) {
      return false;
    }
    HopGui hopGui = HopGui.getInstance();

    String dialogClassName = calculateDialogClassname();

    // Create the dialog class editor...
    // Always pass the shell, the metastore and the object to edit...
    //
    Class<?>[] constructorArguments = new Class<?>[] {
      Shell.class,
      IMetaStore.class,
      managedClass
    };
    Object[] constructorParameters = new Object[] {
      hopGui.getShell(), metaStore, element
    };

    Class<IMetaStoreDialog> dialogClass;
    try {
      dialogClass = (Class<IMetaStoreDialog>) classLoader.loadClass( dialogClassName );
    } catch ( ClassNotFoundException e1 ) {
      dialogClass = (Class<IMetaStoreDialog>) Class.forName( dialogClassName );
    }
    Constructor<IMetaStoreDialog> constructor;
    try {
      constructor = dialogClass.getDeclaredConstructor( constructorArguments );
    } catch ( NoSuchMethodException nsm ) {
      constructorArguments = new Class<?>[] {
        Shell.class,
        IMetaStore.class,
        managedClass,
        IVariables.class
      };
      constructorParameters = new Object[] {
        hopGui.getShell(), metaStore, element, hopGui.getVariables()
      };
      constructor = dialogClass.getDeclaredConstructor( constructorArguments );
    }
    if ( constructor == null ) {
      throw new HopException( "Unable to find dialog class (" + dialogClassName + ") constructor with arguments: Shell, IMetaStore, T and optionally IVariables" );
    }

    IMetaStoreDialog dialog = constructor.newInstance( constructorParameters );
    String name = dialog.open();
    if ( name != null ) {
      // Save it in the MetaStore
      factory.saveElement( element );

      ExtensionPointHandler.callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopGuiMetaStoreElementUpdated.id, element );

      return true;
    } else {
      return false;
    }
  }

  public T newMetadata() {
    try {
      // Create a new instance of the managed class
      //
      T element = managedClass.newInstance();
      initializeElementVariables( element );
      boolean created = openMetaDialog( element, element.getFactory( metaStore ) );
      if (created) {
        ExtensionPointHandler.callExtensionPoint( HopGui.getInstance().getLog(), HopExtensionPoint.HopGuiMetaStoreElementCreated.id, element );
      }
      return element;
    } catch ( Exception e ) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error creating new metadata element", e );
      return null;
    }
  }

  public List<String> getNames() throws HopException {
    try {
      List<String> names = getFactory().getElementNames();
      Collections.sort( names );
      return names;

    } catch ( Exception e ) {
      throw new HopException( "Unable to get list of element names in the MetaStore for class " + managedClass.getName(), e );
    }
  }

  public String[] getNamesArray() throws HopException {
    try {
      return getNames().toArray( new String[ 0 ] );
    } catch ( Exception e ) {
      throw new HopException( "Unable to get element names array in the MetaStore for class " + managedClass.getName(), e );
    }
  }

  public String calculateDialogClassname() {
    String dialogClassName;
    MetaStoreElementType elementType = managedClass.getAnnotation( MetaStoreElementType.class );
    if ( elementType != null && StringUtils.isNotEmpty( elementType.dialogClassname() ) ) {
      dialogClassName = elementType.dialogClassname();
    } else {
      dialogClassName = managedClass.getName();
      dialogClassName = dialogClassName.replaceFirst( "\\.hop\\.", ".hop.ui." );
      dialogClassName += "Dialog";
    }
    return dialogClassName;
  }

  /**
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public IMetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * @param metaStore The metaStore to set
   */
  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
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
   * Gets classLoader
   *
   * @return value of classLoader
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * @param classLoader The classLoader to set
   */
  public void setClassLoader( ClassLoader classLoader ) {
    this.classLoader = classLoader;
  }

  /**
   * Gets managedClass
   *
   * @return value of managedClass
   */
  public Class<T> getManagedClass() {
    return managedClass;
  }

  /**
   * @param managedClass The managedClass to set
   */
  public void setManagedClass( Class<T> managedClass ) {
    this.managedClass = managedClass;
  }

  /**
   * Gets props
   *
   * @return value of props
   */
  public PropsUi getProps() {
    return props;
  }

  /**
   * @param props The props to set
   */
  public void setProps( PropsUi props ) {
    this.props = props;
  }

}

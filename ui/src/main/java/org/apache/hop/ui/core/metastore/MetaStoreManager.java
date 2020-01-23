/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.dialog.IMetaStoreDialog;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.hopui.dialog.MetaStoreExplorerDialog;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

/**
 * This is a utility class which allows you to create or edit metastore elements in a generic fashion
 *
 * @author Matt
 * @since 2020-01-21
 */
public class MetaStoreManager<T extends IHopMetaStoreElement>  {

  private IMetaStore metaStore;
  private VariableSpace space;
  private ClassLoader classLoader;

  private Class<T> managedClass;
  private Shell parent;
  private PropsUI props;

  public MetaStoreManager( VariableSpace space, IMetaStore metaStore, Class<T> managedClass, Shell parent) {
    this.parent = parent;
    this.space = space;
    this.classLoader = managedClass.getClassLoader();
    this.metaStore = metaStore;
    this.managedClass = managedClass;
    this.props = PropsUI.getInstance();
  }

  public void openMetaStoreExplorer() {
    MetaStoreExplorerDialog dialog = new MetaStoreExplorerDialog( parent, metaStore );
    dialog.open();
  }

  /**
   * We look at the managed class name, add Dialog to it and then simply us that class to edit the dialog.
   *
   * @param elementName The name of the element to edit
   * @return True if anything was changed
   */
  public boolean editMetadata(String elementName) {

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

      return openMetaDialog( element, factory );

    } catch ( Exception e ) {
      new ErrorDialog( parent, "Error", "Error editing metadata", e );
      return false;
    }
  }

  public MetaStoreFactory<T> getFactory() throws IllegalAccessException, InstantiationException {
    // T implements getFactory so let's create a new empty instance and get the factory...
    //
    return managedClass.newInstance().getFactory( metaStore );
  }

  public boolean openMetaDialog( T element, MetaStoreFactory<T> factory ) throws Exception {
    if (element==null) {
      return false;
    }

    String dialogClassName = calculateDialogClassname();

    // Create the dialog class editor...
    // Always pass the shell, the metastore and the object to edit...
    //
    Class<?>[] constructorArguments = new Class<?>[] {
      Shell.class,
      IMetaStore.class,
      managedClass
    };

    Class<IMetaStoreDialog> dialogClass;
    try {
      dialogClass = (Class<IMetaStoreDialog>) classLoader.loadClass( dialogClassName );
    } catch ( ClassNotFoundException e1 ) {
      dialogClass = (Class<IMetaStoreDialog>) Class.forName( dialogClassName );
    }
    Constructor<IMetaStoreDialog> constructor = dialogClass.getDeclaredConstructor( constructorArguments );
    IMetaStoreDialog dialog = constructor.newInstance( parent, metaStore, element );
    String name = dialog.open();
    if ( name != null ) {
      // Save it in the MetaStore
      factory.saveElement( element );

      return true;
    } else {
      return false;
    }
  }

  public boolean newMetadata() {
    try {
      // Create a new instance of the managed class
      //
      T element = managedClass.newInstance();
      return openMetaDialog( element, element.getFactory( metaStore ) );
    } catch ( Exception e ) {
      new ErrorDialog( parent, "Error", "Error creating new metadata element", e );
      return false;
    }
  }

  public List<String> getNames() throws HopException {
    try {
      List<String> names = getFactory().getElementNames();
      Collections.sort( names );
      return names;

    } catch(Exception e ) {
      throw new HopException( "Unable to get list of element names in the MetaStore for class "+managedClass.getName(), e );
    }
  }

  public String[] getNamesArray() throws HopException {
    try {
      return getNames().toArray( new String[0] );
    } catch(Exception e ) {
      throw new HopException( "Unable to get element names array in the MetaStore for class "+managedClass.getName(), e );
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
  public VariableSpace getSpace() {
    return space;
  }

  /**
   * @param space The space to set
   */
  public void setSpace( VariableSpace space ) {
    this.space = space;
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
  public PropsUI getProps() {
    return props;
  }

  /**
   * @param props The props to set
   */
  public void setProps( PropsUI props ) {
    this.props = props;
  }

  /**
   * Gets parent
   *
   * @return value of parent
   */
  public Shell getParent() {
    return parent;
  }

  /**
   * @param parent The parent to set
   */
  public void setParent( Shell parent ) {
    this.parent = parent;
  }
}

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

package org.apache.hop.ui.core.widget;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Props;
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
 * The goal of this composite is to add a line on a dialog which contains:
 * - A label (for example: Database connection)
 * - A Combo Variable selection (editable ComboBox, for example containing all connection values in the MetaStore)
 * - New, Edit and Manage buttons (The latter opens up a generic MetaStore editor)
 *
 * @author Matt
 * @since 2019-12-17
 */
public class MetaSelectionManager<T extends IHopMetaStoreElement> extends Composite {
  private static final Class<?> PKG = MetaSelectionManager.class; // i18n
  private final Button wManage;
  private final Button wNew;
  private final Button wEdit;

  private IMetaStore metaStore;
  private VariableSpace space;
  private ClassLoader classLoader;

  private Class<T> managedClass;
  private Composite parentComposite;
  private PropsUI props;
  private Label wLabel;
  private ComboVar wCombo;

  public MetaSelectionManager( VariableSpace space, IMetaStore metaStore, Class<T> managedClass, Composite parentComposite, int flags, String labelText, String toolTipText ) {
    super( parentComposite, SWT.NONE );
    this.space = space;
    this.classLoader = managedClass.getClassLoader();
    this.metaStore = metaStore;
    this.managedClass = managedClass;
    this.parentComposite = parentComposite;
    this.props = PropsUI.getInstance();

    props.setLook( this );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout( formLayout );

    wLabel = new Label( this, SWT.RIGHT );
    props.setLook( wLabel );
    wLabel.setText( labelText );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 0, 0 );
    fdLabel.right = new FormAttachment( middle, 0 );
    fdLabel.top = new FormAttachment( 0, 0 );
    wLabel.setLayoutData( fdLabel );
    wLabel.setToolTipText( toolTipText );

    wManage = new Button( this, SWT.PUSH );
    wManage.setText( BaseMessages.getString( PKG, "System.Button.Manage" ) );
    FormData fdManage = new FormData();
    fdManage.right = new FormAttachment( 100, 0 );
    fdManage.top = new FormAttachment( wLabel, 0, SWT.CENTER );
    wManage.setLayoutData( fdManage );
    wManage.addListener( SWT.Selection, e -> manageMetadata() );

    wNew = new Button( this, SWT.PUSH );
    wNew.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNew = new FormData();
    fdNew.right = new FormAttachment( wManage, -margin );
    fdNew.top = new FormAttachment( wLabel, 0, SWT.CENTER );
    wNew.setLayoutData( fdNew );
    wNew.addListener( SWT.Selection, e -> newMetadata() );

    wEdit = new Button( this, SWT.PUSH );
    wEdit.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEdit = new FormData();
    fdEdit.right = new FormAttachment( wNew, -margin );
    fdEdit.top = new FormAttachment( wLabel, 0, SWT.CENTER );
    wEdit.setLayoutData( fdEdit );
    wEdit.addListener( SWT.Selection, e -> editMetadata() );

    int textFlags = SWT.SINGLE | SWT.LEFT | SWT.BORDER;
    if ( flags != SWT.NONE ) {
      textFlags = flags;
    }
    wCombo = new ComboVar( this.space, this, textFlags, toolTipText );
    FormData fdText = new FormData();
    fdText.left = new FormAttachment( middle, margin );
    fdText.right = new FormAttachment( wEdit, -margin );
    fdText.top = new FormAttachment( wLabel, 0, SWT.CENTER );
    wCombo.setLayoutData( fdText );
    wCombo.getCComboWidget().setToolTipText( toolTipText );
  }


  protected void manageMetadata() {
    MetaStoreExplorerDialog dialog = new MetaStoreExplorerDialog( parentComposite.getShell(), metaStore );
    dialog.open();
  }

  /**
   * We look at the managed class name, add Dialog to it and then simply us that class to edit the dialog.
   */
  protected void editMetadata() {

    String selected = wCombo.getText();
    if ( StringUtils.isEmpty( selected ) ) {
      return;
    }

    try {
      MetaStoreFactory<T> factory = getFactory();

      // Load the metadata element from the metastore
      //
      T element = factory.loadElement( selected );
      if ( element == null ) {
        // Something removed or renamed the element in the background
        //
        throw new HopException( "Unable to find element '" + selected + "' in the metastore" );
      }

      openMetaDialog( element, factory );

    } catch ( Exception e ) {
      new ErrorDialog( getShell(), "Error", "Error editing metadata", e );
    }
  }

  private MetaStoreFactory<T> getFactory() throws IllegalAccessException, InstantiationException {
    // T implements getFactory so let's create a new empty instance and get the factory...
    //
    return managedClass.newInstance().getFactory( metaStore );
  }

  private void openMetaDialog( T element, MetaStoreFactory<T> factory ) throws Exception {
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
    IMetaStoreDialog dialog = constructor.newInstance( getShell(), metaStore, element );
    String name = dialog.open();
    if ( name != null ) {
      // Save it in the MetaStore
      factory.saveElement( element );

      fillItems();
      wCombo.setText( name );
    }
  }

  private void newMetadata() {
    try {
      // Create a new instance of the managed class
      //
      T element = managedClass.newInstance();
      openMetaDialog( element, element.getFactory( metaStore ) );
    } catch ( Exception e ) {
      new ErrorDialog( getShell(), "Error", "Error creating new metadata element", e );
    }
  }

  private String calculateDialogClassname() {
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
   * Look up the element names from the metastore and populate the items in the combobox with it.
   *
   * @throws MetaStoreException In case something went horribly wrong.
   */
  public void fillItems() throws MetaStoreException, InstantiationException, IllegalAccessException {
    MetaStoreFactory<T> factory = getFactory();
    List<String> elementNames = factory.getElementNames();
    Collections.sort( elementNames );
    wCombo.setItems( elementNames.toArray( new String[ 0 ] ) );
  }

  /**
   * Adds the connection line for the given parent and previous control, and returns a meta selection manager control
   *
   * @param parent   the parent composite object
   * @param previous the previous control
   * @param
   * @return the combo box UI component
   */
  public void addToConnectionLine( Composite parent, Control previous, T selected, ModifyListener lsMod ) {

    try {
      fillItems();
    } catch ( Exception e ) {
      LogChannel.UI.logError( "Error getting list of relational database connection names from the metastore", e );
    }
    addModifyListener( lsMod );

    // Set a default value if there is only 1 connection in the list and nothing else is previously selected...
    //
    if ( selected == null ) {
      if ( getItemCount() == 1 ) {
        select( 0 );
      }
    } else {
      // Just set the value
      //
      setText( Const.NVL( selected.getName(), "" ) );
    }

    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( 0, 0 );
    fdConnection.right = new FormAttachment( 100, 0 );
    if ( previous != null ) {
      fdConnection.top = new FormAttachment( previous, Const.MARGIN );
    } else {
      fdConnection.top = new FormAttachment( 0, 0 );
    }
    setLayoutData( fdConnection );
  }


  public void addModifyListener( ModifyListener lsMod ) {
    wCombo.addModifyListener( lsMod );
  }

  public void addSelectionListener( SelectionListener lsDef ) {
    wCombo.addSelectionListener( lsDef );
  }

  public void setText( String name ) {
    wCombo.setText( name );
  }

  public String getText() {
    return wCombo.getText();
  }

  public void setItems( String[] items ) {
    wCombo.setItems( items );
  }

  public void add( String item ) {
    wCombo.add( item );
  }

  public String[] getItems() {
    return wCombo.getItems();
  }

  public int getItemCount() {
    return wCombo.getItemCount();
  }

  public void removeAll() {
    wCombo.removeAll();
  }

  public void remove( int index ) {
    wCombo.remove( index );
  }

  public void select( int index ) {
    wCombo.select( index );
  }

  public int getSelectionIndex() {
    return wCombo.getSelectionIndex();
  }

  public void setEnabled( boolean flag ) {
    wCombo.setEnabled( flag );
    wLabel.setEnabled( flag );
    wManage.setEnabled( flag );
    wNew.setEnabled( flag );
    wEdit.setEnabled( flag );
  }

  public boolean setFocus() {
    return wCombo.setFocus();
  }

  public void addTraverseListener( TraverseListener tl ) {
    wCombo.addTraverseListener( tl );
  }

  public CCombo getComboWidget() {
    return wCombo.getCComboWidget();
  }

  public Label getLabelWidget() {
    return wLabel;
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
   * Gets parentComposite
   *
   * @return value of parentComposite
   */
  public Composite getParentComposite() {
    return parentComposite;
  }

  /**
   * @param parentComposite The parentComposite to set
   */
  public void setParentComposite( Composite parentComposite ) {
    this.parentComposite = parentComposite;
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

  public static void main( String[] args ) throws Exception {
    HopClientEnvironment.init();
    Display display = new Display();
    PropsUI.init( display );
    HopEnvironment.init();
    IMetaStore metaStore = buildTestMetaStore();

    Shell shell = new Shell( display, SWT.MIN | SWT.MAX | SWT.RESIZE | SWT.CLOSE );
    shell.setText( "MetaSelectionManager" );
    FormLayout shellLayout = new FormLayout();
    shellLayout.marginTop = 5;
    shellLayout.marginBottom = 5;
    shellLayout.marginLeft = 5;
    shellLayout.marginRight = 5;
    shell.setLayout( shellLayout );

    MetaSelectionManager<DatabaseMeta> wConnection = new MetaSelectionManager<DatabaseMeta>(
      Variables.getADefaultVariableSpace(),
      metaStore,
      DatabaseMeta.class,
      shell, SWT.NONE,
      "Database connection",
      "Select the database connection to use."
    );
    wConnection.fillItems();

    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( 0, 0 );
    fdConnection.top = new FormAttachment( 0, 0 );
    fdConnection.right = new FormAttachment( 100, 0 );
    wConnection.setLayoutData( fdConnection );

    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( "Owkeej" );
    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK }, Const.MARGIN, wConnection );

    BaseStepDialog.setSize( shell );

    shell.open();

    while ( shell != null && !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    display.dispose();
  }

  private static IMetaStore buildTestMetaStore() throws MetaStoreException {
    MemoryMetaStore metaStore = new MemoryMetaStore();
    MetaStoreFactory<DatabaseMeta> dbFactory = DatabaseMeta.createFactory( metaStore );

    DatabaseMeta one = new DatabaseMeta();
    one.setName( "One" );
    one.setDatabaseType( "MYSQL" );
    one.setHostname( "${HOSTNAME1}" );
    one.setPort( "${PORT1}" );
    one.setDBName( "${DB1}" );
    one.setUsername( "${USERNAME1}" );
    one.setPassword( "${PASSWORD1}" );
    dbFactory.saveElement( one );

    DatabaseMeta two = new DatabaseMeta();
    two.setName( "Two" );
    two.setDatabaseType( "ORACLE" );
    two.setHostname( "${HOSTNAME2}" );
    two.setPort( "${PORT2}" );
    two.setDBName( "${DB2}" );
    two.setUsername( "${USERNAME2}" );
    two.setPassword( "${PASSWORD2}" );
    dbFactory.saveElement( two );

    return metaStore;
  }
}

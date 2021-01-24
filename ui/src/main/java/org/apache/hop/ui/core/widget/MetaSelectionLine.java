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

package org.apache.hop.ui.core.widget;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.metadata.MetadataContextHandler;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/**
 * The goal of this composite is to add a line on a dialog which contains:
 * - A label (for example: Database connection)
 * - A Combo Variable selection (editable ComboBox, for example containing all connection values in the MetaStore)
 * - New and Edit buttons (The latter opens up a generic Metadata editor)
 *
 * @author Matt
 * @since 2019-12-17
 */
public class MetaSelectionLine<T extends IHopMetadata> extends Composite {
  private static final Class<?> PKG = MetaSelectionLine.class; // For Translator

  private IHopMetadataProvider metadataProvider;
  private IVariables variables;
  private MetadataManager<T> manager;

  private Class<T> managedClass;
  private PropsUi props;
  private final Label wLabel;
  private final ComboVar wCombo;
  private final ToolBar wToolBar;

  public MetaSelectionLine( IVariables variables, IHopMetadataProvider metadataProvider, Class<T> managedClass, Composite parentComposite, int flags, String labelText, String toolTipText ) {
    this(variables, metadataProvider, managedClass, parentComposite, flags, labelText, toolTipText, false);
  }

  public MetaSelectionLine( IVariables variables, IHopMetadataProvider metadataProvider, Class<T> managedClass, Composite parentComposite, int flags, String labelText, String toolTipText, boolean leftAlignedLabel ) {
    super( parentComposite, SWT.NONE );
    this.variables = variables;
   // this.classLoader = managedClass.getClassLoader();
    this.metadataProvider = metadataProvider;
    this.managedClass = managedClass;
    this.props = PropsUi.getInstance();  

    this.manager = new MetadataManager<>( variables, metadataProvider, managedClass );

    props.setLook( this );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginLeft = 0;
    formLayout.marginRight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;
    this.setLayout( formLayout );

    int labelFlags;
    if (leftAlignedLabel) {
      labelFlags = SWT.NONE | SWT.SINGLE;
    } else {
      labelFlags = SWT.RIGHT | SWT.SINGLE;
    }
    wLabel = new Label( this, labelFlags );
    props.setLook( wLabel );
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment( 0, 0 );
    if (!leftAlignedLabel) {
      fdLabel.right = new FormAttachment( middle, -margin );
    }
    fdLabel.top = new FormAttachment( 0, margin );
    wLabel.setLayoutData( fdLabel );
    wLabel.setText( labelText );
    wLabel.setToolTipText( toolTipText );
    wLabel.requestLayout(); // Avoid GTK error in log

    // Toolbar for default actions
    //
    HopMetadata metadata = HopMetadataUtil.getHopMetadataAnnotation( managedClass );
    Image image = SwtSvgImageUtil.getImage( getDisplay(), managedClass.getClassLoader(), metadata.image(), ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
         
    wToolBar = new ToolBar(this, SWT.FLAT | SWT.HORIZONTAL );    
    FormData fdToolBar = new FormData();
    fdToolBar.right = new FormAttachment( 100, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    wToolBar.setLayoutData( fdToolBar );
    props.setLook( wToolBar );
    
    ToolItem item = new ToolItem(wToolBar, SWT.DROP_DOWN);
    item.setImage(image);
    
    int textFlags = SWT.SINGLE | SWT.LEFT | SWT.BORDER;
    if ( flags != SWT.NONE ) {
      textFlags = flags;
    }
    wCombo = new ComboVar( this.variables, this, textFlags, toolTipText );
    FormData fdCombo = new FormData();
    if (leftAlignedLabel) {
      fdCombo.left = new FormAttachment( wLabel, margin, SWT.RIGHT );
    } else {
      fdCombo.left = new FormAttachment( middle, 0 );
    }
    fdCombo.right = new FormAttachment( wToolBar, -margin );
    fdCombo.top = new FormAttachment( wLabel, 0, SWT.CENTER );
    wCombo.setLayoutData( fdCombo );
    wCombo.setToolTipText( toolTipText );

    // Menu for gui actions
    //
    final Menu menu = new Menu (getShell(), SWT.POP_UP);
    
    MenuItem itemNew = new MenuItem (menu, SWT.PUSH);
    itemNew.setText( BaseMessages.getString( PKG, "System.Button.New" ) );   
    itemNew.addListener( SWT.Selection, e -> newMetadata() );    
    final MenuItem itemEdit = new MenuItem (menu, SWT.PUSH);
    itemEdit.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    itemEdit.addListener( SWT.Selection, e -> editMetadata() );
        
    
    MetadataContextHandler contextHandler = new MetadataContextHandler(HopGui.getInstance(), metadataProvider, managedClass);

    // Filter custom action
    //
    List<GuiAction> actions = new ArrayList<>();
    for (GuiAction action : contextHandler.getSupportedActions()) {
      if (action.getType() == GuiActionType.Custom) {
        actions.add(action);
      }
    }

    
    if (!actions.isEmpty()) {
      new MenuItem(menu, SWT.SEPARATOR);
      // Add custom action
      for (GuiAction action : actions) {
        if (action.getType() == GuiActionType.Custom) {
          MenuItem menuItem = new MenuItem(menu, SWT.PUSH);
          menuItem.setText(action.getShortName());
          menuItem.setToolTipText(action.getTooltip());
          menuItem.addListener(
              SWT.Selection,
              e -> {
                IGuiActionLambda actionLambda = action.getActionLambda();
                actionLambda.executeAction(false,false,wCombo.getText());
              });
        }
      }
    }

    props.setLook( wCombo );
   
    item.addListener(SWT.Selection,(event) -> {
        if (event.detail == SWT.ARROW) {
           Rectangle rect = item.getBounds ();
           Point pt = new Point (rect.x, rect.y + rect.height);
           pt = wToolBar.toDisplay (pt);
           menu.setLocation (pt.x, pt.y);
           menu.setVisible (true);
        }
        else {
     	   if ( Utils.isEmpty(wCombo.getText()) ) this.newMetadata();
     	   else this.editMetadata();
        }
     });
    
    layout( true, true );
  }


  protected void manageMetadata() {
   // manager.openMetaStoreExplorer();
  }

  /**
   * We look at the managed class name, add Dialog to it and then simply us that class to edit the dialog.
   */
  protected boolean editMetadata() {
    String selected = wCombo.getText();
    if ( StringUtils.isEmpty( selected ) ) {
      return false;
    }

    return manager.editMetadata( selected );
  }

  private T newMetadata() {
    T element = manager.newMetadata();
    if ( element!=null ) {
      try {
        fillItems();
        getComboWidget().setText(Const.NVL(element.getName(),""));
      } catch ( Exception e ) {
        LogChannel.UI.logError( "Error updating list of element names from the metadata", e );
      }
    }
    return element;
  }

  /**
   * Look up the object names from the metadata and populate the items in the combobox with it.
   *
   * @throws HopException In case something went horribly wrong.
   */
  public void fillItems() throws HopException {
    List<String> elementNames = manager.getSerializer().listObjectNames();
    Collections.sort( elementNames );
    wCombo.setItems( elementNames.toArray( new String[ 0 ] ) );
  }

  /**
   * Load the selected element and return it.
   * In case of errors, log them to LogChannel.UI
   * @return The selected element or null if it doesn't exist or there was an error
   */
  public T loadSelectedElement() {
    String selectedItem = wCombo.getText();
    if (StringUtils.isEmpty( selectedItem )) {
      return null;
    }

    try {
      return manager.loadElement(selectedItem);
    } catch(Exception e) {
      LogChannel.UI.logError( "Error loading element '"+selectedItem+"'", e );
      return null;
    }
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
      LogChannel.UI.logError( "Error getting list of element names from the metadata", e );
    }
    if (lsMod!=null) {
      addModifyListener( lsMod );
    }

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
      fdConnection.top = new FormAttachment( previous, props.getMargin() );
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
	wLabel.setEnabled( flag );
    wCombo.setEnabled( flag );
    wToolBar.setEnabled( flag );
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
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getSpace() {
    return variables;
  }

}

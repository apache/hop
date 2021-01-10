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

package org.apache.hop.pipeline.transforms.fake;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.lang.reflect.Method;
import java.util.List;

public class FakeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = FakeDialog.class; // For Translator

  private TableView wFields;
  
  private ComboVar wLocale;

  private final FakeMeta input;
  private ModifyListener lsMod;

  public FakeDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (FakeMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "FakeDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Filename line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // Locale line
    Label wlLocale = new Label( shell, SWT.RIGHT );
    wlLocale.setText( BaseMessages.getString( PKG, "FakeDialog.Label.Locale" ) );
    props.setLook( wlLocale );
    FormData fdlLocale = new FormData();
    fdlLocale.left = new FormAttachment( 0, 0 );
    fdlLocale.right = new FormAttachment( middle, -margin );
    fdlLocale.top = new FormAttachment( wTransformName, margin );
    wlLocale.setLayoutData( fdlLocale );
    wLocale = new ComboVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLocale.setItems( FakeMeta.getFakerLocales() );
    wLocale.setText( transformName );
    props.setLook( wLocale );
    wLocale.addModifyListener( lsMod );
    FormData fdLocale = new FormData();
    fdLocale.left = new FormAttachment( middle, 0 );
    fdLocale.top = new FormAttachment( wlLocale, 0, SWT.CENTER );
    fdLocale.right = new FormAttachment( 100, 0 );
    wLocale.setLayoutData( fdLocale );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    ColumnInfo[] columns =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "FakeDialog.Name.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "FakeDialog.Type.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0] ),
        new ColumnInfo( BaseMessages.getString( PKG, "FakeDialog.Topic.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[0] ),
      };
    columns[1].setComboValuesSelectionListener( this::getComboValues );
    columns[2].setComboValuesSelectionListener( this::getComboValues );

    wFields = new TableView( variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, columns, input.getFields().size(), lsMod, props );
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wLocale, 2*margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wOk, -2*margin );
    wFields.setLayoutData( fdFields );
    wTransformName.addListener( SWT.DefaultSelection, e->ok() );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = event -> {
      Point size = shell.getSize();
      wFields.setSize( size.x - 10, size.y - 50 );
      wFields.table.setSize( size.x - 10, size.y - 50 );
      wFields.redraw();
    };
    shell.addListener( SWT.Resize, lsResize );

    getData();

    // Set the shell size, based upon previous time...
    setSize();

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private String[] getComboValues( TableItem tableItem, int rowNr, int colNr ) {
    if (colNr==2) {
      return FakerType.getTypeDescriptions();
    }
    if (colNr==3) {
      String typeDescription = tableItem.getText(2);
      FakerType fakerType = FakerType.getTypeUsingDescription( typeDescription );
      if (fakerType!=null) {
        return getMethodNames( fakerType );
      }
    }
    return new String[] {};
  }

  public String[] getMethodNames(FakerType fakerType) {
    try {
      Method[] methods = fakerType.getFakerClass().getDeclaredMethods();
      String[] names = new String[methods.length];
      for (int i=0;i<names.length;i++) {
        names[i] = methods[i].getName();
      }
      return names;
    } catch(Exception e) {
      return new String[] {};
    }
  }


  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wLocale.setText(Const.NVL(input.getLocale(), ""));
    for (int row=0;row<input.getFields().size();row++) {
      FakeField fakeField = input.getFields().get( row );
      TableItem item = wFields.table.getItem( row );
      int col=1;
      item.setText(col++, Const.NVL(fakeField.getName(), ""));
      item.setText(col++, Const.NVL(fakeField.getType(), ""));
      item.setText(col++, Const.NVL(fakeField.getTopic(), ""));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void getInfo( FakeMeta meta ) {
    meta.setLocale( wLocale.getText() );
    meta.getFields().clear();
    List<TableItem> nonEmptyItems = wFields.getNonEmptyItems();
    for (TableItem tableItem : nonEmptyItems) {
      int col = 1;
      String name = tableItem.getText(col++);
      String typeDesc = tableItem.getText(col++);
      String topic = tableItem.getText(col++);

      FakerType fakerType = FakerType.getTypeUsingDescription( typeDesc );
      if (fakerType==null) {
        fakerType = FakerType.getTypeUsingName( typeDesc );
      }
      if (fakerType!=null) {
        meta.getFields().add( new FakeField( name, fakerType.name(), topic ) );
      }
    }
  }
}

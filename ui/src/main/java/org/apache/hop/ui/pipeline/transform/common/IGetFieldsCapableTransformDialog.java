/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.pipeline.transform.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.BaseMessageDialog;
import org.apache.hop.ui.core.widget.TableView;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An interface providing functionality for any transform dialog that has the "get fields" capability.
 */
public interface IGetFieldsCapableTransformDialog<TransformMetaType extends BaseTransformMeta> {
  Class<?> PKG = IGetFieldsCapableTransformDialog.class; // Needed by Translator

  LogChannel logger = new LogChannel( IGetFieldsCapableTransformDialog.class );

  /**
   * Returns the {@link Shell} of the parent control.
   *
   * @return the {@link Shell} of the parent control
   */
  Shell getParent();

  /**
   * Returns the {@link Shell} of this dialog.
   *
   * @return the {@link Shell} of this dialog
   */
  Shell getShell();

  /**
   * Returns an array of incoming field names, or an empty array, if fields cannot be fetched, for some reason.
   *
   * @param meta the {@link TransformMetaType}
   * @return an array of incoming field names, or an empty array, if fields cannot be fetched, for some reason.
   */
  String[] getFieldNames( final TransformMetaType meta );

  /**
   * @return the {@link TableView} containing transform fields
   */
  TableView getFieldsTable();

  default TableItem findTableItem( final String fieldName ) {
    for ( int i = 0; i < getFieldsTable().table.getItemCount(); i++ ) {
      final TableItem item = getFieldsTable().table.getItem( i );
      int fieldNameIndex = getFieldsTable().hasIndexColumn() ? 1 : 0;
      final String itemFieldName = item.getText( fieldNameIndex );
      if ( itemFieldName != null && itemFieldName.equals( fieldName ) ) {
        return item;
      }
    }
    return null;
  }

  default List<String> getNewFieldNames( final String[] incomingFieldNames ) {
    final Set<String> fieldNamesInTable = new HashSet();
    for ( int i = 0; i < getFieldsTable().nrNonEmpty(); i++ ) {
      final TableItem item = getFieldsTable().getNonEmpty( i );
      int fieldNameIndex = getFieldsTable().hasIndexColumn() ? 1 : 0;
      fieldNamesInTable.add( item.getText( fieldNameIndex ) );
    }
    final List<String> newFieldNames = Arrays.asList( fieldNamesInTable.toArray( new String[ fieldNamesInTable.size() ] ) ).stream().filter(
      fieldName -> !Arrays.asList( incomingFieldNames ).contains( fieldName ) ).collect( Collectors.toList() );
    return newFieldNames;
  }

  /**
   * This can be called by the "Get fields" button handler to inherit the common "get fields" behavior.
   */
  default void getFields() {
    getFields( getPopulatedMeta() );
  }

  default void getFields( final TransformMetaType meta ) {

    final String[] incomingFieldNames = getFieldNames( meta );
    final List<String> newFieldNames = getNewFieldNames( incomingFieldNames );

    if ( newFieldNames != null && newFieldNames.size() > 0 ) {
      // we have new incoming fields
      final int nrNonEmptyFields = getFieldsTable().nrNonEmpty();
      // are any fields already populated in the fields table?
      if ( nrNonEmptyFields > 0 ) {
        final FieldSelectionDialog fieldSelectDialog = new FieldSelectionDialog( this.getShell(), newFieldNames.size() ) {
          @Override protected void ok() {
            super.ok();
            openGetFieldsSampleDataDialog( reloadAllFields );
          }
        };
        fieldSelectDialog.open();
      } else {
        // no fields are populated yet, go straight to "sample data" dialog
        openGetFieldsSampleDataDialog( true );
      }
    } else {
      // we have no new fields
      final BaseDialog errorDlg = new BaseMessageDialog( getShell(),
        BaseMessages.getString( PKG, "GetFieldsCapableTransformDialog.NoNewFields.Title" ),
        BaseMessages.getString( PKG, "GetFieldsCapableTransformDialog.NoNewFields.Message" ) );
      // if there are no incoming fields at all, we leave the OK button handler as-is and simply dispose the dialog;
      // if there are some incoming fields, we overwrite the OK button handler to show the GetFieldsSampleDataDialog
      if ( incomingFieldNames != null && incomingFieldNames.length > 0 ) {
        final Map<String, Listener> buttons = new HashMap();
        buttons.put( BaseMessages.getString( PKG, "System.Button.OK" ), event -> {
          errorDlg.dispose();
          openGetFieldsSampleDataDialog( true );
        } );
        errorDlg.setButtons( buttons );
      }
      errorDlg.open();
    }
  }

  default void openGetFieldsSampleDataDialog( boolean reloadAllFields ) {
    final GetFieldsSampleDataDialog dlg = new GetFieldsSampleDataDialog( getShell(), this, reloadAllFields );
    dlg.open();
  }

  String loadFieldsImpl( final TransformMetaType meta, final int samples );

  default Map<String, List<String>> getFieldValues() {
    getFieldsTable().nrNonEmpty();
    final Map<String, List<String>> rowValues = new HashMap<>();
    for ( int i = 0; i < getFieldsTable().table.getItemCount(); i++ ) {
      final TableItem item = getFieldsTable().table.getItem( i );
      int startIndex = getFieldsTable().hasIndexColumn() ? 1 : 0;
      final String fieldName = item.getText( startIndex );
      if ( StringUtils.isBlank( fieldName ) ) {
        continue;
      }
      final List<String> values = new ArrayList();
      for ( int j = startIndex; j < getFieldsTable().getColumns().length; j++ ) {
        values.add( item.getText( j ) );
      }
      rowValues.put( fieldName, values );
    }
    return rowValues;
  }

  default Set<String> repopulateFields( final TransformMetaType meta, final Map<String, List<String>> previousFieldValues,
                                        final boolean reloadAllFields ) {
    // incoming field names
    final String[] incomingFieldNames = getFieldNames( meta );
    final Set<String> newFieldNames = new HashSet();
    for ( final String incomingFieldName : incomingFieldNames ) {
      final TableItem item = new TableItem( getFieldsTable().table, SWT.NONE );
      int columnIndexOffset = getFieldsTable().hasIndexColumn() ? 1 : 0;
      item.setText( columnIndexOffset, incomingFieldName );
      if ( previousFieldValues.containsKey( incomingFieldName ) ) {
        // remove the values corresponding to this field from previousFieldValues, that way, all that remains in the
        // previousFieldValues map is field names that are not incoming from other transforms, but rather may have been
        // entered manually by the user
        final List<String> values = previousFieldValues.remove( incomingFieldName );
        int columnIndex = 0;
        if ( !reloadAllFields && values != null ) {
          for ( final String value : values ) {
            item.setText( columnIndex++ + columnIndexOffset, value );
          }
        }
      } else {
        newFieldNames.add( incomingFieldName );
      }
    }
    // whatever is left in previousFieldValues represents user defined fields that may have been entered manually. If
    // we are not clearing and reloading, we should preserve these fields
    if ( !reloadAllFields ) {
      loadRemainingFields( previousFieldValues );
    }
    return newFieldNames;
  }

  default void loadRemainingFields( final Map<String, List<String>> previousFieldValues ) {
    final Iterator<List<String>> remainigValues = previousFieldValues.values().iterator();
    while ( remainigValues.hasNext() ) {
      final List<String> values = remainigValues.next();
      if ( values != null ) {
        final TableItem item = new TableItem( getFieldsTable().table, SWT.NONE );
        int columnIndexOffset = getFieldsTable().hasIndexColumn() ? 1 : 0;
        int columnIndex = 0;
        for ( final String value : values ) {
          item.setText( columnIndex++ + columnIndexOffset, value );
        }
      }
    }
  }

  default String loadFields( final TransformMetaType meta, final int samples, final boolean reloadAllFields ) {
    // fields loading might rely on specific order, and since we allow users to enter fields manually, order is not
    // guaranteed; we therefore need to ensure that fields are properly ordered within the fields table

    // cache the fields currently present in the fields table
    final Map<String, List<String>> fieldValues = getFieldValues();

    // clear the table so that we can re-add the fields in the correct order
    getFieldsTable().removeAll();
    getFieldsTable().removeEmptyRows();
    getFieldsTable().setRowNums();
    getFieldsTable().optWidth( true );

    // ...repopulate the field values in the correct order, keeping track of new incoming fields
    final Set<String> newFieldNames = repopulateFields( meta, fieldValues, reloadAllFields );

    populateMeta( meta );
    final String message = loadFieldsImpl( meta, samples );
    if ( message != null ) {
      if ( reloadAllFields ) {
        getFieldsTable().removeAll();
      }
      // OK, what's the result of our search?
      getData( meta, false, reloadAllFields, newFieldNames );
      getFieldsTable().removeEmptyRows();
      getFieldsTable().setRowNums();
      getFieldsTable().optWidth( true );
    }
    return message;
  }

  default TableItem getTableItem( final String fieldName ) {
    // try to find a table item corresponding to the current field name
    TableItem item = findTableItem( fieldName );
    // if one doesn't exist, create a new one;
    if ( item == null ) {
      item = new TableItem( getFieldsTable().table, SWT.NONE );
    }
    return item;
  }


  void getData( final TransformMetaType inputMeta, final boolean copyTransformName, final boolean reloadAllFields,
                final Set<String> newFieldNames );

  default TransformMetaType getPopulatedMeta() {
    final TransformMetaType newMeta = getNewMetaInstance();
    populateMeta( newMeta );
    return newMeta;
  }

  void populateMeta( final TransformMetaType meta );

  TransformMetaType getNewMetaInstance();

  PipelineMeta getPipelineMeta();
}

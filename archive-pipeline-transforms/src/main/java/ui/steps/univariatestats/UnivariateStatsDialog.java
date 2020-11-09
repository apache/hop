/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.ui.pipeline.transforms.univariatestats;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.univariatestats.UnivariateStatsMeta;
import org.apache.hop.pipeline.transforms.univariatestats.UnivariateStatsMetaFunction;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The UI class for the UnivariateStats transform
 *
 * @author Mark Hall (mhall{[at]}pentaho.org
 * @version 1.0
 */
public class UnivariateStatsDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = UnivariateStatsMeta.class; // Needed by Translator

  /**
   * various UI bits and pieces for the dialog
   */
  private Label m_wlTransformName;
  private Text m_wTransformName;
  private FormData m_fdlTransformName;
  private FormData m_fdTransformName;
  private Label m_wlFields;
  private TableView m_wFields;
  private FormData m_fdlFields;
  private FormData m_fdFields;

  /**
   * meta data for the transform. A copy is made so that changes, in terms of choices made by the user, can be detected.
   */
  private UnivariateStatsMeta m_currentMeta;
  private UnivariateStatsMeta m_originalMeta;

  // holds the names of the fields entering this transform
  private Map<String, Integer> mInputFields;
  private ColumnInfo[] mColinf;

  public UnivariateStatsDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {

    super( parent, (BaseTransformMeta) in, tr, sname );

    // The order here is important...
    // m_currentMeta is looked at for changes
    m_currentMeta = (UnivariateStatsMeta) in;
    m_originalMeta = (UnivariateStatsMeta) m_currentMeta.clone();
    mInputFields = new HashMap<String, Integer>();
  }

  /**
   * Open the dialog
   *
   * @return the transform name
   */
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, m_currentMeta );

    // used to listen to a text field (m_wTransformName)
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
      }
    };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "UnivariateStatsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    m_wlTransformName = new Label( shell, SWT.RIGHT );
    m_wlTransformName.setText( BaseMessages.getString( PKG, "UnivariateStatsDialog.TransformName.Label" ) );
    props.setLook( m_wlTransformName );

    m_fdlTransformName = new FormData();
    m_fdlTransformName.left = new FormAttachment( 0, 0 );
    m_fdlTransformName.right = new FormAttachment( middle, -margin );
    m_fdlTransformName.top = new FormAttachment( 0, margin );
    m_wlTransformName.setLayoutData( m_fdlTransformName );
    m_wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_wTransformName.setText( transformName );
    props.setLook( m_wTransformName );
    m_wTransformName.addModifyListener( lsMod );

    // format the text field
    m_fdTransformName = new FormData();
    m_fdTransformName.left = new FormAttachment( middle, 0 );
    m_fdTransformName.top = new FormAttachment( 0, margin );
    m_fdTransformName.right = new FormAttachment( 100, 0 );
    m_wTransformName.setLayoutData( m_fdTransformName );

    m_wlFields = new Label( shell, SWT.NONE );
    m_wlFields.setText( BaseMessages.getString( PKG, "UnivariateStatsDialog.Fields.Label" ) );
    props.setLook( m_wlFields );
    m_fdlFields = new FormData();
    m_fdlFields.left = new FormAttachment( 0, 0 );
    m_fdlFields.top = new FormAttachment( m_wTransformName, margin );
    m_wlFields.setLayoutData( m_fdlFields );

    final int fieldsRows =
      ( m_currentMeta.getInputFieldMetaFunctions() != null ) ? m_currentMeta.getNumFieldsToProcess() : 1;

    mColinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.InputFieldColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.NColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.MeanColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.StdDevColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.MinColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.MaxColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.MedianColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.PercentileColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "UnivariateStatsDialog.InterpolateColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "True", "False" }, true ) };

    m_wFields =
      new TableView(
        pipelineMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, mColinf, fieldsRows, lsMod, props );

    m_fdFields = new FormData();
    m_fdFields.left = new FormAttachment( 0, 0 );
    m_fdFields.top = new FormAttachment( m_wlFields, margin );
    m_fdFields.right = new FormAttachment( 100, 0 );
    m_fdFields.bottom = new FormAttachment( 100, -50 );
    m_wFields.setLayoutData( m_fdFields );

    // Search the fields in the background
    final Runnable runnable = new Runnable() {
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );

        if ( transformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields( transformMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              IValueMeta field = row.getValueMeta( i );
              // limit the choices to only numeric input fields
              if ( field.isNumeric() ) {
                mInputFields.put( field.getName(), Integer.valueOf( i ) );
              }
            }

            setComboBoxes();
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "UnivariateStatsDialog.Log.UnableToFindInput" ) );
          }
        }
      }
    };

    new Thread( runnable ).start();

    m_wFields.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        // Now set the combo's
        shell.getDisplay().asyncExec( new Runnable() {
          public void run() {
            setComboBoxes();
          }
        } );
      }
    } );

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    m_wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData(); // read stats settings from the transform

    m_currentMeta.setChanged( changed );

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return transformName;
  }

  /**
   * Set up the input field combo box
   */
  protected void setComboBoxes() {
    Set<String> keySet = mInputFields.keySet();
    List<String> entries = new ArrayList<>( keySet );
    String[] fieldNames = entries.toArray( new String[ entries.size() ] );
    Const.sortStrings( fieldNames );
    mColinf[ 0 ].setComboValues( fieldNames );
  }

  /**
   * Copy information from the meta-data m_currentMeta to the dialog fields.
   */
  public void getData() {

    if ( m_currentMeta.getInputFieldMetaFunctions() != null ) {
      for ( int i = 0; i < m_currentMeta.getNumFieldsToProcess(); i++ ) {
        UnivariateStatsMetaFunction fn = m_currentMeta.getInputFieldMetaFunctions()[ i ];

        TableItem item = m_wFields.table.getItem( i );

        item.setText( 1, Const.NVL( fn.getSourceFieldName(), "" ) );
        item.setText( 2, Const.NVL( ( fn.getCalcN() ) ? "True" : "False", "" ) );
        item.setText( 3, Const.NVL( ( fn.getCalcMean() ) ? "True" : "False", "" ) );
        item.setText( 4, Const.NVL( ( fn.getCalcStdDev() ) ? "True" : "False", "" ) );
        item.setText( 5, Const.NVL( ( fn.getCalcMin() ) ? "True" : "False", "" ) );
        item.setText( 6, Const.NVL( ( fn.getCalcMax() ) ? "True" : "False", "" ) );
        item.setText( 7, Const.NVL( ( fn.getCalcMedian() ) ? "True" : "False", "" ) );
        double p = fn.getCalcPercentile();
        NumberFormat pF = NumberFormat.getInstance();
        pF.setMaximumFractionDigits( 2 );
        String res = ( p < 0 ) ? "" : pF.format( p * 100 );
        item.setText( 8, Const.NVL( res, "" ) );

        item.setText( 9, Const.NVL( ( fn.getInterpolatePercentile() ) ? "True" : "False", "" ) );
      }

      m_wFields.setRowNums();
      m_wFields.optWidth( true );
    }

    m_wTransformName.selectAll();
    m_wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    m_currentMeta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( m_wTransformName.getText() ) ) {
      return;
    }

    transformName = m_wTransformName.getText(); // return value

    int nrNonEmptyFields = m_wFields.nrNonEmpty();
    m_currentMeta.allocate( nrNonEmptyFields );

    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = m_wFields.getNonEmpty( i );

      String inputFieldName = item.getText( 1 );
      boolean n = item.getText( 2 ).equalsIgnoreCase( "True" );
      boolean mean = item.getText( 3 ).equalsIgnoreCase( "True" );
      boolean stdDev = item.getText( 4 ).equalsIgnoreCase( "True" );
      boolean min = item.getText( 5 ).equalsIgnoreCase( "True" );
      boolean max = item.getText( 6 ).equalsIgnoreCase( "True" );
      boolean median = item.getText( 7 ).equalsIgnoreCase( "True" );
      String percentileS = item.getText( 8 );
      double percentile = -1;
      if ( percentileS.length() > 0 ) {
        // try to parse percentile
        try {
          percentile = Double.parseDouble( percentileS );
          if ( percentile < 0 ) {
            percentile = -1;
          } else if ( percentile > 1 && percentile <= 100 ) {
            percentile /= 100;
          }
        } catch ( Exception ex ) {
          // Ignore errors
        }
      }
      boolean interpolate = item.getText( 9 ).equalsIgnoreCase( "True" );

      //CHECKSTYLE:Indentation:OFF
      m_currentMeta.getInputFieldMetaFunctions()[ i ] = new UnivariateStatsMetaFunction(
        inputFieldName, n, mean, stdDev, min, max, median, percentile, interpolate );
    }

    if ( !m_originalMeta.equals( m_currentMeta ) ) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }
}

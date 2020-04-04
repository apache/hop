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

package org.apache.hop.ui.pipeline.transforms.reservoirsampling;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.reservoirsampling.ReservoirSamplingMeta;
import org.apache.hop.ui.core.widget.TextVar;
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
import org.eclipse.swt.widgets.Text;

/**
 * The UI class for the ReservoirSampling transform
 *
 * @author Mark Hall (mhall{[at]}pentaho.org
 * @version 1.0
 */
public class ReservoirSamplingDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = ReservoirSamplingMeta.class; // for i18n purposes, needed by Translator!!

  // various UI bits and pieces
  private Label m_wlTransformName;
  private Text m_wTransformName;
  private FormData m_fdlTransformName;
  private FormData m_fdTransformName;

  private Label m_wlSampleSize;
  private TextVar m_wSampleSize;
  private FormData m_fdlSampleSize;
  private FormData m_fdSampleSize;

  private Label m_wlSeed;
  private TextVar m_wSeed;
  private FormData m_fdlSeed;
  private FormData m_fdSeed;

  /**
   * meta data for the transform. A copy is made so that changes, in terms of choices made by the user, can be detected.
   */
  private ReservoirSamplingMeta m_currentMeta;
  private ReservoirSamplingMeta m_originalMeta;

  public ReservoirSamplingDialog( Shell parent, Object in, PipelineMeta tr, String sname ) {

    super( parent, (BaseTransformMeta) in, tr, sname );

    // The order here is important...
    // m_currentMeta is looked at for changes
    m_currentMeta = (ReservoirSamplingMeta) in;
    m_originalMeta = (ReservoirSamplingMeta) m_currentMeta.clone();
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
    shell.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    m_wlTransformName = new Label( shell, SWT.RIGHT );
    m_wlTransformName.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.TransformName.Label" ) );
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

    // Sample size text field
    m_wlSampleSize = new Label( shell, SWT.RIGHT );
    m_wlSampleSize.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.SampleSize.Label" ) );
    props.setLook( m_wlSampleSize );

    m_fdlSampleSize = new FormData();
    m_fdlSampleSize.left = new FormAttachment( 0, 0 );
    m_fdlSampleSize.right = new FormAttachment( middle, -margin );
    m_fdlSampleSize.top = new FormAttachment( m_wTransformName, margin );
    m_wlSampleSize.setLayoutData( m_fdlSampleSize );

    m_wSampleSize = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wSampleSize );
    m_wSampleSize.addModifyListener( lsMod );
    m_wSampleSize.setText( "" + m_originalMeta.getSampleSize() );
    m_fdSampleSize = new FormData();
    m_fdSampleSize.left = new FormAttachment( m_wlSampleSize, margin );
    m_fdSampleSize.right = new FormAttachment( 100, -margin );
    m_fdSampleSize.top = new FormAttachment( m_wTransformName, margin );
    m_wSampleSize.setLayoutData( m_fdSampleSize );

    // Seed text field
    m_wlSeed = new Label( shell, SWT.RIGHT );
    m_wlSeed.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.Seed.Label" ) );
    props.setLook( m_wlSeed );

    m_fdlSeed = new FormData();
    m_fdlSeed.left = new FormAttachment( 0, 0 );
    m_fdlSeed.right = new FormAttachment( middle, -margin );
    m_fdlSeed.top = new FormAttachment( m_wSampleSize, margin );
    m_wlSeed.setLayoutData( m_fdlSeed );

    m_wSeed = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wSeed );
    m_wSeed.addModifyListener( lsMod );
    m_wSeed.setText( "" + m_originalMeta.getSeed() );
    m_fdSeed = new FormData();
    m_fdSeed.left = new FormAttachment( m_wlSeed, margin );
    m_fdSeed.right = new FormAttachment( 100, -margin );
    m_fdSeed.top = new FormAttachment( m_wSampleSize, margin );
    m_wSeed.setLayoutData( m_fdSeed );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, m_wSeed );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

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

    // Whenever something changes, set the tooltip to the expanded version:
    m_wSampleSize.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_wSampleSize.setToolTipText( pipelineMeta.environmentSubstitute( m_wSampleSize.getText() ) );
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version:
    m_wSeed.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_wSeed.setToolTipText( pipelineMeta.environmentSubstitute( m_wSeed.getText() ) );
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    m_currentMeta.setChanged( changed );

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return transformName;
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

    m_currentMeta.setSampleSize( m_wSampleSize.getText() );
    m_currentMeta.setSeed( m_wSeed.getText() );
    if ( !m_originalMeta.equals( m_currentMeta ) ) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }
}

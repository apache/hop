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

package org.apache.hop.pipeline.transforms.reservoirsampling;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class ReservoirSamplingDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = ReservoirSamplingMeta.class; // For Translator

  private Text mWTransformName;

  private TextVar mWSampleSize;

  private TextVar mWSeed;

  /**
   * meta data for the transform. A copy is made so that changes, in terms of choices made by the user, can be detected.
   */
  private final ReservoirSamplingMeta mCurrentMeta;
  private final ReservoirSamplingMeta mOriginalMeta;

  public ReservoirSamplingDialog( Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname ) {

    super( parent, variables, (BaseTransformMeta) in, tr, sname );

    // The order here is important...
    // m_currentMeta is looked at for changes
    mCurrentMeta = (ReservoirSamplingMeta) in;
    mOriginalMeta = (ReservoirSamplingMeta) mCurrentMeta.clone();
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
    setShellImage( shell, mCurrentMeta);

    // used to listen to a text field (m_wTransformName)
    ModifyListener lsMod = e -> mCurrentMeta.setChanged();

    changed = mCurrentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    // various UI bits and pieces
    Label mWlTransformName = new Label(shell, SWT.RIGHT);
    mWlTransformName.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.TransformName.Label" ) );
    props.setLook(mWlTransformName);

    FormData mFdlTransformName = new FormData();
    mFdlTransformName.left = new FormAttachment( 0, 0 );
    mFdlTransformName.right = new FormAttachment( middle, -margin );
    mFdlTransformName.top = new FormAttachment( 0, margin );
    mWlTransformName.setLayoutData(mFdlTransformName);
    mWTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    mWTransformName.setText( transformName );
    props.setLook(mWTransformName);
    mWTransformName.addModifyListener( lsMod );

    // format the text field
    FormData mFdTransformName = new FormData();
    mFdTransformName.left = new FormAttachment( middle, 0 );
    mFdTransformName.top = new FormAttachment( 0, margin );
    mFdTransformName.right = new FormAttachment( 100, 0 );
    mWTransformName.setLayoutData(mFdTransformName);

    // Sample size text field
    Label mWlSampleSize = new Label(shell, SWT.RIGHT);
    mWlSampleSize.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.SampleSize.Label" ) );
    props.setLook(mWlSampleSize);

    FormData mFdlSampleSize = new FormData();
    mFdlSampleSize.left = new FormAttachment( 0, 0 );
    mFdlSampleSize.right = new FormAttachment( middle, -margin );
    mFdlSampleSize.top = new FormAttachment(mWTransformName, margin );
    mWlSampleSize.setLayoutData(mFdlSampleSize);

    mWSampleSize = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(mWSampleSize);
    mWSampleSize.addModifyListener( lsMod );
    mWSampleSize.setText( "" + mOriginalMeta.getSampleSize() );
    FormData mFdSampleSize = new FormData();
    mFdSampleSize.left = new FormAttachment(mWlSampleSize, margin );
    mFdSampleSize.right = new FormAttachment( 100, -margin );
    mFdSampleSize.top = new FormAttachment(mWTransformName, margin );
    mWSampleSize.setLayoutData(mFdSampleSize);

    // Seed text field
    Label mWlSeed = new Label(shell, SWT.RIGHT);
    mWlSeed.setText( BaseMessages.getString( PKG, "ReservoirSamplingDialog.Seed.Label" ) );
    props.setLook(mWlSeed);

    FormData mFdlSeed = new FormData();
    mFdlSeed.left = new FormAttachment( 0, 0 );
    mFdlSeed.right = new FormAttachment( middle, -margin );
    mFdlSeed.top = new FormAttachment(mWSampleSize, margin );
    mWlSeed.setLayoutData(mFdlSeed);

    mWSeed = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook(mWSeed);
    mWSeed.addModifyListener( lsMod );
    mWSeed.setText( "" + mOriginalMeta.getSeed() );
    FormData mFdSeed = new FormData();
    mFdSeed.left = new FormAttachment(mWlSeed, margin );
    mFdSeed.right = new FormAttachment( 100, -margin );
    mFdSeed.top = new FormAttachment(mWSampleSize, margin );
    mWSeed.setLayoutData(mFdSeed);

    // Some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, mWSeed);

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    mWTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version:
    mWSampleSize.addModifyListener(e -> mWSampleSize.setToolTipText( variables.resolve( mWSampleSize.getText() ) ) );

    // Whenever something changes, set the tooltip to the expanded version:
    mWSeed.addModifyListener(e -> mWSeed.setToolTipText( variables.resolve( mWSeed.getText() ) ) );

    // Set the shell size, based upon previous time...
    setSize();

    mCurrentMeta.setChanged( changed );

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
    mCurrentMeta.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( mWTransformName.getText() ) ) {
      return;
    }

    transformName = mWTransformName.getText(); // return value

    mCurrentMeta.setSampleSize( mWSampleSize.getText() );
    mCurrentMeta.setSeed( mWSeed.getText() );
    if ( !mOriginalMeta.equals(mCurrentMeta) ) {
      mCurrentMeta.setChanged();
      changed = mCurrentMeta.hasChanged();
    }

    dispose();
  }
}

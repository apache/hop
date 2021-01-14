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
package org.apache.hop.ui.pipeline.transforms.missing;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import java.util.List;

public class MissingPipelineDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = MissingPipelineDialog.class; // For Translator

  private Shell shell;
  private Shell shellParent;
  private List<Missing> missingPipeline;
  private int mode;
  private PropsUi props;
  private String transformResult;

  public static final int MISSING_PIPELINE_TRANSFORMS = 1;
  public static final int MISSING_PIPELINE_TRANSFORM_ID = 2;

  public MissingPipelineDialog( Shell parent, IVariables variables, List<Missing> missingPipeline, ITransformMeta baseTransformMeta,
                                PipelineMeta pipelineMeta, String transformName ) {
    super( parent, variables, baseTransformMeta, pipelineMeta, transformName );
    this.shellParent = parent;
    this.missingPipeline = missingPipeline;
    this.mode = MISSING_PIPELINE_TRANSFORMS;
  }

  public MissingPipelineDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String transformName ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, transformName );
    this.shellParent = parent;
    this.mode = MISSING_PIPELINE_TRANSFORM_ID;
  }

  private String getErrorMessage( List<Missing> missingPipeline, int mode ) {
    String message = "";
    if ( mode == MISSING_PIPELINE_TRANSFORMS ) {
      StringBuilder entries = new StringBuilder();
      for ( Missing entry : missingPipeline ) {
        if ( missingPipeline.indexOf( entry ) == missingPipeline.size() - 1 ) {
          entries.append( "- " + entry.getTransformName() + " - " + entry.getMissingPluginId() + "\n\n" );
        } else {
          entries.append( "- " + entry.getTransformName() + " - " + entry.getMissingPluginId() + "\n" );
        }
      }
      message = BaseMessages.getString( PKG, "MissingPipelineDialog.MissingPipelineTransforms", entries.toString() );
    }

    if ( mode == MISSING_PIPELINE_TRANSFORM_ID ) {
      message =
        BaseMessages.getString( PKG, "MissingPipelineDialog.MissingPipelineTransformId", transformName + " - "
          + ( (Missing) baseTransformMeta ).getMissingPluginId() );
    }
    return message.toString();
  }

  public String open() {
    this.props = PropsUi.getInstance();
    Display display = shellParent.getDisplay();
    int margin = props.getMargin();

    shell =
      new Shell( shellParent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.ICON
        | SWT.APPLICATION_MODAL );

    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginLeft = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "MissingPipelineDialog.MissingPlugins" ) );
    shell.setLayout( formLayout );

    Label image = new Label( shell, SWT.NONE );
    props.setLook( image );
    Image icon = display.getSystemImage( SWT.ICON_QUESTION );
    image.setImage( icon );
    FormData imageData = new FormData();
    imageData.left = new FormAttachment( 0, 5 );
    imageData.right = new FormAttachment( 11, 0 );
    imageData.top = new FormAttachment( 0, 10 );
    image.setLayoutData( imageData );

    Label error = new Label( shell, SWT.WRAP );
    props.setLook( error );
    error.setText( getErrorMessage( missingPipeline, mode ) );
    FormData errorData = new FormData();
    errorData.left = new FormAttachment( image, 5 );
    errorData.right = new FormAttachment( 100, -5 );
    errorData.top = new FormAttachment( 0, 10 );
    error.setLayoutData( errorData );

    Label separator = new Label( shell, SWT.WRAP );
    props.setLook( separator );
    FormData separatorData = new FormData();
    separatorData.top = new FormAttachment( error, 10 );
    separator.setLayoutData( separatorData );

    Button closeButton = new Button( shell, SWT.PUSH );
    props.setLook( closeButton );
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment( 98 );
    fdClose.top = new FormAttachment( separator );
    closeButton.setLayoutData( fdClose );
    closeButton.setText( BaseMessages.getString( PKG, "MissingPipelineDialog.Close" ) );
    closeButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        shell.dispose();
        transformResult = null;
      }
    } );

    FormData fdSearch = new FormData();
    if ( this.mode == MISSING_PIPELINE_TRANSFORMS ) {
      Button openButton = new Button( shell, SWT.PUSH );
      props.setLook( openButton );
      FormData fdOpen = new FormData();
      fdOpen.right = new FormAttachment( closeButton, -5 );
      fdOpen.bottom = new FormAttachment( closeButton, 0, SWT.BOTTOM );
      openButton.setLayoutData( fdOpen );
      openButton.setText( BaseMessages.getString( PKG, "MissingPipelineDialog.OpenFile" ) );
      openButton.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          shell.dispose();
          transformResult = transformName;
        }
      } );
      fdSearch.right = new FormAttachment( openButton, -5 );
      fdSearch.bottom = new FormAttachment( openButton, 0, SWT.BOTTOM );
    } else {
      fdSearch.right = new FormAttachment( closeButton, -5 );
      fdSearch.bottom = new FormAttachment( closeButton, 0, SWT.BOTTOM );
    }

    Button searchButton = new Button( shell, SWT.PUSH );
    props.setLook( searchButton );
    searchButton.setText( BaseMessages.getString( PKG, "MissingPipelineDialog.SearchMarketplace" ) );
    searchButton.setLayoutData( fdSearch );
    searchButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        try {
          shell.dispose();
          // HopGui.getInstance().openMarketplace();  TODO: implement marketplace
        } catch ( Exception ex ) {
          ex.printStackTrace();
        }
      }
    } );

    shell.pack();
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformResult;
  }
}

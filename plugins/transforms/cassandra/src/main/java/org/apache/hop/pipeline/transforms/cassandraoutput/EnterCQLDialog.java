/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.cassandraoutput;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transforms.tableinput.SqlValuesHighlight;
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
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

/**
 * Provides a popup dialog for editing CQL commands.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class EnterCQLDialog extends Dialog {

  private static Class<?> PKG = EnterCQLDialog.class; // for i18n purposes,
                                                      // needed by Translator2!!
                                                      // $NON-NLS-1$

  protected String m_title;

  protected String m_originalCQL;
  protected String m_currentCQL;

  protected Shell m_parent;
  protected Shell m_shell;

  protected Button m_ok;
  protected Button m_cancel;
  protected Listener m_lsCancel;
  protected Listener m_lsOK;

  protected Button m_dontComplainAboutAprioriCQLFailing;

  protected PropsUi m_props;

  protected StyledTextComp m_cqlText;

  protected PipelineMeta m_transMeta;

  protected ModifyListener m_lsMod;

  protected boolean m_dontComplain;

  public EnterCQLDialog( Shell parent, PipelineMeta transMeta, ModifyListener lsMod, String title, String cql,
      boolean dontComplain ) {
    super( parent, SWT.NONE );

    m_parent = parent;
    m_props = PropsUi.getInstance();
    m_title = title;
    m_originalCQL = cql;
    m_transMeta = transMeta;
    m_lsMod = lsMod;
    m_dontComplain = dontComplain;
  }

  public String open() {

    Display display = m_parent.getDisplay();

    m_shell = new Shell( m_parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
    m_props.setLook( m_shell );
    m_shell.setImage( GuiResource.getInstance().getImageHopUi() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    m_shell.setLayout( formLayout );
    m_shell.setText( m_title );

    int margin = Const.MARGIN;
    int middle = Const.MIDDLE_PCT;

    Label dontComplainLab = new Label( m_shell, SWT.RIGHT );
    m_props.setLook( dontComplainLab );
    dontComplainLab.setText( BaseMessages.getString( this.getClass(), "EnterCQLDialog.DontComplainIfCQLFails.Label" ) ); //$NON-NLS-1$
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.bottom = new FormAttachment( 100, -50 );
    dontComplainLab.setLayoutData( fd );

    m_dontComplainAboutAprioriCQLFailing = new Button( m_shell, SWT.CHECK );
    m_props.setLook( m_dontComplainAboutAprioriCQLFailing );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -50 );
    fd.left = new FormAttachment( middle, 0 );
    m_dontComplainAboutAprioriCQLFailing.setLayoutData( fd );
    m_dontComplainAboutAprioriCQLFailing.setSelection( m_dontComplain );
    m_dontComplainAboutAprioriCQLFailing.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_dontComplain = m_dontComplainAboutAprioriCQLFailing.getSelection();
      }
    } );

    m_cqlText =
        new StyledTextComp( m_transMeta, m_shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" ); //$NON-NLS-1$
    m_props.setLook( m_cqlText, m_props.WIDGET_STYLE_FIXED );

    m_cqlText.setText( m_originalCQL );
    m_currentCQL = m_originalCQL;

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, -2 * margin );
    fd.bottom = new FormAttachment( m_dontComplainAboutAprioriCQLFailing, -margin );
    m_cqlText.setLayoutData( fd );
    m_cqlText.addModifyListener( m_lsMod );
    m_cqlText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_cqlText.setToolTipText( m_transMeta.environmentSubstitute( m_cqlText.getText() ) );
      }
    } );

    // Text Highlighting
    m_cqlText.addLineStyleListener( new SqlValuesHighlight() );

    // Some buttons
    m_ok = new Button( m_shell, SWT.PUSH );
    m_ok.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$
    m_cancel = new Button( m_shell, SWT.PUSH );
    m_cancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$

    BaseTransformDialog.positionBottomButtons( m_shell, new Button[] { m_ok, m_cancel }, margin, null );

    // Add listeners
    m_lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    m_lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    m_ok.addListener( SWT.Selection, m_lsOK );
    m_cancel.addListener( SWT.Selection, m_lsCancel );

    // Detect [X] or ALT-F4 or something that kills this window...
    m_shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        checkCancel( e );
      }
    } );

    BaseTransformDialog.setSize( m_shell );
    m_shell.open();

    while ( !m_shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return m_currentCQL;
  }

  public void dispose() {
    m_props.setScreen( new WindowProperty( m_shell ) );
    m_shell.dispose();
  }

  public boolean getDontComplainStatus() {
    return m_dontComplain;
  }

  protected void ok() {
    m_currentCQL = m_cqlText.getText();
    dispose();
  }

  protected void cancel() {
    m_currentCQL = m_originalCQL;
    dispose();
  }

  public void checkCancel( ShellEvent e ) {
    String newText = m_cqlText.getText();
    if ( !newText.equals( m_originalCQL ) ) {
      int save = 0;// TODO FIX BY finding replacement JobGraph.showChangedWarning( m_shell, m_title );
      if ( save == SWT.CANCEL ) {
        e.doit = false;
      } else if ( save == SWT.YES ) {
        ok();
      } else {
        cancel();
      }
    } else {
      cancel();
    }
  }
}

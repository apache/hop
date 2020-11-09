/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.regexeval;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.regexeval.RegexEvalMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Dialog to test a regular expression
 *
 * @author Samatar
 * @since 20-04-2009
 */
public class RegexEvalHelperDialog extends Dialog {
  private static final Class<?> PKG = RegexEvalMeta.class; // Needed by Translator

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private Shell shell;
  private PropsUi props;
  private String regexscript;
  private String regexoptions;
  private boolean canonicalEqualityFlagSet;
  private PipelineMeta transmeta;

  private CTabFolder wNoteFolder;
  private FormData fdNoteFolder;

  private CTabItem wNoteContentTab;

  private FormData fdNoteContentComp;

  private Label wlRegExScript;
  private StyledTextComp wRegExScript;
  private FormData fdlRegExScript, fdRegExScript;

  private Label wlValue1;
  private Text wValue1;
  private FormData fdlValue1, fdValue1;

  private Label wlValue2;
  private Text wValue2;
  private FormData fdlValue2, fdValue2;

  private Label wlValue3;
  private Text wValue3;
  private FormData fdlValue3, fdValue3;

  private Group wValuesGroup;
  private FormData fdValuesGroup;

  private Group wCaptureGroups;
  private FormData fdCaptureGroups;

  private Text wRegExScriptCompile;
  private FormData fdRegExScriptCompile;

  private List wGroups;
  private FormData fdGroups;
  private Label wlGroups;
  private FormData fdlGroups;

  private Label wlValueGroup;
  private Text wValueGroup;
  private FormData fdlValueGroup, fdValueGroup;

  GuiResource guiresource = GuiResource.getInstance();

  private boolean errorDisplayed;

  /**
   * Dialog to allow someone to test regular expression
   *
   * @param parent                   The parent shell to use
   * @param RegexScript              The expression to test
   * @param RegexOptions             Any extended options for the regular expression
   * @param canonicalEqualityFlagSet
   */
  public RegexEvalHelperDialog( Shell parent, PipelineMeta transmeta, String RegexScript, String RegexOptions,
                                boolean canonicalEqualityFlagSet ) {
    super( parent, SWT.NONE );
    props = PropsUi.getInstance();
    this.regexscript = RegexScript;
    this.regexoptions = RegexOptions;
    this.transmeta = transmeta;
    this.errorDisplayed = false;
    this.canonicalEqualityFlagSet = canonicalEqualityFlagSet;
  }

  private boolean isCanonicalEqualityFlagSet() {
    return this.canonicalEqualityFlagSet;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.NONE );
    props.setLook( shell );
    shell.setImage( guiresource.getImageHopUi() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.Shell.Label" ) );

    int margin = props.getMargin();
    int middle = 30;

    wNoteFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wNoteFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF NOTE CONTENT TAB///
    // /
    wNoteContentTab = new CTabItem( wNoteFolder, SWT.NONE );
    wNoteContentTab.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.RegExTab.Label" ) );
    Composite wNoteContentComp = new Composite( wNoteFolder, SWT.NONE );
    props.setLook( wNoteContentComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wNoteContentComp.setLayout( fileLayout );

    // RegEx Script
    wlRegExScript = new Label( wNoteContentComp, SWT.RIGHT );
    wlRegExScript.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.Script.Label" ) );
    props.setLook( wlRegExScript );
    fdlRegExScript = new FormData();
    fdlRegExScript.left = new FormAttachment( 0, 0 );
    fdlRegExScript.top = new FormAttachment( 0, 2 * margin );
    wlRegExScript.setLayoutData( fdlRegExScript );

    wRegExScript =
      new StyledTextComp( transmeta, wNoteContentComp, SWT.MULTI
        | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    wRegExScript.setText( "" );
    props.setLook( wRegExScript, Props.WIDGET_STYLE_FIXED );
    props.setLook( wRegExScript );
    fdRegExScript = new FormData();
    fdRegExScript.left = new FormAttachment( 0, 0 );
    fdRegExScript.top = new FormAttachment( wlRegExScript, 2 * margin );
    fdRegExScript.right = new FormAttachment( 100, -2 * margin );
    fdRegExScript.bottom = new FormAttachment( 40, -margin );
    wRegExScript.setLayoutData( fdRegExScript );

    wRegExScriptCompile = new Text( wNoteContentComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL );
    wRegExScriptCompile.setText( "" );
    props.setLook( wRegExScriptCompile, Props.WIDGET_STYLE_FIXED );
    fdRegExScriptCompile = new FormData();
    fdRegExScriptCompile.left = new FormAttachment( 0, 0 );
    fdRegExScriptCompile.top = new FormAttachment( wRegExScript, margin );
    fdRegExScriptCompile.right = new FormAttachment( 100, 0 );
    wRegExScriptCompile.setLayoutData( fdRegExScriptCompile );
    wRegExScriptCompile.setEditable( false );
    wRegExScriptCompile.setFont( guiresource.getFontNote() );

    // ////////////////////////
    // START OF Values GROUP
    //

    wValuesGroup = new Group( wNoteContentComp, SWT.SHADOW_NONE );
    props.setLook( wValuesGroup );
    wValuesGroup.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.TestValues.Label" ) );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wValuesGroup.setLayout( groupLayout );

    // Value1
    wlValue1 = new Label( wValuesGroup, SWT.RIGHT );
    wlValue1.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.Value1.Label" ) );
    props.setLook( wlValue1 );
    fdlValue1 = new FormData();
    fdlValue1.left = new FormAttachment( 0, 0 );
    fdlValue1.top = new FormAttachment( wRegExScriptCompile, margin );
    fdlValue1.right = new FormAttachment( middle, -margin );
    wlValue1.setLayoutData( fdlValue1 );
    wValue1 = new Text( wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValue1 );
    fdValue1 = new FormData();
    fdValue1.left = new FormAttachment( middle, margin );
    fdValue1.top = new FormAttachment( wRegExScriptCompile, margin );
    fdValue1.right = new FormAttachment( 100, -margin );
    wValue1.setLayoutData( fdValue1 );

    // Value2
    wlValue2 = new Label( wValuesGroup, SWT.RIGHT );
    wlValue2.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.Value2.Label" ) );
    props.setLook( wlValue2 );
    fdlValue2 = new FormData();
    fdlValue2.left = new FormAttachment( 0, 0 );
    fdlValue2.top = new FormAttachment( wValue1, margin );
    fdlValue2.right = new FormAttachment( middle, -margin );
    wlValue2.setLayoutData( fdlValue2 );
    wValue2 = new Text( wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValue2 );
    fdValue2 = new FormData();
    fdValue2.left = new FormAttachment( middle, margin );
    fdValue2.top = new FormAttachment( wValue1, margin );
    fdValue2.right = new FormAttachment( 100, -margin );
    wValue2.setLayoutData( fdValue2 );

    // Value3
    wlValue3 = new Label( wValuesGroup, SWT.RIGHT );
    wlValue3.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.Value3.Label" ) );
    props.setLook( wlValue3 );
    fdlValue3 = new FormData();
    fdlValue3.left = new FormAttachment( 0, 0 );
    fdlValue3.top = new FormAttachment( wValue2, margin );
    fdlValue3.right = new FormAttachment( middle, -margin );
    wlValue3.setLayoutData( fdlValue3 );
    wValue3 = new Text( wValuesGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValue3 );
    fdValue3 = new FormData();
    fdValue3.left = new FormAttachment( middle, margin );
    fdValue3.top = new FormAttachment( wValue2, margin );
    fdValue3.right = new FormAttachment( 100, -margin );
    wValue3.setLayoutData( fdValue3 );

    fdValuesGroup = new FormData();
    fdValuesGroup.left = new FormAttachment( 0, margin );
    fdValuesGroup.top = new FormAttachment( wRegExScriptCompile, margin );
    fdValuesGroup.right = new FormAttachment( 100, -margin );
    wValuesGroup.setLayoutData( fdValuesGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF VALUEs GROUP

    // ////////////////////////
    // START OF Values GROUP
    //

    wCaptureGroups = new Group( wNoteContentComp, SWT.SHADOW_NONE );
    props.setLook( wCaptureGroups );
    wCaptureGroups.setText( "Capture" );
    FormLayout captureLayout = new FormLayout();
    captureLayout.marginWidth = 10;
    captureLayout.marginHeight = 10;
    wCaptureGroups.setLayout( captureLayout );

    // ValueGroup
    wlValueGroup = new Label( wCaptureGroups, SWT.RIGHT );
    wlValueGroup.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.ValueGroup.Label" ) );
    props.setLook( wlValueGroup );
    fdlValueGroup = new FormData();
    fdlValueGroup.left = new FormAttachment( 0, 0 );
    fdlValueGroup.top = new FormAttachment( wValuesGroup, margin );
    fdlValueGroup.right = new FormAttachment( middle, -margin );
    wlValueGroup.setLayoutData( fdlValueGroup );
    wValueGroup = new Text( wCaptureGroups, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValueGroup );
    fdValueGroup = new FormData();
    fdValueGroup.left = new FormAttachment( middle, margin );
    fdValueGroup.top = new FormAttachment( wValuesGroup, margin );
    fdValueGroup.right = new FormAttachment( 100, -margin );
    wValueGroup.setLayoutData( fdValueGroup );

    wlGroups = new Label( wCaptureGroups, SWT.RIGHT );
    wlGroups.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.GroupFields.Label" ) );
    props.setLook( wlGroups );
    fdlGroups = new FormData();
    fdlGroups.left = new FormAttachment( 0, 0 );
    fdlGroups.top = new FormAttachment( wValueGroup, margin );
    fdlGroups.right = new FormAttachment( middle, -margin );
    wlGroups.setLayoutData( fdlGroups );
    wGroups = new List( wCaptureGroups, SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL | SWT.SINGLE );
    props.setLook( wValue3 );
    fdGroups = new FormData();
    fdGroups.left = new FormAttachment( middle, margin );
    fdGroups.top = new FormAttachment( wValueGroup, margin );
    fdGroups.right = new FormAttachment( 100, -margin );
    fdGroups.bottom = new FormAttachment( 100, -margin );
    wGroups.setLayoutData( fdGroups );

    fdCaptureGroups = new FormData();
    fdCaptureGroups.left = new FormAttachment( 0, margin );
    fdCaptureGroups.top = new FormAttachment( wValuesGroup, margin );
    fdCaptureGroups.right = new FormAttachment( 100, -margin );
    fdCaptureGroups.bottom = new FormAttachment( 100, -margin );
    wCaptureGroups.setLayoutData( fdCaptureGroups );

    // ///////////////////////////////////////////////////////////
    // / END OF VALUEs GROUP
    // ///////////////////////////////////////////////////////////

    fdNoteContentComp = new FormData();
    fdNoteContentComp.left = new FormAttachment( 0, 0 );
    fdNoteContentComp.top = new FormAttachment( 0, 0 );
    fdNoteContentComp.right = new FormAttachment( 100, 0 );
    fdNoteContentComp.bottom = new FormAttachment( 100, 0 );
    wNoteContentComp.setLayoutData( fdNoteContentComp );
    wNoteContentComp.layout();
    wNoteContentTab.setControl( wNoteContentComp );

    // ///////////////////////////////////////////////////////////
    // / END OF NOTE CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////

    fdNoteFolder = new FormData();
    fdNoteFolder.left = new FormAttachment( 0, 0 );
    fdNoteFolder.top = new FormAttachment( 0, margin );
    fdNoteFolder.right = new FormAttachment( 100, 0 );
    fdNoteFolder.bottom = new FormAttachment( 100, -50 );
    wNoteFolder.setLayoutData( fdNoteFolder );

    // Some buttons

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wNoteFolder );

    // Add listeners
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

    wValue1.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        testValue( 1, true, null );
      }
    } );
    wValue2.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        testValue( 2, true, null );
      }
    } );
    wValue3.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        testValue( 3, true, null );
      }
    } );
    wValueGroup.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        testValue( 4, true, null );
      }
    } );
    wRegExScript.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        errorDisplayed = false;
        testValues();
      }
    } );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return regexscript;
  }

  private void testValues() {
    String realScript = transmeta.environmentSubstitute( wRegExScript.getText() );

    for ( int i = 1; i < 5; i++ ) {
      testValue( i, false, realScript );
    }
  }

  private void testValue( int index, boolean testRegEx, String regExString ) {
    String realScript = regExString;
    if ( realScript == null ) {
      realScript = transmeta.environmentSubstitute( wRegExScript.getText() );
    }
    if ( Utils.isEmpty( realScript ) ) {
      if ( testRegEx ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( PKG, "RegexEvalHelperDialog.EnterScript.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.EnterScript.Title" ) );
        mb.open();
      }
      return;
    }
    String realValue = null;
    Text control = null;
    switch ( index ) {
      case 1:
        realValue = Const.NVL( transmeta.environmentSubstitute( wValue1.getText() ), "" );
        control = wValue1;
        break;
      case 2:
        realValue = Const.NVL( transmeta.environmentSubstitute( wValue2.getText() ), "" );
        control = wValue2;
        break;
      case 3:
        realValue = Const.NVL( transmeta.environmentSubstitute( wValue3.getText() ), "" );
        control = wValue3;
        break;
      case 4:
        realValue = Const.NVL( transmeta.environmentSubstitute( wValueGroup.getText() ), "" );
        control = wValueGroup;
        break;
      default:
        break;
    }
    try {
      Pattern p;
      if ( isCanonicalEqualityFlagSet() ) {
        p = Pattern.compile( regexoptions + realScript, Pattern.CANON_EQ );
      } else {
        p = Pattern.compile( regexoptions + realScript );
      }
      Matcher m = p.matcher( realValue );
      boolean ismatch = m.matches();
      if ( ismatch ) {
        control.setBackground( guiresource.getColorGreen() );
      } else {
        control.setBackground( guiresource.getColorRed() );
      }

      if ( index == 4 ) {
        wGroups.removeAll();
        int nrFields = m.groupCount();
        int nr = 0;
        for ( int i = 1; i <= nrFields; i++ ) {
          if ( m.group( i ) == null ) {
            wGroups.add( "" );
          } else {
            wGroups.add( m.group( i ) );
          }
          nr++;
        }
        wlGroups.setText( BaseMessages.getString( PKG, "RegexEvalHelperDialog.FieldsGroup", nr ) );
      }
      wRegExScriptCompile.setForeground( guiresource.getColorBlue() );
      wRegExScriptCompile.setText( BaseMessages
        .getString( PKG, "RegexEvalHelperDialog.ScriptSuccessfullyCompiled" ) );
      wRegExScriptCompile.setToolTipText( "" );
    } catch ( Exception e ) {
      if ( !errorDisplayed ) {
        wRegExScriptCompile.setForeground( guiresource.getColorRed() );
        wRegExScriptCompile.setText( e.getMessage() );
        wRegExScriptCompile.setToolTipText( BaseMessages.getString(
          PKG, "RegexEvalHelperDialog.ErrorCompiling.Message" )
          + Const.CR + e.toString() );
        this.errorDisplayed = true;
      }
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    if ( regexscript != null ) {
      wRegExScript.setText( regexscript );
    }
  }

  private void cancel() {
    dispose();
  }

  private void ok() {
    if ( wRegExScript.getText() != null ) {
      regexscript = wRegExScript.getText();
    }

    dispose();
  }

}

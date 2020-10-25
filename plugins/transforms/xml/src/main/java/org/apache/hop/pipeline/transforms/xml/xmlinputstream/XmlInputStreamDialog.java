/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.xml.xmlinputstream;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

//TODO correct sizing of window

public class XmlInputStreamDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = XmlInputStreamMeta.class; // for i18n purposes, needed by Translator2!!

  // for tabs later on:
  // private CTabFolder wTabFolder;
  // private FormData fdTabFolder;
  //
  // private CTabItem wFileTab;
  // private CTabItem wContentTab;
  // private CTabItem wFieldsTab;

  private TextVar wFilename;

  private CCombo wFilenameCombo;

  private Button wbbFilename; // Browse for a file

  private Button wAddResult;

  private Button cbFromSource;

  private TextVar wRowsToSkip;

  private CCombo cbSourceField;

  private TextVar wLimit;

  private TextVar wDefaultStringLen;

  private TextVar wEncoding;

  private Button wEnableNamespaces;

  private Button wEnableTrim;

  private Button wIncludeFilename;
  private Text wFilenameField;

  private Button wIncludeRowNumber;
  private Text wRowNumberField;

  private Button wIncludeXmlDataTypeNumeric;
  private Text wXmlDataTypeNumericField;

  private Button wIncludeXmlDataTypeDescription;
  private Text wXmlDataTypeDescriptionField;

  private Button wIncludeXmlLocationLine;
  private Text wXmlLocationLineField;

  private Button wIncludeXmlLocationColumn;
  private Text wXmlLocationColumnField;

  private Button wIncludeXmlElementID;
  private Text wXmlElementIDField;

  private Button wIncludeXmlParentElementID;
  private Text wXmlParentElementIDField;

  private Button wIncludeXmlElementLevel;
  private Text wXmlElementLevelField;

  private Button wIncludeXmlPath;
  private Text wXmlPathField;

  private Button wIncludeXmlParentPath;
  private Text wXmlParentPathField;

  private Button wIncludeXmlDataName;
  private Text wXmlDataNameField;

  private Button wIncludeXmlDataValue;
  private Text wXmlDataValueField;

  private final XmlInputStreamMeta inputMeta;

  private boolean isReceivingInput;

  public XmlInputStreamDialog(Shell parent, Object in, PipelineMeta tr, String sname ) {
    super( parent, (BaseTransformMeta) in, tr, sname );
    inputMeta = (XmlInputStreamMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    props.setLook( shell );
    setShellImage( shell, inputMeta );

    ModifyListener lsMod = e -> inputMeta.setChanged();
    changed = inputMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Shell.Text" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transform name line
    //
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    // split into tabs for better overview, later on:
    // wTabFolder = new CTabFolder(shell, SWT.BORDER);
    // props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
    //
    //
    // addFilesTab();
    // addContentTab();
    // addAdditionalFieldsTab();
    //
    // fdTabFolder = new FormData();
    // fdTabFolder.left = new FormAttachment(0, 0);
    // fdTabFolder.top = new FormAttachment(wTransformName, margin);
    // fdTabFolder.right = new FormAttachment(100, 0);
    // fdTabFolder.bottom= new FormAttachment(100, -50);
    // wTabFolder.setLayoutData(fdTabFolder);

    // See if the step receives input. If so, we don't ask for the filename, but
    // for the filename field.
    //
    isReceivingInput = pipelineMeta.findNrPrevTransforms( transformMeta ) > 0;
    if ( isReceivingInput ) {

      IRowMeta previousFields;
      try {
        previousFields = pipelineMeta.getPrevTransformFields( transformMeta );
      } catch ( HopTransformException e ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "XMLInputStreamDialog.ErrorDialog.UnableToGetInputFields.Title" ),
          BaseMessages.getString( PKG, "XMLInputStreamDialog.ErrorDialog.UnableToGetInputFields.Message" ), e );
        previousFields = new RowMeta();
      }

      // The field itself...
      //
      Label wlFilename = new Label( shell, SWT.RIGHT );
      wlFilename.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Filename.Label" ) );
      props.setLook( wlFilename );
      FormData fdlFilename = new FormData();
      fdlFilename.top = new FormAttachment( lastControl, margin );
      fdlFilename.left = new FormAttachment( 0, 0 );
      fdlFilename.right = new FormAttachment( middle, -margin );
      wlFilename.setLayoutData( fdlFilename );
      wFilenameCombo = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      wFilenameCombo.setItems( previousFields.getFieldNames() );
      props.setLook( wFilenameCombo );
      wFilenameCombo.addModifyListener( lsMod );
      FormData fdFilename = new FormData();
      fdFilename.top = new FormAttachment( lastControl, margin );
      fdFilename.left = new FormAttachment( middle, 0 );
      fdFilename.right = new FormAttachment( 100, -margin );
      wFilenameCombo.setLayoutData( fdFilename );
      lastControl = wFilenameCombo;
    } else {
      // Filename...
      //
      // The filename browse button
      //
      wbbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
      props.setLook( wbbFilename );
      wbbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
      wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
      FormData fdbFilename = new FormData();
      fdbFilename.top = new FormAttachment( lastControl, margin );
      fdbFilename.right = new FormAttachment( 100, 0 );
      wbbFilename.setLayoutData( fdbFilename );

      // The field itself...
      //
      Label wlFilename = new Label( shell, SWT.RIGHT );
      wlFilename.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Filename.Label" ) );
      props.setLook( wlFilename );
      FormData fdlFilename = new FormData();
      fdlFilename.top = new FormAttachment( lastControl, margin );
      fdlFilename.left = new FormAttachment( 0, 0 );
      fdlFilename.right = new FormAttachment( middle, -margin );
      wlFilename.setLayoutData( fdlFilename );
      wFilename = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      props.setLook( wFilename );
      wFilename.addModifyListener( lsMod );
      FormData fdFilename = new FormData();
      fdFilename.top = new FormAttachment( lastControl, margin );
      fdFilename.left = new FormAttachment( middle, 0 );
      fdFilename.right = new FormAttachment( wbbFilename, -margin );
      wFilename.setLayoutData( fdFilename );
      lastControl = wFilename;
    }
    // data from previous step
    Label lblAcceptingFilenames = new Label( shell, SWT.RIGHT );
    lblAcceptingFilenames.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.SourceStreamField.Label" ) );
    props.setLook( lblAcceptingFilenames );
    FormData fdlAcceptingFilenames = new FormData();
    fdlAcceptingFilenames.left = new FormAttachment( 0, 0 );
    fdlAcceptingFilenames.top = new FormAttachment( lastControl, margin );
    fdlAcceptingFilenames.right = new FormAttachment( middle, -margin );
    lblAcceptingFilenames.setLayoutData( fdlAcceptingFilenames );
    cbFromSource = new Button( shell, SWT.CHECK );
    props.setLook( cbFromSource );
    fdlAcceptingFilenames = new FormData();
    fdlAcceptingFilenames.left = new FormAttachment( middle, 0 );
    fdlAcceptingFilenames.top = new FormAttachment( lblAcceptingFilenames, 0, SWT.CENTER );
    cbFromSource.setLayoutData( fdlAcceptingFilenames );
    lastControl = cbFromSource;

    // field name
    Label lblAcceptingField = new Label( shell, SWT.RIGHT );
    lblAcceptingField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.SourceField.Label" ) );
    props.setLook( lblAcceptingField );
    FormData fdlAcceptingField = new FormData();
    fdlAcceptingField = new FormData();
    fdlAcceptingField.left = new FormAttachment( 0, 0 );
    fdlAcceptingField.top = new FormAttachment( lastControl, 2*margin );
    fdlAcceptingField.right = new FormAttachment( middle, -margin );
    lblAcceptingField.setLayoutData( fdlAcceptingField );
    cbSourceField = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( cbSourceField );
    cbSourceField.addModifyListener( lsMod );
    fdlAcceptingField = new FormData();
    fdlAcceptingField.left = new FormAttachment( middle, 0 );
    fdlAcceptingField.top = new FormAttachment( lastControl, 2*margin );
    fdlAcceptingField.right = new FormAttachment( 100, 0 );
    cbSourceField.setLayoutData( fdlAcceptingField );
    lastControl = cbSourceField;
    setSourceStreamField();

    // add filename to result?
    //
    Label wlAddResult = new Label( shell, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( lastControl, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( shell, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( PKG, "XMLInputStreamDialog.AddResult.Tooltip" ) );
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wlAddResult, 0, SWT.CENTER );
    wAddResult.setLayoutData( fdAddResult );
    lastControl = wAddResult;

    // RowsToSkip line
    //
    Label wlRowsToSkip = new Label( shell, SWT.RIGHT );
    wlRowsToSkip.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.RowsToSkip.Label" ) );
    props.setLook( wlRowsToSkip );
    FormData fdlRowsToSkip = new FormData();
    fdlRowsToSkip = new FormData();
    fdlRowsToSkip.left = new FormAttachment( 0, 0 );
    fdlRowsToSkip.top = new FormAttachment( lastControl, 2*margin );
    fdlRowsToSkip.right = new FormAttachment( middle, -margin );
    wlRowsToSkip.setLayoutData( fdlRowsToSkip );
    wRowsToSkip = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowsToSkip );
    wRowsToSkip.addModifyListener( lsMod );
    FormData fdRowsToSkip = new FormData();
    fdRowsToSkip = new FormData();
    fdRowsToSkip.left = new FormAttachment( middle, 0 );
    fdRowsToSkip.top = new FormAttachment( lastControl, 2*margin );
    fdRowsToSkip.right = new FormAttachment( 100, 0 );
    wRowsToSkip.setLayoutData( fdRowsToSkip );
    lastControl = wRowsToSkip;

    // Limit line
    //
    Label wlLimit = new Label( shell, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Limit.Label" ) );
    props.setLook( wlLimit );
    FormData fdlLimit = new FormData();
    fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( lastControl, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );
    FormData fdLimit = new FormData();
    fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( lastControl, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );
    lastControl = wLimit;

    // DefaultStringLen line
    //
    Label wlDefaultStringLen = new Label( shell, SWT.RIGHT );
    wlDefaultStringLen.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.DefaultStringLen.Label" ) );
    props.setLook( wlDefaultStringLen );
    FormData fdlDefaultStringLen = new FormData();
    fdlDefaultStringLen = new FormData();
    fdlDefaultStringLen.left = new FormAttachment( 0, 0 );
    fdlDefaultStringLen.top = new FormAttachment( lastControl, margin );
    fdlDefaultStringLen.right = new FormAttachment( middle, -margin );
    wlDefaultStringLen.setLayoutData( fdlDefaultStringLen );
    wDefaultStringLen = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDefaultStringLen );
    wDefaultStringLen.addModifyListener( lsMod );
    FormData fdDefaultStringLen = new FormData();
    fdDefaultStringLen = new FormData();
    fdDefaultStringLen.left = new FormAttachment( middle, 0 );
    fdDefaultStringLen.top = new FormAttachment( lastControl, margin );
    fdDefaultStringLen.right = new FormAttachment( 100, 0 );
    wDefaultStringLen.setLayoutData( fdDefaultStringLen );
    lastControl = wDefaultStringLen;

    // Encoding line
    //
    Label wlEncoding = new Label( shell, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );
    FormData fdlEncoding = new FormData();
    fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( lastControl, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new TextVar( pipelineMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );
    FormData fdEncoding = new FormData();
    fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( lastControl, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData( fdEncoding );
    lastControl = wEncoding;

    // EnableNamespaces?
    //
    Label wlEnableNamespaces = new Label( shell, SWT.RIGHT );
    wlEnableNamespaces.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.EnableNamespaces.Label" ) );
    props.setLook( wlEnableNamespaces );
    FormData fdlEnableNamespaces = new FormData();
    fdlEnableNamespaces.left = new FormAttachment( 0, 0 );
    fdlEnableNamespaces.top = new FormAttachment( lastControl, margin );
    fdlEnableNamespaces.right = new FormAttachment( middle, -margin );
    wlEnableNamespaces.setLayoutData( fdlEnableNamespaces );
    wEnableNamespaces = new Button( shell, SWT.CHECK );
    props.setLook( wEnableNamespaces );
    wEnableNamespaces.setToolTipText( BaseMessages.getString( PKG, "XMLInputStreamDialog.EnableNamespaces.Tooltip" ) );
    FormData fdEnableNamespaces = new FormData();
    fdEnableNamespaces.left = new FormAttachment( middle, 0 );
    fdEnableNamespaces.top = new FormAttachment( wlEnableNamespaces, 0, SWT.CENTER );
    wEnableNamespaces.setLayoutData( fdEnableNamespaces );
    lastControl = wEnableNamespaces;

    // EnableTrim?
    //
    Label wlEnableTrim = new Label( shell, SWT.RIGHT );
    wlEnableTrim.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.EnableTrim.Label" ) );
    props.setLook( wlEnableTrim );
    FormData fdlEnableTrim = new FormData();
    fdlEnableTrim.left = new FormAttachment( 0, 0 );
    fdlEnableTrim.top = new FormAttachment( lastControl, 2*margin );
    fdlEnableTrim.right = new FormAttachment( middle, -margin );
    wlEnableTrim.setLayoutData( fdlEnableTrim );
    wEnableTrim = new Button( shell, SWT.CHECK );
    props.setLook( wEnableTrim );
    wEnableTrim.setToolTipText( BaseMessages.getString( PKG, "XMLInputStreamDialog.EnableTrim.Tooltip" ) );
    FormData fdEnableTrim = new FormData();
    fdEnableTrim.left = new FormAttachment( middle, 0 );
    fdEnableTrim.top = new FormAttachment( wlEnableTrim, 0, SWT.CENTER );
    wEnableTrim.setLayoutData( fdEnableTrim );
    lastControl = wEnableTrim;

    // IncludeFilename?
    //
    Label wlIncludeFilename = new Label( shell, SWT.RIGHT );
    wlIncludeFilename.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeFilename.Label" ) );
    props.setLook( wlIncludeFilename );
    FormData fdlIncludeFilename = new FormData();
    fdlIncludeFilename.top = new FormAttachment( lastControl, margin );
    fdlIncludeFilename.left = new FormAttachment( 0, 0 );
    fdlIncludeFilename.right = new FormAttachment( middle, -margin );
    wlIncludeFilename.setLayoutData( fdlIncludeFilename );
    wIncludeFilename = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeFilename );
    FormData fdIncludeFilename = new FormData();
    fdIncludeFilename.top = new FormAttachment( wlIncludeFilename, 0, SWT.CENTER );
    fdIncludeFilename.left = new FormAttachment( middle, 0 );
    wIncludeFilename.setLayoutData( fdIncludeFilename );

    // FilenameField line
    //
    Label wlFilenameField = new Label( shell, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlFilenameField );
    FormData fdlFilenameField = new FormData();
    fdlFilenameField = new FormData();
    fdlFilenameField.top = new FormAttachment( lastControl, margin );
    fdlFilenameField.left = new FormAttachment( wIncludeFilename, margin );
    wlFilenameField.setLayoutData( fdlFilenameField );
    wFilenameField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilenameField );
    wFilenameField.addModifyListener( lsMod );
    FormData fdFilenameField = new FormData();
    fdFilenameField = new FormData();
    fdFilenameField.top = new FormAttachment( lastControl, margin );
    fdFilenameField.left = new FormAttachment( wlFilenameField, margin );
    fdFilenameField.right = new FormAttachment( 100, 0 );
    wFilenameField.setLayoutData( fdFilenameField );
    lastControl = wFilenameField;

    // IncludeRowNumber?
    //
    Label wlIncludeRowNumber = new Label( shell, SWT.RIGHT );
    wlIncludeRowNumber.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeRowNumber.Label" ) );
    props.setLook( wlIncludeRowNumber );
    FormData fdlIncludeRowNumber = new FormData();
    fdlIncludeRowNumber.top = new FormAttachment( lastControl, margin );
    fdlIncludeRowNumber.left = new FormAttachment( 0, 0 );
    fdlIncludeRowNumber.right = new FormAttachment( middle, -margin );
    wlIncludeRowNumber.setLayoutData( fdlIncludeRowNumber );
    wIncludeRowNumber = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeRowNumber );
    FormData fdIncludeRowNumber = new FormData();
    fdIncludeRowNumber.top = new FormAttachment( wlIncludeRowNumber, 0, SWT.CENTER );
    fdIncludeRowNumber.left = new FormAttachment( middle, 0 );
    wIncludeRowNumber.setLayoutData( fdIncludeRowNumber );

    // RowNumberField line
    //
    Label wlRowNumberField = new Label( shell, SWT.RIGHT );
    wlRowNumberField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlRowNumberField );
    FormData fdlRowNumberField = new FormData();
    fdlRowNumberField = new FormData();
    fdlRowNumberField.top = new FormAttachment( lastControl, margin );
    fdlRowNumberField.left = new FormAttachment( wIncludeRowNumber, margin );
    wlRowNumberField.setLayoutData( fdlRowNumberField );
    wRowNumberField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowNumberField );
    wRowNumberField.addModifyListener( lsMod );
    FormData fdRowNumberField = new FormData();
    fdRowNumberField = new FormData();
    fdRowNumberField.top = new FormAttachment( lastControl, margin );
    fdRowNumberField.left = new FormAttachment( wlRowNumberField, margin );
    fdRowNumberField.right = new FormAttachment( 100, 0 );
    wRowNumberField.setLayoutData( fdRowNumberField );
    lastControl = wRowNumberField;

    // IncludeXmlDataTypeNumeric?
    //
    Label wlIncludeXmlDataTypeNumeric = new Label( shell, SWT.RIGHT );
    wlIncludeXmlDataTypeNumeric.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlDataTypeNumeric.Label" ) );
    props.setLook( wlIncludeXmlDataTypeNumeric );
    FormData fdlIncludeXmlDataTypeNumeric = new FormData();
    fdlIncludeXmlDataTypeNumeric.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlDataTypeNumeric.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlDataTypeNumeric.right = new FormAttachment( middle, -margin );
    wlIncludeXmlDataTypeNumeric.setLayoutData( fdlIncludeXmlDataTypeNumeric );
    wIncludeXmlDataTypeNumeric = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlDataTypeNumeric );
    FormData fdIncludeXmlDataTypeNumeric = new FormData();
    fdIncludeXmlDataTypeNumeric.top = new FormAttachment( wlIncludeXmlDataTypeNumeric, 0, SWT.CENTER );
    fdIncludeXmlDataTypeNumeric.left = new FormAttachment( middle, 0 );
    wIncludeXmlDataTypeNumeric.setLayoutData( fdIncludeXmlDataTypeNumeric );

    // XmlDataTypeNumericField line
    //
    Label wlXmlDataTypeNumericField = new Label( shell, SWT.RIGHT );
    wlXmlDataTypeNumericField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlDataTypeNumericField );
    FormData fdlXmlDataTypeNumericField = new FormData();
    fdlXmlDataTypeNumericField = new FormData();
    fdlXmlDataTypeNumericField.top = new FormAttachment( lastControl, margin );
    fdlXmlDataTypeNumericField.left = new FormAttachment( wIncludeXmlDataTypeNumeric, margin );
    wlXmlDataTypeNumericField.setLayoutData( fdlXmlDataTypeNumericField );
    wXmlDataTypeNumericField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlDataTypeNumericField );
    wXmlDataTypeNumericField.addModifyListener( lsMod );
    FormData fdXmlDataTypeNumericField = new FormData();
    fdXmlDataTypeNumericField = new FormData();
    fdXmlDataTypeNumericField.top = new FormAttachment( lastControl, margin );
    fdXmlDataTypeNumericField.left = new FormAttachment( wlXmlDataTypeNumericField, margin );
    fdXmlDataTypeNumericField.right = new FormAttachment( 100, 0 );
    wXmlDataTypeNumericField.setLayoutData( fdXmlDataTypeNumericField );
    lastControl = wXmlDataTypeNumericField;

    // IncludeXmlDataTypeDescription?
    //
    Label wlIncludeXmlDataTypeDescription = new Label( shell, SWT.RIGHT );
    wlIncludeXmlDataTypeDescription.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlDataTypeDescription.Label" ) );
    props.setLook( wlIncludeXmlDataTypeDescription );
    FormData fdlIncludeXmlDataTypeDescription = new FormData();
    fdlIncludeXmlDataTypeDescription.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlDataTypeDescription.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlDataTypeDescription.right = new FormAttachment( middle, -margin );
    wlIncludeXmlDataTypeDescription.setLayoutData( fdlIncludeXmlDataTypeDescription );
    wIncludeXmlDataTypeDescription = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlDataTypeDescription );
    FormData fdIncludeXmlDataTypeDescription = new FormData();
    fdIncludeXmlDataTypeDescription.top = new FormAttachment( wlIncludeXmlDataTypeDescription, 0, SWT.CENTER );
    fdIncludeXmlDataTypeDescription.left = new FormAttachment( middle, 0 );
    wIncludeXmlDataTypeDescription.setLayoutData( fdIncludeXmlDataTypeDescription );

    // XmlDataTypeDescriptionField line
    //
    Label wlXmlDataTypeDescriptionField = new Label( shell, SWT.RIGHT );
    wlXmlDataTypeDescriptionField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlDataTypeDescriptionField );
    FormData fdlXmlDataTypeDescriptionField = new FormData();
    fdlXmlDataTypeDescriptionField = new FormData();
    fdlXmlDataTypeDescriptionField.top = new FormAttachment( lastControl, margin );
    fdlXmlDataTypeDescriptionField.left = new FormAttachment( wIncludeXmlDataTypeDescription, margin );
    wlXmlDataTypeDescriptionField.setLayoutData( fdlXmlDataTypeDescriptionField );
    wXmlDataTypeDescriptionField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlDataTypeDescriptionField );
    wXmlDataTypeDescriptionField.addModifyListener( lsMod );
    FormData fdXmlDataTypeDescriptionField = new FormData();
    fdXmlDataTypeDescriptionField = new FormData();
    fdXmlDataTypeDescriptionField.top = new FormAttachment( lastControl, margin );
    fdXmlDataTypeDescriptionField.left = new FormAttachment( wlXmlDataTypeDescriptionField, margin );
    fdXmlDataTypeDescriptionField.right = new FormAttachment( 100, 0 );
    wXmlDataTypeDescriptionField.setLayoutData( fdXmlDataTypeDescriptionField );
    lastControl = wXmlDataTypeDescriptionField;

    // IncludeXmlLocationLine?
    //
    Label wlIncludeXmlLocationLine = new Label( shell, SWT.RIGHT );
    wlIncludeXmlLocationLine.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlLocationLine.Label" ) );
    props.setLook( wlIncludeXmlLocationLine );
    FormData fdlIncludeXmlLocationLine = new FormData();
    fdlIncludeXmlLocationLine.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlLocationLine.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlLocationLine.right = new FormAttachment( middle, -margin );
    wlIncludeXmlLocationLine.setLayoutData( fdlIncludeXmlLocationLine );
    wIncludeXmlLocationLine = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlLocationLine );
    FormData fdIncludeXmlLocationLine = new FormData();
    fdIncludeXmlLocationLine.top = new FormAttachment( wlIncludeXmlLocationLine, 0, SWT.CENTER );
    fdIncludeXmlLocationLine.left = new FormAttachment( middle, 0 );
    wIncludeXmlLocationLine.setLayoutData( fdIncludeXmlLocationLine );

    // XmlLocationLineField line
    //
    Label wlXmlLocationLineField = new Label( shell, SWT.RIGHT );
    wlXmlLocationLineField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlLocationLineField );
    FormData fdlXmlLocationLineField = new FormData();
    fdlXmlLocationLineField = new FormData();
    fdlXmlLocationLineField.top = new FormAttachment( lastControl, margin );
    fdlXmlLocationLineField.left = new FormAttachment( wIncludeXmlLocationLine, margin );
    wlXmlLocationLineField.setLayoutData( fdlXmlLocationLineField );
    wXmlLocationLineField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlLocationLineField );
    wXmlLocationLineField.addModifyListener( lsMod );
    FormData fdXmlLocationLineField = new FormData();
    fdXmlLocationLineField = new FormData();
    fdXmlLocationLineField.top = new FormAttachment( lastControl, margin );
    fdXmlLocationLineField.left = new FormAttachment( wlXmlLocationLineField, margin );
    fdXmlLocationLineField.right = new FormAttachment( 100, 0 );
    wXmlLocationLineField.setLayoutData( fdXmlLocationLineField );
    lastControl = wXmlLocationLineField;

    // IncludeXmlLocationColumn?
    //
    Label wlIncludeXmlLocationColumn = new Label( shell, SWT.RIGHT );
    wlIncludeXmlLocationColumn.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlLocationColumn.Label" ) );
    props.setLook( wlIncludeXmlLocationColumn );
    FormData fdlIncludeXmlLocationColumn = new FormData();
    fdlIncludeXmlLocationColumn.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlLocationColumn.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlLocationColumn.right = new FormAttachment( middle, -margin );
    wlIncludeXmlLocationColumn.setLayoutData( fdlIncludeXmlLocationColumn );
    wIncludeXmlLocationColumn = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlLocationColumn );
    FormData fdIncludeXmlLocationColumn = new FormData();
    fdIncludeXmlLocationColumn.top = new FormAttachment( wlIncludeXmlLocationColumn, 0, SWT.CENTER );
    fdIncludeXmlLocationColumn.left = new FormAttachment( middle, 0 );
    wIncludeXmlLocationColumn.setLayoutData( fdIncludeXmlLocationColumn );

    // XmlLocationColumnField line
    //
    Label wlXmlLocationColumnField = new Label( shell, SWT.RIGHT );
    wlXmlLocationColumnField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlLocationColumnField );
    FormData fdlXmlLocationColumnField = new FormData();
    fdlXmlLocationColumnField = new FormData();
    fdlXmlLocationColumnField.top = new FormAttachment( lastControl, margin );
    fdlXmlLocationColumnField.left = new FormAttachment( wIncludeXmlLocationColumn, margin );
    wlXmlLocationColumnField.setLayoutData( fdlXmlLocationColumnField );
    wXmlLocationColumnField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlLocationColumnField );
    wXmlLocationColumnField.addModifyListener( lsMod );
    FormData fdXmlLocationColumnField = new FormData();
    fdXmlLocationColumnField = new FormData();
    fdXmlLocationColumnField.top = new FormAttachment( lastControl, margin );
    fdXmlLocationColumnField.left = new FormAttachment( wlXmlLocationColumnField, margin );
    fdXmlLocationColumnField.right = new FormAttachment( 100, 0 );
    wXmlLocationColumnField.setLayoutData( fdXmlLocationColumnField );
    lastControl = wXmlLocationColumnField;

    // IncludeXmlElementID?
    //
    Label wlIncludeXmlElementID = new Label( shell, SWT.RIGHT );
    wlIncludeXmlElementID.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlElementID.Label" ) );
    props.setLook( wlIncludeXmlElementID );
    FormData fdlIncludeXmlElementID = new FormData();
    fdlIncludeXmlElementID.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlElementID.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlElementID.right = new FormAttachment( middle, -margin );
    wlIncludeXmlElementID.setLayoutData( fdlIncludeXmlElementID );
    wIncludeXmlElementID = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlElementID );
    FormData fdIncludeXmlElementID = new FormData();
    fdIncludeXmlElementID.top = new FormAttachment( wlIncludeXmlElementID, 0, SWT.CENTER );
    fdIncludeXmlElementID.left = new FormAttachment( middle, 0 );
    wIncludeXmlElementID.setLayoutData( fdIncludeXmlElementID );

    // XmlElementIDField line
    //
    Label wlXmlElementIDField = new Label( shell, SWT.RIGHT );
    wlXmlElementIDField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlElementIDField );
    FormData fdlXmlElementIDField = new FormData();
    fdlXmlElementIDField = new FormData();
    fdlXmlElementIDField.top = new FormAttachment( lastControl, margin );
    fdlXmlElementIDField.left = new FormAttachment( wIncludeXmlElementID, margin );
    wlXmlElementIDField.setLayoutData( fdlXmlElementIDField );
    wXmlElementIDField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlElementIDField );
    wXmlElementIDField.addModifyListener( lsMod );
    FormData fdXmlElementIDField = new FormData();
    fdXmlElementIDField = new FormData();
    fdXmlElementIDField.top = new FormAttachment( lastControl, margin );
    fdXmlElementIDField.left = new FormAttachment( wlXmlElementIDField, margin );
    fdXmlElementIDField.right = new FormAttachment( 100, 0 );
    wXmlElementIDField.setLayoutData( fdXmlElementIDField );
    lastControl = wXmlElementIDField;

    // IncludeXmlParentElementID?
    //
    Label wlIncludeXmlParentElementID = new Label( shell, SWT.RIGHT );
    wlIncludeXmlParentElementID.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlParentElementID.Label" ) );
    props.setLook( wlIncludeXmlParentElementID );
    FormData fdlIncludeXmlParentElementID = new FormData();
    fdlIncludeXmlParentElementID.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlParentElementID.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlParentElementID.right = new FormAttachment( middle, -margin );
    wlIncludeXmlParentElementID.setLayoutData( fdlIncludeXmlParentElementID );
    wIncludeXmlParentElementID = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlParentElementID );
    FormData fdIncludeXmlParentElementID = new FormData();
    fdIncludeXmlParentElementID.top = new FormAttachment( wlIncludeXmlParentElementID, 0, SWT.CENTER );
    fdIncludeXmlParentElementID.left = new FormAttachment( middle, 0 );
    wIncludeXmlParentElementID.setLayoutData( fdIncludeXmlParentElementID );

    // XmlParentElementIDField line
    //
    Label wlXmlParentElementIDField = new Label( shell, SWT.RIGHT );
    wlXmlParentElementIDField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlParentElementIDField );
    FormData fdlXmlParentElementIDField = new FormData();
    fdlXmlParentElementIDField = new FormData();
    fdlXmlParentElementIDField.top = new FormAttachment( lastControl, margin );
    fdlXmlParentElementIDField.left = new FormAttachment( wIncludeXmlParentElementID, margin );
    wlXmlParentElementIDField.setLayoutData( fdlXmlParentElementIDField );
    wXmlParentElementIDField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlParentElementIDField );
    wXmlParentElementIDField.addModifyListener( lsMod );
    FormData fdXmlParentElementIDField = new FormData();
    fdXmlParentElementIDField = new FormData();
    fdXmlParentElementIDField.top = new FormAttachment( lastControl, margin );
    fdXmlParentElementIDField.left = new FormAttachment( wlXmlParentElementIDField, margin );
    fdXmlParentElementIDField.right = new FormAttachment( 100, 0 );
    wXmlParentElementIDField.setLayoutData( fdXmlParentElementIDField );
    lastControl = wXmlParentElementIDField;

    // IncludeXmlElementLevel?
    //
    Label wlIncludeXmlElementLevel = new Label( shell, SWT.RIGHT );
    wlIncludeXmlElementLevel.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlElementLevel.Label" ) );
    props.setLook( wlIncludeXmlElementLevel );
    FormData fdlIncludeXmlElementLevel = new FormData();
    fdlIncludeXmlElementLevel.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlElementLevel.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlElementLevel.right = new FormAttachment( middle, -margin );
    wlIncludeXmlElementLevel.setLayoutData( fdlIncludeXmlElementLevel );
    wIncludeXmlElementLevel = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlElementLevel );
    FormData fdIncludeXmlElementLevel = new FormData();
    fdIncludeXmlElementLevel.top = new FormAttachment( wlIncludeXmlElementLevel, 0, SWT.CENTER );
    fdIncludeXmlElementLevel.left = new FormAttachment( middle, 0 );
    wIncludeXmlElementLevel.setLayoutData( fdIncludeXmlElementLevel );

    // XmlElementLevelField line
    //
    Label wlXmlElementLevelField = new Label( shell, SWT.RIGHT );
    wlXmlElementLevelField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlElementLevelField );
    FormData fdlXmlElementLevelField = new FormData();
    fdlXmlElementLevelField = new FormData();
    fdlXmlElementLevelField.top = new FormAttachment( lastControl, margin );
    fdlXmlElementLevelField.left = new FormAttachment( wIncludeXmlElementLevel, margin );
    wlXmlElementLevelField.setLayoutData( fdlXmlElementLevelField );
    wXmlElementLevelField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlElementLevelField );
    wXmlElementLevelField.addModifyListener( lsMod );
    FormData fdXmlElementLevelField = new FormData();
    fdXmlElementLevelField = new FormData();
    fdXmlElementLevelField.top = new FormAttachment( lastControl, margin );
    fdXmlElementLevelField.left = new FormAttachment( wlXmlElementLevelField, margin );
    fdXmlElementLevelField.right = new FormAttachment( 100, 0 );
    wXmlElementLevelField.setLayoutData( fdXmlElementLevelField );
    lastControl = wXmlElementLevelField;

    // IncludeXmlPath?
    //
    Label wlIncludeXmlPath = new Label( shell, SWT.RIGHT );
    wlIncludeXmlPath.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlPath.Label" ) );
    props.setLook( wlIncludeXmlPath );
    FormData fdlIncludeXmlPath = new FormData();
    fdlIncludeXmlPath.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlPath.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlPath.right = new FormAttachment( middle, -margin );
    wlIncludeXmlPath.setLayoutData( fdlIncludeXmlPath );
    wIncludeXmlPath = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlPath );
    FormData fdIncludeXmlPath = new FormData();
    fdIncludeXmlPath.top = new FormAttachment( wlIncludeXmlPath, 0, SWT.CENTER );
    fdIncludeXmlPath.left = new FormAttachment( middle, 0 );
    wIncludeXmlPath.setLayoutData( fdIncludeXmlPath );

    // XmlPathField line
    //
    Label wlXmlPathField = new Label( shell, SWT.RIGHT );
    wlXmlPathField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlPathField );
    FormData fdlXmlPathField = new FormData();
    fdlXmlPathField = new FormData();
    fdlXmlPathField.top = new FormAttachment( lastControl, margin );
    fdlXmlPathField.left = new FormAttachment( wIncludeXmlPath, margin );
    wlXmlPathField.setLayoutData( fdlXmlPathField );
    wXmlPathField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlPathField );
    wXmlPathField.addModifyListener( lsMod );
    FormData fdXmlPathField = new FormData();
    fdXmlPathField = new FormData();
    fdXmlPathField.top = new FormAttachment( lastControl, margin );
    fdXmlPathField.left = new FormAttachment( wlXmlPathField, margin );
    fdXmlPathField.right = new FormAttachment( 100, 0 );
    wXmlPathField.setLayoutData( fdXmlPathField );
    lastControl = wXmlPathField;

    // IncludeXmlParentPath?
    //
    Label wlIncludeXmlParentPath = new Label( shell, SWT.RIGHT );
    wlIncludeXmlParentPath.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlParentPath.Label" ) );
    props.setLook( wlIncludeXmlParentPath );
    FormData fdlIncludeXmlParentPath = new FormData();
    fdlIncludeXmlParentPath.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlParentPath.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlParentPath.right = new FormAttachment( middle, -margin );
    wlIncludeXmlParentPath.setLayoutData( fdlIncludeXmlParentPath );
    wIncludeXmlParentPath = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlParentPath );
    FormData fdIncludeXmlParentPath = new FormData();
    fdIncludeXmlParentPath.top = new FormAttachment( wlIncludeXmlParentPath, 0, SWT.CENTER );
    fdIncludeXmlParentPath.left = new FormAttachment( middle, 0 );
    wIncludeXmlParentPath.setLayoutData( fdIncludeXmlParentPath );

    // XmlParentPathField line
    //
    Label wlXmlParentPathField = new Label( shell, SWT.RIGHT );
    wlXmlParentPathField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlParentPathField );
    FormData fdlXmlParentPathField = new FormData();
    fdlXmlParentPathField = new FormData();
    fdlXmlParentPathField.top = new FormAttachment( lastControl, margin );
    fdlXmlParentPathField.left = new FormAttachment( wIncludeXmlParentPath, margin );
    wlXmlParentPathField.setLayoutData( fdlXmlParentPathField );
    wXmlParentPathField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlParentPathField );
    wXmlParentPathField.addModifyListener( lsMod );
    FormData fdXmlParentPathField = new FormData();
    fdXmlParentPathField = new FormData();
    fdXmlParentPathField.top = new FormAttachment( lastControl, margin );
    fdXmlParentPathField.left = new FormAttachment( wlXmlParentPathField, margin );
    fdXmlParentPathField.right = new FormAttachment( 100, 0 );
    wXmlParentPathField.setLayoutData( fdXmlParentPathField );
    lastControl = wXmlParentPathField;

    // IncludeXmlDataName?
    //
    Label wlIncludeXmlDataName = new Label( shell, SWT.RIGHT );
    wlIncludeXmlDataName.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlDataName.Label" ) );
    props.setLook( wlIncludeXmlDataName );
    FormData fdlIncludeXmlDataName = new FormData();
    fdlIncludeXmlDataName.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlDataName.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlDataName.right = new FormAttachment( middle, -margin );
    wlIncludeXmlDataName.setLayoutData( fdlIncludeXmlDataName );
    wIncludeXmlDataName = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlDataName );
    FormData fdIncludeXmlDataName = new FormData();
    fdIncludeXmlDataName.top = new FormAttachment( wlIncludeXmlDataName, 0, SWT.CENTER );
    fdIncludeXmlDataName.left = new FormAttachment( middle, 0 );
    wIncludeXmlDataName.setLayoutData( fdIncludeXmlDataName );

    // XmlDataNameField line
    //
    Label wlXmlDataNameField = new Label( shell, SWT.RIGHT );
    wlXmlDataNameField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlDataNameField );
    FormData fdlXmlDataNameField = new FormData();
    fdlXmlDataNameField = new FormData();
    fdlXmlDataNameField.top = new FormAttachment( lastControl, margin );
    fdlXmlDataNameField.left = new FormAttachment( wIncludeXmlDataName, margin );
    wlXmlDataNameField.setLayoutData( fdlXmlDataNameField );
    wXmlDataNameField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlDataNameField );
    wXmlDataNameField.addModifyListener( lsMod );
    FormData fdXmlDataNameField = new FormData();
    fdXmlDataNameField = new FormData();
    fdXmlDataNameField.top = new FormAttachment( lastControl, margin );
    fdXmlDataNameField.left = new FormAttachment( wlXmlDataNameField, margin );
    fdXmlDataNameField.right = new FormAttachment( 100, 0 );
    wXmlDataNameField.setLayoutData( fdXmlDataNameField );
    lastControl = wXmlDataNameField;

    // IncludeXmlDataValue?
    //
    Label wlIncludeXmlDataValue = new Label( shell, SWT.RIGHT );
    wlIncludeXmlDataValue.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.IncludeXmlDataValue.Label" ) );
    props.setLook( wlIncludeXmlDataValue );
    FormData fdlIncludeXmlDataValue = new FormData();
    fdlIncludeXmlDataValue.top = new FormAttachment( lastControl, margin );
    fdlIncludeXmlDataValue.left = new FormAttachment( 0, 0 );
    fdlIncludeXmlDataValue.right = new FormAttachment( middle, -margin );
    wlIncludeXmlDataValue.setLayoutData( fdlIncludeXmlDataValue );
    wIncludeXmlDataValue = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeXmlDataValue );
    FormData fdIncludeXmlDataValue = new FormData();
    fdIncludeXmlDataValue.top = new FormAttachment( wlIncludeXmlDataValue, 0, SWT.CENTER );
    fdIncludeXmlDataValue.left = new FormAttachment( middle, 0 );
    wIncludeXmlDataValue.setLayoutData( fdIncludeXmlDataValue );

    // XmlDataValueField line
    //
    Label wlXmlDataValueField = new Label( shell, SWT.RIGHT );
    wlXmlDataValueField.setText( BaseMessages.getString( PKG, "XMLInputStreamDialog.Fieldname.Label" ) );
    props.setLook( wlXmlDataValueField );
    FormData fdlXmlDataValueField = new FormData();
    fdlXmlDataValueField = new FormData();
    fdlXmlDataValueField.top = new FormAttachment( lastControl, margin );
    fdlXmlDataValueField.left = new FormAttachment( wIncludeXmlDataValue, margin );
    wlXmlDataValueField.setLayoutData( fdlXmlDataValueField );
    wXmlDataValueField = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wXmlDataValueField );
    wXmlDataValueField.addModifyListener( lsMod );
    FormData fdXmlDataValueField = new FormData();
    fdXmlDataValueField = new FormData();
    fdXmlDataValueField.top = new FormAttachment( lastControl, margin );
    fdXmlDataValueField.left = new FormAttachment( wlXmlDataValueField, margin );
    fdXmlDataValueField.right = new FormAttachment( 100, 0 );
    wXmlDataValueField.setLayoutData( fdXmlDataValueField );
    lastControl = wXmlDataValueField;

    // Some buttons first, so that the dialog scales nicely...
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wPreview, wCancel }, margin, lastControl );

    // Add listeners
    lsCancel = e -> cancel();
    lsOk = e -> ok();
    lsPreview = e -> preview();

    wCancel.addListener( SWT.Selection, lsCancel );
    wOk.addListener( SWT.Selection, lsOk );
    wPreview.addListener( SWT.Selection, lsPreview );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    if ( isReceivingInput ) {
      wFilenameCombo.addSelectionListener( lsDef );
    } else {
      wFilename.addSelectionListener( lsDef );
      // Listen to the browse button next to the file name
      wbbFilename.addSelectionListener( new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent e ) {
          FileDialog dialog = new FileDialog( shell, SWT.OPEN );
          dialog.setFilterExtensions( new String[] { "*.xml;*.XML", "*" } );
          if ( wFilename.getText() != null ) {
            String fname = pipelineMeta.environmentSubstitute( wFilename.getText() );
            dialog.setFileName( fname );
          }

          dialog.setFilterNames( new String[] {
            BaseMessages.getString( PKG, "System.FileType.XMLFiles" ),
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

          if ( dialog.open() != null ) {
            String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
            wFilename.setText( str );
          }
        }
      } );
    }

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    inputMeta.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setSourceStreamField() {
    try {
      String value = cbSourceField.getText();
      cbSourceField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        cbSourceField.setItems( r.getFieldNames() );
      }
      if ( value != null ) {
        cbSourceField.setText( value );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "XMLInputStreamDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "XMLInputStreamDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  // private void addFilesTab()
  // {
  // //////////////////////////
  // // START OF FILE TAB ///
  // //////////////////////////###
  //
  // wFileTab=new CTabItem(wTabFolder, SWT.NONE);
  // wFileTab.setText(BaseMessages.getString(PKG, "TextFileInputDialog.FileTab.TabTitle"));
  //
  // wFileSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
  // wFileSComp.setLayout(new FillLayout());
  //
  // wFileComp = new Composite(wFileSComp, SWT.NONE );
  // props.setLook(wFileComp);
  //
  // FormLayout fileLayout = new FormLayout();
  // fileLayout.marginWidth = 3;
  // fileLayout.marginHeight = 3;
  // wFileComp.setLayout(fileLayout);
  //
  // //###
  // }
  //
  // private void addContentTab()
  // {
  // //////////////////////////
  // // START OF CONTENT TAB///
  // ///
  // wContentTab=new CTabItem(wTabFolder, SWT.NONE);
  // wContentTab.setText(BaseMessages.getString(PKG, "TextFileInputDialog.ContentTab.TabTitle"));
  //
  // FormLayout contentLayout = new FormLayout ();
  // contentLayout.marginWidth = 3;
  // contentLayout.marginHeight = 3;
  //
  // wContentSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
  // wContentSComp.setLayout(new FillLayout());
  //
  // wContentComp = new Composite(wContentSComp, SWT.NONE );
  // props.setLook(wContentComp);
  // wContentComp.setLayout(contentLayout);
  //
  // //###
  // }
  //
  // private void addAdditionalFieldsTab()
  // {
  // // ////////////////////////
  // // START OF ADDITIONAL FIELDS TAB ///
  // // ////////////////////////
  // wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
  // wAdditionalFieldsTab.setText(BaseMessages.getString(PKG, "TextFileInputDialog.AdditionalFieldsTab.TabTitle"));
  //
  // wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
  // props.setLook(wAdditionalFieldsComp);
  //
  // FormLayout fieldsLayout = new FormLayout();
  // fieldsLayout.marginWidth = 3;
  // fieldsLayout.marginHeight = 3;
  // wAdditionalFieldsComp.setLayout(fieldsLayout);
  //
  // //###
  // }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wTransformName.setText( transformName );
    if ( isReceivingInput ) {
      wFilenameCombo.setText( Const.NVL( inputMeta.getFilename(), "" ) );
    } else {
      wFilename.setText( Const.NVL( inputMeta.getFilename(), "" ) );
    }
    cbFromSource.setSelection( inputMeta.sourceFromInput );
    cbSourceField.setText( Const.NVL( inputMeta.sourceFieldName, "" ) );

    wAddResult.setSelection( inputMeta.isAddResultFile() );
    wRowsToSkip.setText( Const.NVL( inputMeta.getNrRowsToSkip(), "0" ) );
    wLimit.setText( Const.NVL( inputMeta.getRowLimit(), "0" ) );
    wDefaultStringLen.setText( Const.NVL( inputMeta.getDefaultStringLen(), XmlInputStreamMeta.DEFAULT_STRING_LEN ) );
    wEncoding.setText( Const.NVL( inputMeta.getEncoding(), XmlInputStreamMeta.DEFAULT_ENCODING ) );
    wEnableNamespaces.setSelection( inputMeta.isEnableNamespaces() );
    wEnableTrim.setSelection( inputMeta.isEnableTrim() );

    wIncludeFilename.setSelection( inputMeta.isIncludeFilenameField() );
    wFilenameField.setText( Const.NVL( inputMeta.getFilenameField(), "" ) );

    wIncludeRowNumber.setSelection( inputMeta.isIncludeRowNumberField() );
    wRowNumberField.setText( Const.NVL( inputMeta.getRowNumberField(), "" ) );

    wIncludeXmlDataTypeNumeric.setSelection( inputMeta.isIncludeXmlDataTypeNumericField() );
    wXmlDataTypeNumericField.setText( Const.NVL( inputMeta.getXmlDataTypeNumericField(), "" ) );

    wIncludeXmlDataTypeDescription.setSelection( inputMeta.isIncludeXmlDataTypeDescriptionField() );
    wXmlDataTypeDescriptionField.setText( Const.NVL( inputMeta.getXmlDataTypeDescriptionField(), "" ) );

    wIncludeXmlLocationLine.setSelection( inputMeta.isIncludeXmlLocationLineField() );
    wXmlLocationLineField.setText( Const.NVL( inputMeta.getXmlLocationLineField(), "" ) );

    wIncludeXmlLocationColumn.setSelection( inputMeta.isIncludeXmlLocationColumnField() );
    wXmlLocationColumnField.setText( Const.NVL( inputMeta.getXmlLocationColumnField(), "" ) );

    wIncludeXmlElementID.setSelection( inputMeta.isIncludeXmlElementIDField() );
    wXmlElementIDField.setText( Const.NVL( inputMeta.getXmlElementIDField(), "" ) );

    wIncludeXmlParentElementID.setSelection( inputMeta.isIncludeXmlParentElementIDField() );
    wXmlParentElementIDField.setText( Const.NVL( inputMeta.getXmlParentElementIDField(), "" ) );

    wIncludeXmlElementLevel.setSelection( inputMeta.isIncludeXmlElementLevelField() );
    wXmlElementLevelField.setText( Const.NVL( inputMeta.getXmlElementLevelField(), "" ) );

    wIncludeXmlPath.setSelection( inputMeta.isIncludeXmlPathField() );
    wXmlPathField.setText( Const.NVL( inputMeta.getXmlPathField(), "" ) );

    wIncludeXmlParentPath.setSelection( inputMeta.isIncludeXmlParentPathField() );
    wXmlParentPathField.setText( Const.NVL( inputMeta.getXmlParentPathField(), "" ) );

    wIncludeXmlDataName.setSelection( inputMeta.isIncludeXmlDataNameField() );
    wXmlDataNameField.setText( Const.NVL( inputMeta.getXmlDataNameField(), "" ) );

    wIncludeXmlDataValue.setSelection( inputMeta.isIncludeXmlDataValueField() );
    wXmlDataValueField.setText( Const.NVL( inputMeta.getXmlDataValueField(), "" ) );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    inputMeta.setChanged( backupChanged );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( inputMeta );

    dispose();
  }

  private void getInfo( XmlInputStreamMeta xmlInputMeta ) {

    if ( isReceivingInput ) {
      xmlInputMeta.setFilename( wFilenameCombo.getText() );
    } else {
      xmlInputMeta.setFilename( wFilename.getText() );
    }
    xmlInputMeta.sourceFromInput = cbFromSource.getSelection();
    xmlInputMeta.sourceFieldName = cbSourceField.getText();

    xmlInputMeta.setAddResultFile( wAddResult.getSelection() );
    xmlInputMeta.setNrRowsToSkip( Const.NVL( wRowsToSkip.getText(), "0" ) );
    xmlInputMeta.setRowLimit( Const.NVL( wLimit.getText(), "0" ) );
    xmlInputMeta.setDefaultStringLen( Const.NVL( wDefaultStringLen.getText(), XmlInputStreamMeta.DEFAULT_STRING_LEN ) );
    xmlInputMeta.setEncoding( Const.NVL( wEncoding.getText(), XmlInputStreamMeta.DEFAULT_ENCODING ) );
    xmlInputMeta.setEnableNamespaces( wEnableNamespaces.getSelection() );
    xmlInputMeta.setEnableTrim( wEnableTrim.getSelection() );

    xmlInputMeta.setIncludeFilenameField( wIncludeFilename.getSelection() );
    xmlInputMeta.setFilenameField( wFilenameField.getText() );

    xmlInputMeta.setIncludeRowNumberField( wIncludeRowNumber.getSelection() );
    xmlInputMeta.setRowNumberField( wRowNumberField.getText() );

    xmlInputMeta.setIncludeXmlDataTypeNumericField( wIncludeXmlDataTypeNumeric.getSelection() );
    xmlInputMeta.setXmlDataTypeNumericField( wXmlDataTypeNumericField.getText() );

    xmlInputMeta.setIncludeXmlDataTypeDescriptionField( wIncludeXmlDataTypeDescription.getSelection() );
    xmlInputMeta.setXmlDataTypeDescriptionField( wXmlDataTypeDescriptionField.getText() );

    xmlInputMeta.setIncludeXmlLocationLineField( wIncludeXmlLocationLine.getSelection() );
    xmlInputMeta.setXmlLocationLineField( wXmlLocationLineField.getText() );

    xmlInputMeta.setIncludeXmlLocationColumnField( wIncludeXmlLocationColumn.getSelection() );
    xmlInputMeta.setXmlLocationColumnField( wXmlLocationColumnField.getText() );

    xmlInputMeta.setIncludeXmlElementIDField( wIncludeXmlElementID.getSelection() );
    xmlInputMeta.setXmlElementIDField( wXmlElementIDField.getText() );

    xmlInputMeta.setIncludeXmlParentElementIDField( wIncludeXmlParentElementID.getSelection() );
    xmlInputMeta.setXmlParentElementIDField( wXmlParentElementIDField.getText() );

    xmlInputMeta.setIncludeXmlElementLevelField( wIncludeXmlElementLevel.getSelection() );
    xmlInputMeta.setXmlElementLevelField( wXmlElementLevelField.getText() );

    xmlInputMeta.setIncludeXmlPathField( wIncludeXmlPath.getSelection() );
    xmlInputMeta.setXmlPathField( wXmlPathField.getText() );

    xmlInputMeta.setIncludeXmlParentPathField( wIncludeXmlParentPath.getSelection() );
    xmlInputMeta.setXmlParentPathField( wXmlParentPathField.getText() );

    xmlInputMeta.setIncludeXmlDataNameField( wIncludeXmlDataName.getSelection() );
    xmlInputMeta.setXmlDataNameField( wXmlDataNameField.getText() );

    xmlInputMeta.setIncludeXmlDataValueField( wIncludeXmlDataValue.getSelection() );
    xmlInputMeta.setXmlDataValueField( wXmlDataValueField.getText() );

    xmlInputMeta.setChanged();
  }

  // Preview the data
  private void preview() {
    // execute a complete preview transformation in the background.
    // This is how we do it...
    //
    XmlInputStreamMeta oneMeta = new XmlInputStreamMeta();
    getInfo( oneMeta );

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( pipelineMeta, metadataProvider, oneMeta, wTransformName.getText() );

    EnterNumberDialog numberDialog =
        new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString( PKG,
            "XMLInputStreamDialog.Dialog.EnterPreviewSize.Title" ), BaseMessages.getString( PKG,
            "XMLInputStreamDialog.Dialog.EnterPreviewSize.Message" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog( shell, previewMeta, new String[] { wTransformName.getText() },
              new int[] { previewSize } );
      progressDialog.open();

      Pipeline trans = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
              new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ),
                  BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
          new PreviewRowsDialog( shell, pipelineMeta, SWT.NONE, wTransformName.getText(), progressDialog
              .getPreviewRowsMeta( wTransformName.getText() ), progressDialog.getPreviewRows( wTransformName.getText() ),
              loggingText );
      prd.open();
    }
  }

}

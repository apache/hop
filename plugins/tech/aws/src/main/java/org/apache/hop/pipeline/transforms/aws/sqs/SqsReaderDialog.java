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

package org.apache.hop.pipeline.transforms.aws.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.sqs.AmazonSQS;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class SqsReaderDialog extends BaseTransformDialog {

  /**
   * The PKG member is used when looking up internationalized strings. The properties file with
   * localized keys is expected to reside in {the package of the class
   * specified}/messages/messages_{locale}.properties
   */
  private static final Class<?> PKG = SqsReaderMeta.class; // for i18n purposes

  // this is the object the stores the transform's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed
  private SqsReaderMeta meta;

  private ComboVar tAWSCredChain;
  private Label lblAWSKey;
  private TextVar tAWSKey;
  private Label lblAWSKeySecret;
  private PasswordTextVar tAWSKeySecret;
  private Label lblAWSRegion;
  private ComboVar tAWSRegion;
  private TextVar tMessageID;
  private TextVar tMessageBody;
  private TextVar tReceiptHandle;
  private TextVar tBodyMD5;
  private ComboVar tMessageDelete;
  private TextVar tSQSQueue;
  private TextVar tMaxMessages;
  private TextVar tSNSMessage;

  /**
   * The constructor should simply invoke super() and save the incoming meta object to a local
   * variable, so it can conveniently read and write settings from/to it.
   *
   * @param parent the SWT shell to open the dialog in
   * @param variables in the meta object holding the transform's settings
   * @param transformMeta transform definition
   * @param pipelineMeta pipeline description
   */
  public SqsReaderDialog(
      Shell parent, IVariables variables, SqsReaderMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    meta = transformMeta;
  }

  /**
   * This method is called by Hop Gui when the user opens the settings dialog of the transform. It
   * should open the dialog and return only once the dialog has been closed by the user.
   *
   * <p>If the user confirms the dialog, the meta object (passed in the constructor) must be updated
   * to reflect the new transform settings. The changed flag of the meta object must reflect whether
   * the transform configuration was changed by the dialog.
   *
   * <p>If the user cancels the dialog, the meta object must not be updated, and its changed flag
   * must remain unaltered.
   *
   * <p>The open() method must return the name of the transform after the user has confirmed the
   * dialog, or null if the user cancelled the dialog.
   */
  public String open() {

    // store some convenient SWT variables
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // SWT code for preparing the dialog
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, meta);

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseTransformDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod = e -> meta.setChanged();

    // ------------------------------------------------------- //
    // SWT code for building the actual settings dialog        //
    // ------------------------------------------------------- //
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SQSReader.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // transformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // ------------------------------------------------------- //
    // TABULATOREN START //
    // ------------------------------------------------------- //

    // TABS - ANFANG
    // text field holding the name of the field to add to the row stream
    CTabFolder tabFolder = new CTabFolder(shell, SWT.BORDER);
    FormData fdTabFolder = new FormData();
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    tabFolder.setLayoutData(fdTabFolder);
    PropsUi.setLook(tabFolder);

    // ------------------------------------------------------- //
    // - TAB Settings START //
    // ------------------------------------------------------- //

    // Settings-TAB - ANFANG
    CTabItem tbtmSettings = new CTabItem(tabFolder, SWT.NONE);
    tbtmSettings.setText(BaseMessages.getString(PKG, "SQSReaderTransform.Settings.Title"));

    ScrolledComposite scrlSettingsComp =
        new ScrolledComposite(tabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrlSettingsComp.setLayout(new FillLayout());
    PropsUi.setLook(scrlSettingsComp);

    Composite settingsComp = new Composite(scrlSettingsComp, SWT.NONE);
    PropsUi.setLook(settingsComp);

    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    settingsComp.setLayout(settingsLayout);

    // Use AWS Credentials Provider Chain
    // Credentials Chain
    Label lblAWSCredChain = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSCredChain);
    FormData fdLblAWSCredChain = new FormData();
    fdLblAWSCredChain.left = new FormAttachment(0, 0);
    fdLblAWSCredChain.top = new FormAttachment(0, margin);
    fdLblAWSCredChain.right = new FormAttachment(middle, -margin);
    lblAWSCredChain.setLayoutData(fdLblAWSCredChain);
    lblAWSCredChain.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSCredChain.Label"));

    tAWSCredChain = new ComboVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSCredChain);
    FormData fdTAWSCredChain = new FormData();
    fdTAWSCredChain.top = new FormAttachment(0, margin);
    fdTAWSCredChain.left = new FormAttachment(middle, 0);
    fdTAWSCredChain.right = new FormAttachment(100, 0);
    tAWSCredChain.setLayoutData(fdTAWSCredChain);
    tAWSCredChain.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSCredChain.Tooltip"));
    tAWSCredChain.addModifyListener(arg0 -> changeCredentialChainSelection());

    // AWS Key
    lblAWSKey = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSKey);
    FormData fdLblAWSKey = new FormData();
    fdLblAWSKey.left = new FormAttachment(0, 0);
    fdLblAWSKey.top = new FormAttachment(tAWSCredChain, margin);
    fdLblAWSKey.right = new FormAttachment(middle, -margin);
    lblAWSKey.setLayoutData(fdLblAWSKey);
    lblAWSKey.setText(BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKey.Label"));

    tAWSKey = new TextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSKey);
    FormData fdTAWSKey = new FormData();
    fdTAWSKey.top = new FormAttachment(tAWSCredChain, margin);
    fdTAWSKey.left = new FormAttachment(middle, 0);
    fdTAWSKey.right = new FormAttachment(100, 0);
    tAWSKey.setLayoutData(fdTAWSKey);
    tAWSKey.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKey.Tooltip"));

    // AWS Key Secret
    lblAWSKeySecret = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSKeySecret);
    FormData fdLblAWSKeySecret = new FormData();
    fdLblAWSKeySecret.left = new FormAttachment(0, 0);
    fdLblAWSKeySecret.top = new FormAttachment(tAWSKey, margin);
    fdLblAWSKeySecret.right = new FormAttachment(middle, -margin);
    lblAWSKeySecret.setLayoutData(fdLblAWSKeySecret);
    lblAWSKeySecret.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKeySecret.Label"));

    tAWSKeySecret =
        new PasswordTextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSKeySecret);
    FormData fdTAWSKeySecret = new FormData();
    fdTAWSKeySecret.top = new FormAttachment(tAWSKey, margin);
    fdTAWSKeySecret.left = new FormAttachment(middle, 0);
    fdTAWSKeySecret.right = new FormAttachment(100, 0);
    tAWSKeySecret.setLayoutData(fdTAWSKeySecret);
    tAWSKeySecret.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKeySecret.Tooltip"));

    // AWS Region
    lblAWSRegion = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSRegion);
    FormData fdLblAWSRegion = new FormData();
    fdLblAWSRegion.left = new FormAttachment(0, 0);
    fdLblAWSRegion.top = new FormAttachment(tAWSKeySecret, margin);
    fdLblAWSRegion.right = new FormAttachment(middle, -margin);
    lblAWSRegion.setLayoutData(fdLblAWSRegion);
    lblAWSRegion.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSRegion.Label"));

    tAWSRegion = new ComboVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSRegion);
    FormData fdTAWSRegion = new FormData();
    fdTAWSRegion.top = new FormAttachment(tAWSKeySecret, margin);
    fdTAWSRegion.left = new FormAttachment(middle, 0);
    fdTAWSRegion.right = new FormAttachment(100, 0);
    tAWSRegion.setLayoutData(fdTAWSRegion);
    tAWSRegion.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSRegion.Tooltip"));
    populateAWSRegion(tAWSRegion);

    // SQS Queue
    Label lblSQSQueue = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblSQSQueue);
    FormData fdLblSQSQueue = new FormData();
    fdLblSQSQueue.left = new FormAttachment(0, 0);
    fdLblSQSQueue.top = new FormAttachment(tAWSRegion, margin);
    fdLblSQSQueue.right = new FormAttachment(middle, -margin);
    lblSQSQueue.setLayoutData(fdLblSQSQueue);
    lblSQSQueue.setText(BaseMessages.getString(PKG, "SQSReaderTransform.Settings.SQSQueue.Label"));

    tSQSQueue = new TextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tSQSQueue);
    FormData fdTSQSQueue = new FormData();
    fdTSQSQueue.top = new FormAttachment(tAWSRegion, margin);
    fdTSQSQueue.left = new FormAttachment(middle, 0);
    fdTSQSQueue.right = new FormAttachment(100, 0);
    tSQSQueue.setLayoutData(fdTSQSQueue);
    tSQSQueue.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.SQSQueue.Tooltip"));

    Control[] queueTabList = new Control[] {tAWSCredChain, tAWSKey, tAWSKeySecret, tAWSRegion};
    settingsComp.setTabList(queueTabList);

    settingsComp.pack();
    Rectangle settingsBounds = settingsComp.getBounds();

    scrlSettingsComp.setContent(settingsComp);
    scrlSettingsComp.setExpandHorizontal(true);
    scrlSettingsComp.setExpandVertical(true);
    scrlSettingsComp.setMinWidth(settingsBounds.width);
    scrlSettingsComp.setMinHeight(settingsBounds.height);
    // Settings-TAB - ENDE

    // ------------------------------------------------------- //
    // - TAB Output START //
    // ------------------------------------------------------- //

    // Output-TAB - ANFANG
    CTabItem tbtmReaderOutput = new CTabItem(tabFolder, SWT.NONE);
    tbtmReaderOutput.setText(BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.Title"));

    ScrolledComposite scrlreaderOutputComp =
        new ScrolledComposite(tabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrlreaderOutputComp.setLayout(new FillLayout());
    PropsUi.setLook(scrlreaderOutputComp);

    Composite readerOutputComp = new Composite(scrlreaderOutputComp, SWT.NONE);
    PropsUi.setLook(readerOutputComp);

    FormLayout readerOutputLayout = new FormLayout();
    readerOutputLayout.marginWidth = 3;
    readerOutputLayout.marginHeight = 3;
    readerOutputComp.setLayout(readerOutputLayout);

    // ------------------------------------------------------- //
    // --- GROUP Output Settings START //
    // ------------------------------------------------------- //

    Group grpOutputSettings = new Group(readerOutputComp, SWT.SHADOW_NONE);
    PropsUi.setLook(grpOutputSettings);
    grpOutputSettings.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.OutputSettings.GroupTitle"));
    FormData fdGrpOutputSettings = new FormData();
    fdGrpOutputSettings.top = new FormAttachment(0, margin);
    fdGrpOutputSettings.left = new FormAttachment(0, margin);
    fdGrpOutputSettings.right = new FormAttachment(100, -margin);
    fdGrpOutputSettings.bottom = new FormAttachment(40, -margin);
    grpOutputSettings.setLayoutData(fdGrpOutputSettings);

    FormLayout outputSettingsLayout = new FormLayout();
    outputSettingsLayout.marginWidth = 10;
    outputSettingsLayout.marginHeight = 10;
    grpOutputSettings.setLayout(outputSettingsLayout);

    // FELDER
    // Message Deletion
    Label lblMessageDelete = new Label(grpOutputSettings, SWT.RIGHT);
    PropsUi.setLook(lblMessageDelete);
    FormData fdLblMessageDelete = new FormData();
    fdLblMessageDelete.left = new FormAttachment(0, 0);
    fdLblMessageDelete.top = new FormAttachment(0, margin);
    fdLblMessageDelete.right = new FormAttachment(middle, -margin);
    lblMessageDelete.setLayoutData(fdLblMessageDelete);
    lblMessageDelete.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageDelete.Label"));

    tMessageDelete = new ComboVar(variables, grpOutputSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tMessageDelete);
    FormData fdTMessageDelete = new FormData();
    fdTMessageDelete.top = new FormAttachment(0, margin);
    fdTMessageDelete.left = new FormAttachment(middle, 0);
    fdTMessageDelete.right = new FormAttachment(100, 0);
    tMessageDelete.setLayoutData(fdTMessageDelete);
    tMessageDelete.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageDelete.Tooltip"));

    // Max Messages
    Label lblMaxMessages = new Label(grpOutputSettings, SWT.RIGHT);
    PropsUi.setLook(lblMaxMessages);
    FormData fdLblMaxMessages = new FormData();
    fdLblMaxMessages.left = new FormAttachment(0, 0);
    fdLblMaxMessages.top = new FormAttachment(tMessageDelete, margin);
    fdLblMaxMessages.right = new FormAttachment(middle, -margin);
    lblMaxMessages.setLayoutData(fdLblMaxMessages);
    lblMaxMessages.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MaxMessages.Label"));

    tMaxMessages = new TextVar(variables, grpOutputSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tMaxMessages);
    FormData fdTMaxMessages = new FormData();
    fdTMaxMessages.top = new FormAttachment(tMessageDelete, margin);
    fdTMaxMessages.left = new FormAttachment(middle, 0);
    fdTMaxMessages.right = new FormAttachment(100, 0);
    tMaxMessages.setLayoutData(fdTMaxMessages);
    tMaxMessages.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MaxMessages.Tooltip"));

    Control[] readerSettingsTabList = new Control[] {tMessageDelete, tMaxMessages};
    grpOutputSettings.setTabList(readerSettingsTabList);

    // ------------------------------------------------------- //
    // --- GROUP Output Fields START //
    // ------------------------------------------------------- //

    Group grpOutputField = new Group(readerOutputComp, SWT.SHADOW_NONE);
    PropsUi.setLook(grpOutputField);
    grpOutputField.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.OutputFields.GroupTitle"));
    FormData fdGrpOutputField = new FormData();
    fdGrpOutputField.top = new FormAttachment(40, margin);
    fdGrpOutputField.left = new FormAttachment(0, margin);
    fdGrpOutputField.right = new FormAttachment(100, -margin);
    fdGrpOutputField.bottom = new FormAttachment(100, -margin);
    grpOutputField.setLayoutData(fdGrpOutputField);

    FormLayout outputFieldsLayout = new FormLayout();
    outputFieldsLayout.marginWidth = 10;
    outputFieldsLayout.marginHeight = 10;
    grpOutputField.setLayout(outputFieldsLayout);

    // FELDER
    // MessageID
    Label lblMessageID = new Label(grpOutputField, SWT.RIGHT);
    PropsUi.setLook(lblMessageID);
    FormData fdLblMessageID = new FormData();
    fdLblMessageID.left = new FormAttachment(0, 0);
    fdLblMessageID.top = new FormAttachment(0, margin);
    fdLblMessageID.right = new FormAttachment(middle, -margin);
    lblMessageID.setLayoutData(fdLblMessageID);
    lblMessageID.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageID.Label"));

    tMessageID = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tMessageID);
    FormData fdTMessageID = new FormData();
    fdTMessageID.top = new FormAttachment(0, margin);
    fdTMessageID.left = new FormAttachment(middle, 0);
    fdTMessageID.right = new FormAttachment(100, 0);
    tMessageID.setLayoutData(fdTMessageID);
    tMessageID.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageID.Tooltip"));

    // MessageBody
    Label lblMessageBody = new Label(grpOutputField, SWT.RIGHT);
    PropsUi.setLook(lblMessageBody);
    FormData fdLblMessageBody = new FormData();
    fdLblMessageBody.left = new FormAttachment(0, 0);
    fdLblMessageBody.top = new FormAttachment(tMessageID, margin);
    fdLblMessageBody.right = new FormAttachment(middle, -margin);
    lblMessageBody.setLayoutData(fdLblMessageBody);
    lblMessageBody.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageBody.Label"));

    tMessageBody = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tMessageBody);
    FormData fdTMessageBody = new FormData();
    fdTMessageBody.top = new FormAttachment(tMessageID, margin);
    fdTMessageBody.left = new FormAttachment(middle, 0);
    fdTMessageBody.right = new FormAttachment(100, 0);
    tMessageBody.setLayoutData(fdTMessageBody);
    tMessageBody.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageBody.Tooltip"));

    // ReceiptHandle
    Label lblReceiptHandle = new Label(grpOutputField, SWT.RIGHT);
    PropsUi.setLook(lblReceiptHandle);
    FormData fdLblReceiptHandle = new FormData();
    fdLblReceiptHandle.left = new FormAttachment(0, 0);
    fdLblReceiptHandle.top = new FormAttachment(tMessageBody, margin);
    fdLblReceiptHandle.right = new FormAttachment(middle, -margin);
    lblReceiptHandle.setLayoutData(fdLblReceiptHandle);
    lblReceiptHandle.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.ReceiptHandle.Label"));

    tReceiptHandle = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tReceiptHandle);
    FormData fdTReceiptHandle = new FormData();
    fdTReceiptHandle.top = new FormAttachment(tMessageBody, margin);
    fdTReceiptHandle.left = new FormAttachment(middle, 0);
    fdTReceiptHandle.right = new FormAttachment(100, 0);
    tReceiptHandle.setLayoutData(fdTReceiptHandle);
    tReceiptHandle.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.ReceiptHandle.Tooltip"));

    // BodyMD5
    Label lblBodyMD5 = new Label(grpOutputField, SWT.RIGHT);
    PropsUi.setLook(lblBodyMD5);
    FormData fdLblBodyMD5 = new FormData();
    fdLblBodyMD5.left = new FormAttachment(0, 0);
    fdLblBodyMD5.top = new FormAttachment(tReceiptHandle, margin);
    fdLblBodyMD5.right = new FormAttachment(middle, -margin);
    lblBodyMD5.setLayoutData(fdLblBodyMD5);
    lblBodyMD5.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.BodyMD5.Label"));

    tBodyMD5 = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tBodyMD5);
    FormData fdTBodyMD5 = new FormData();
    fdTBodyMD5.top = new FormAttachment(tReceiptHandle, margin);
    fdTBodyMD5.left = new FormAttachment(middle, 0);
    fdTBodyMD5.right = new FormAttachment(100, 0);
    tBodyMD5.setLayoutData(fdTBodyMD5);
    tBodyMD5.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.BodyMD5.Tooltip"));

    // SNSMessage
    Label lblSNSMessage = new Label(grpOutputField, SWT.RIGHT);
    PropsUi.setLook(lblSNSMessage);
    FormData fdLblSNSMessage = new FormData();
    fdLblSNSMessage.left = new FormAttachment(0, 0);
    fdLblSNSMessage.top = new FormAttachment(tBodyMD5, margin);
    fdLblSNSMessage.right = new FormAttachment(middle, -margin);
    lblSNSMessage.setLayoutData(fdLblSNSMessage);
    lblSNSMessage.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.SNSMessage.Label"));

    tSNSMessage = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tSNSMessage);
    FormData fdTSNSMessage = new FormData();
    fdTSNSMessage.top = new FormAttachment(tBodyMD5, margin);
    fdTSNSMessage.left = new FormAttachment(middle, 0);
    fdTSNSMessage.right = new FormAttachment(100, 0);
    tSNSMessage.setLayoutData(fdTSNSMessage);
    tSNSMessage.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.SNSMessage.Tooltip"));

    Control[] readerOutputTabList =
        new Control[] {tMessageID, tMessageBody, tReceiptHandle, tBodyMD5, tSNSMessage};
    grpOutputField.setTabList(readerOutputTabList);

    readerOutputComp.pack();
    Rectangle readerOutputBounds = readerOutputComp.getBounds();

    scrlreaderOutputComp.setContent(readerOutputComp);
    scrlreaderOutputComp.setExpandHorizontal(true);
    scrlreaderOutputComp.setExpandVertical(true);
    scrlreaderOutputComp.setMinWidth(readerOutputBounds.width);
    scrlreaderOutputComp.setMinHeight(readerOutputBounds.height);
    // ReaderOutput-TAB - Ende

    scrlSettingsComp.layout();
    tbtmSettings.setControl(scrlSettingsComp);

    scrlreaderOutputComp.layout();
    tbtmReaderOutput.setControl(scrlreaderOutputComp);

    tabFolder.setSelection(0);

    // TABS ENDE

    // OK and cancel buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Add listeners for cancel and OK
    wOk.addListener(SWT.Selection, c -> ok());
    wCancel.addListener(SWT.Selection, c -> cancel());

    // default listener (for hitting "enter")
    SelectionAdapter lsDef =
        new SelectionAdapter() {
          @Override
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wTransformName.addSelectionListener(lsDef);
    tAWSKey.addSelectionListener(lsDef);
    tAWSKeySecret.addSelectionListener(lsDef);
    tAWSRegion.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseTransformDialog
    setSize();

    // populate the dialog with the values from the meta object
    populateYesNoSelection();
    populateDialog();

    // restore the changed flag to original value, as the modify listeners fire during dialog
    // population
    meta.setChanged(changed);

    // open dialog and enter event loop
    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) display.sleep();
    }

    // at this point the dialog has closed, so either ok() or cancel() have been executed
    // The "transformName" variable is inherited from BaseTransformDialog
    return transformName;
  }

  protected void changeCredentialChainSelection() {
    // Output-Info set in fields
    if (tAWSCredChain.getText().equalsIgnoreCase("Y")) {

      // Settings-Fields
      lblAWSKey.setEnabled(false);
      tAWSKey.setEnabled(false);
      lblAWSKeySecret.setEnabled(false);
      tAWSKeySecret.setEnabled(false);
      lblAWSRegion.setEnabled(false);
      tAWSRegion.setEnabled(false);

      // Output-Info set in Config
    } else {

      // Settings-Fields
      lblAWSKey.setEnabled(true);
      tAWSKey.setEnabled(true);
      lblAWSKeySecret.setEnabled(true);
      tAWSKeySecret.setEnabled(true);
      lblAWSRegion.setEnabled(true);
      tAWSRegion.setEnabled(true);
    }

    meta.setChanged();
  }

  private void populateYesNoSelection() {

    tAWSCredChain.removeAll();
    tAWSCredChain.add("Y");
    tAWSCredChain.add("N");
    tAWSCredChain.select(0);

    tMessageDelete.removeAll();
    tMessageDelete.add("Y");
    tMessageDelete.add("N");
    tMessageDelete.select(0);
  }

  /**
   * This method fills the CombarVar with all available AWS Regions
   *
   * @param ComboVar tAWSRegion2
   */
  private void populateAWSRegion(ComboVar tAWSRegion2) {

    tAWSRegion2.removeAll();

    try {

      List<Region> snsRegions = RegionUtils.getRegionsForService(AmazonSQS.ENDPOINT_PREFIX);

      for (Region region : snsRegions) {
        tAWSRegion2.add(region.getName());
      }

    } catch (AmazonClientException e) {
      logError(BaseMessages.getString(PKG, e.getMessage()));
    }
  }

  /**
   * This helper method puts the transform configuration stored in the meta object and puts it into
   * the dialog controls.
   */
  private void populateDialog() {
    wTransformName.selectAll();

    tAWSCredChain.setText(meta.getAwsCredChain());
    tAWSKey.setText(meta.getAwsKey());
    tAWSKeySecret.setText(meta.getAwsKeySecret());
    tAWSRegion.setText(meta.getAwsRegion());
    tSQSQueue.setText(meta.getSqsQueue());

    tMessageDelete.setText(meta.getTFldMessageDelete());
    tMaxMessages.setText(meta.getTFldMaxMessages());

    tMessageID.setText(meta.getTFldMessageID());
    tMessageBody.setText(meta.getTFldMessageBody());
    tReceiptHandle.setText(meta.getTFldReceiptHandle());
    tBodyMD5.setText(meta.getTFldBodyMD5());
    tSNSMessage.setText(meta.getTFldSNSMessage());
  }

  /** Called when the user cancels the dialog. */
  private void cancel() {
    // The "transformName" variable will be the return value for the open() method.
    // Setting to null to indicate that dialog was cancelled.
    transformName = null;
    // Restoring original "changed" flag on the met aobject
    meta.setChanged(changed);
    // close the SWT dialog window
    dispose();
  }

  /** Called when the user confirms the dialog */
  private void ok() {
    // The "transformName" variable will be the return value for the open() method.
    // Setting to transform name from the dialog control
    transformName = wTransformName.getText();

    // Setting the  settings to the meta object
    meta.setAwsCredChain(tAWSCredChain.getText());
    meta.setAwsKey(tAWSKey.getText());
    meta.setAwsKeySecret(tAWSKeySecret.getText());
    meta.setAwsRegion(tAWSRegion.getText());
    meta.setSqsQueue(tSQSQueue.getText());

    meta.setTFldMessageDelete(tMessageDelete.getText());
    meta.setTFldMaxMessages(tMaxMessages.getText());

    meta.setTFldMessageID(tMessageID.getText());
    meta.setTFldMessageBody(tMessageBody.getText());
    meta.setTFldReceiptHandle(tReceiptHandle.getText());
    meta.setTFldBodyMD5(tBodyMD5.getText());
    meta.setTFldSNSMessage(tSNSMessage.getText());

    // close the SWT dialog window
    dispose();
  }
}

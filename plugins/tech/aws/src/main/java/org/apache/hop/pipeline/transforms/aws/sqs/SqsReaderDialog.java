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
import java.util.Iterator;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
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

public class SqsReaderDialog extends BaseTransformDialog implements ITransformDialog {

  /**
   * The PKG member is used when looking up internationalized strings. The properties file with
   * localized keys is expected to reside in {the package of the class
   * specified}/messages/messages_{locale}.properties
   */
  private static Class<?> PKG = SqsReaderMeta.class; // for i18n purposes

  // this is the object the stores the transform's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed
  private SqsReaderMeta meta;

  // text field holding the name of the field to add to the row stream
  private CTabFolder tabFolder;
  private CTabItem tbtmSettings;
  private ScrolledComposite scrlSettingsComp;
  private Composite settingsComp;
  private Label lblAWSCredChain;
  private ComboVar tAWSCredChain;
  private Label lblAWSKey;
  private TextVar tAWSKey;
  private Label lblAWSKeySecret;
  private PasswordTextVar tAWSKeySecret;
  private Label lblAWSRegion;
  private ComboVar tAWSRegion;
  private CTabItem tbtmReaderOutput;
  private ScrolledComposite scrlreaderOutputComp;
  private Composite readerOutputComp;
  private Label lblMessageID;
  private TextVar tMessageID;
  private Label lblMessageBody;
  private TextVar tMessageBody;
  private Label lblReceiptHandle;
  private TextVar tReceiptHandle;
  private Label lblBodyMD5;
  private TextVar tBodyMD5;
  private Label lblMessageDelete;
  private ComboVar tMessageDelete;
  private Label lblSQSQueue;
  private TextVar tSQSQueue;
  private Label lblDevInfo;
  private Group grpOutputField;
  private Group grpOutputSettings;
  private Label lblMaxMessages;
  private TextVar tMaxMessages;
  private Label lblSNSMessage;
  private TextVar tSNSMessage;

  /**
   * The constructor should simply invoke super() and save the incoming meta object to a local
   * variable, so it can conveniently read and write settings from/to it.
   *
   * @param parent the SWT shell to open the dialog in
   * @param in the meta object holding the transform's settings
   * @param pipelineMeta pipeline description
   * @param sname the transform name
   */
  public SqsReaderDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    meta = (SqsReaderMeta) in;
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
    props.setLook(shell);
    setShellImage(shell, meta);

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseTransformDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod =
        new ModifyListener() {
          public void modifyText(ModifyEvent e) {
            meta.setChanged();
          }
        };

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
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
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
    tabFolder = new CTabFolder(shell, SWT.BORDER);
    FormData fd_tabFolder = new FormData();
    fd_tabFolder.right = new FormAttachment(100, 0);
    fd_tabFolder.top = new FormAttachment(wTransformName, margin);
    fd_tabFolder.left = new FormAttachment(0, 0);
    fd_tabFolder.bottom = new FormAttachment(100, -50);
    tabFolder.setLayoutData(fd_tabFolder);
    props.setLook(tabFolder);

    // ------------------------------------------------------- //
    // - TAB Settings START //
    // ------------------------------------------------------- //

    // Settings-TAB - ANFANG
    tbtmSettings = new CTabItem(tabFolder, SWT.NONE);
    tbtmSettings.setText(BaseMessages.getString(PKG, "SQSReaderTransform.Settings.Title"));

    scrlSettingsComp = new ScrolledComposite(tabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrlSettingsComp.setLayout(new FillLayout());
    props.setLook(scrlSettingsComp);

    settingsComp = new Composite(scrlSettingsComp, SWT.NONE);
    props.setLook(settingsComp);

    FormLayout settingsLayout = new FormLayout();
    settingsLayout.marginWidth = 3;
    settingsLayout.marginHeight = 3;
    settingsComp.setLayout(settingsLayout);

    // Use AWS Credentials Provider Chain
    // Credentials Chain
    lblAWSCredChain = new Label(settingsComp, SWT.RIGHT);
    props.setLook(lblAWSCredChain);
    FormData fd_lblAWSCredChain = new FormData();
    fd_lblAWSCredChain.left = new FormAttachment(0, 0);
    fd_lblAWSCredChain.top = new FormAttachment(0, margin);
    fd_lblAWSCredChain.right = new FormAttachment(middle, -margin);
    lblAWSCredChain.setLayoutData(fd_lblAWSCredChain);
    lblAWSCredChain.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSCredChain.Label"));

    tAWSCredChain = new ComboVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tAWSCredChain);
    FormData fd_tAWSCredChain = new FormData();
    fd_tAWSCredChain.top = new FormAttachment(0, margin);
    fd_tAWSCredChain.left = new FormAttachment(middle, 0);
    fd_tAWSCredChain.right = new FormAttachment(100, 0);
    tAWSCredChain.setLayoutData(fd_tAWSCredChain);
    tAWSCredChain.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSCredChain.Tooltip"));
    tAWSCredChain.addModifyListener(
        new ModifyListener() {

          @Override
          public void modifyText(ModifyEvent arg0) {
            changeCredentialChainSelection();
          }
        });

    // AWS Key
    lblAWSKey = new Label(settingsComp, SWT.RIGHT);
    props.setLook(lblAWSKey);
    FormData fd_lblAWSKey = new FormData();
    fd_lblAWSKey.left = new FormAttachment(0, 0);
    fd_lblAWSKey.top = new FormAttachment(tAWSCredChain, margin);
    fd_lblAWSKey.right = new FormAttachment(middle, -margin);
    lblAWSKey.setLayoutData(fd_lblAWSKey);
    lblAWSKey.setText(BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKey.Label"));

    tAWSKey = new TextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tAWSKey);
    FormData fd_tAWSKey = new FormData();
    fd_tAWSKey.top = new FormAttachment(tAWSCredChain, margin);
    fd_tAWSKey.left = new FormAttachment(middle, 0);
    fd_tAWSKey.right = new FormAttachment(100, 0);
    tAWSKey.setLayoutData(fd_tAWSKey);
    tAWSKey.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKey.Tooltip"));

    // AWS Key Secret
    lblAWSKeySecret = new Label(settingsComp, SWT.RIGHT);
    props.setLook(lblAWSKeySecret);
    FormData fd_lblAWSKeySecret = new FormData();
    fd_lblAWSKeySecret.left = new FormAttachment(0, 0);
    fd_lblAWSKeySecret.top = new FormAttachment(tAWSKey, margin);
    fd_lblAWSKeySecret.right = new FormAttachment(middle, -margin);
    lblAWSKeySecret.setLayoutData(fd_lblAWSKeySecret);
    lblAWSKeySecret.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKeySecret.Label"));

    tAWSKeySecret =
        new PasswordTextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tAWSKeySecret);
    FormData fd_tAWSKeySecret = new FormData();
    fd_tAWSKeySecret.top = new FormAttachment(tAWSKey, margin);
    fd_tAWSKeySecret.left = new FormAttachment(middle, 0);
    fd_tAWSKeySecret.right = new FormAttachment(100, 0);
    tAWSKeySecret.setLayoutData(fd_tAWSKeySecret);
    tAWSKeySecret.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSKeySecret.Tooltip"));

    // AWS Region
    lblAWSRegion = new Label(settingsComp, SWT.RIGHT);
    props.setLook(lblAWSRegion);
    FormData fd_lblAWSRegion = new FormData();
    fd_lblAWSRegion.left = new FormAttachment(0, 0);
    fd_lblAWSRegion.top = new FormAttachment(tAWSKeySecret, margin);
    fd_lblAWSRegion.right = new FormAttachment(middle, -margin);
    lblAWSRegion.setLayoutData(fd_lblAWSRegion);
    lblAWSRegion.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSRegion.Label"));

    tAWSRegion = new ComboVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tAWSRegion);
    FormData fd_tAWSRegion = new FormData();
    fd_tAWSRegion.top = new FormAttachment(tAWSKeySecret, margin);
    fd_tAWSRegion.left = new FormAttachment(middle, 0);
    fd_tAWSRegion.right = new FormAttachment(100, 0);
    tAWSRegion.setLayoutData(fd_tAWSRegion);
    tAWSRegion.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.Settings.AWSRegion.Tooltip"));
    populateAWSRegion(tAWSRegion);

    // SQS Queue
    lblSQSQueue = new Label(settingsComp, SWT.RIGHT);
    props.setLook(lblSQSQueue);
    FormData fd_lblSQSQueue = new FormData();
    fd_lblSQSQueue.left = new FormAttachment(0, 0);
    fd_lblSQSQueue.top = new FormAttachment(tAWSRegion, margin);
    fd_lblSQSQueue.right = new FormAttachment(middle, -margin);
    lblSQSQueue.setLayoutData(fd_lblSQSQueue);
    lblSQSQueue.setText(BaseMessages.getString(PKG, "SQSReaderTransform.Settings.SQSQueue.Label"));

    tSQSQueue = new TextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tSQSQueue);
    FormData fd_tSQSQueue = new FormData();
    fd_tSQSQueue.top = new FormAttachment(tAWSRegion, margin);
    fd_tSQSQueue.left = new FormAttachment(middle, 0);
    fd_tSQSQueue.right = new FormAttachment(100, 0);
    tSQSQueue.setLayoutData(fd_tSQSQueue);
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
    tbtmReaderOutput = new CTabItem(tabFolder, SWT.NONE);
    tbtmReaderOutput.setText(BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.Title"));

    scrlreaderOutputComp = new ScrolledComposite(tabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrlreaderOutputComp.setLayout(new FillLayout());
    props.setLook(scrlreaderOutputComp);

    readerOutputComp = new Composite(scrlreaderOutputComp, SWT.NONE);
    props.setLook(readerOutputComp);

    FormLayout ReaderOutputLayout = new FormLayout();
    ReaderOutputLayout.marginWidth = 3;
    ReaderOutputLayout.marginHeight = 3;
    readerOutputComp.setLayout(ReaderOutputLayout);

    // ------------------------------------------------------- //
    // --- GROUP Output Settings START //
    // ------------------------------------------------------- //

    grpOutputSettings = new Group(readerOutputComp, SWT.SHADOW_NONE);
    props.setLook(grpOutputSettings);
    grpOutputSettings.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.OutputSettings.GroupTitle"));
    FormData fd_grpOutputSettings = new FormData();
    fd_grpOutputSettings.top = new FormAttachment(0, margin);
    fd_grpOutputSettings.left = new FormAttachment(0, margin);
    fd_grpOutputSettings.right = new FormAttachment(100, -margin);
    fd_grpOutputSettings.bottom = new FormAttachment(40, -margin);
    grpOutputSettings.setLayoutData(fd_grpOutputSettings);

    FormLayout outputSettingsLayout = new FormLayout();
    outputSettingsLayout.marginWidth = 10;
    outputSettingsLayout.marginHeight = 10;
    grpOutputSettings.setLayout(outputSettingsLayout);

    // FELDER
    // Message Deletion
    lblMessageDelete = new Label(grpOutputSettings, SWT.RIGHT);
    props.setLook(lblMessageDelete);
    FormData fd_lblMessageDelete = new FormData();
    fd_lblMessageDelete.left = new FormAttachment(0, 0);
    fd_lblMessageDelete.top = new FormAttachment(0, margin);
    fd_lblMessageDelete.right = new FormAttachment(middle, -margin);
    lblMessageDelete.setLayoutData(fd_lblMessageDelete);
    lblMessageDelete.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageDelete.Label"));

    tMessageDelete = new ComboVar(variables, grpOutputSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tMessageDelete);
    FormData fd_tMessageDelete = new FormData();
    fd_tMessageDelete.top = new FormAttachment(0, margin);
    fd_tMessageDelete.left = new FormAttachment(middle, 0);
    fd_tMessageDelete.right = new FormAttachment(100, 0);
    tMessageDelete.setLayoutData(fd_tMessageDelete);
    tMessageDelete.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageDelete.Tooltip"));

    // Max Messages
    lblMaxMessages = new Label(grpOutputSettings, SWT.RIGHT);
    props.setLook(lblMaxMessages);
    FormData fd_lblMaxMessages = new FormData();
    fd_lblMaxMessages.left = new FormAttachment(0, 0);
    fd_lblMaxMessages.top = new FormAttachment(tMessageDelete, margin);
    fd_lblMaxMessages.right = new FormAttachment(middle, -margin);
    lblMaxMessages.setLayoutData(fd_lblMaxMessages);
    lblMaxMessages.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MaxMessages.Label"));

    tMaxMessages = new TextVar(variables, grpOutputSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tMaxMessages);
    FormData fd_tMaxMessages = new FormData();
    fd_tMaxMessages.top = new FormAttachment(tMessageDelete, margin);
    fd_tMaxMessages.left = new FormAttachment(middle, 0);
    fd_tMaxMessages.right = new FormAttachment(100, 0);
    tMaxMessages.setLayoutData(fd_tMaxMessages);
    tMaxMessages.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MaxMessages.Tooltip"));

    Control[] readerSettingsTabList = new Control[] {tMessageDelete, tMaxMessages};
    grpOutputSettings.setTabList(readerSettingsTabList);

    // ------------------------------------------------------- //
    // --- GROUP Output Fields START //
    // ------------------------------------------------------- //

    grpOutputField = new Group(readerOutputComp, SWT.SHADOW_NONE);
    props.setLook(grpOutputField);
    grpOutputField.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.OutputFields.GroupTitle"));
    FormData fd_grpOutputField = new FormData();
    fd_grpOutputField.top = new FormAttachment(40, margin);
    fd_grpOutputField.left = new FormAttachment(0, margin);
    fd_grpOutputField.right = new FormAttachment(100, -margin);
    fd_grpOutputField.bottom = new FormAttachment(100, -margin);
    grpOutputField.setLayoutData(fd_grpOutputField);

    FormLayout outputFieldsLayout = new FormLayout();
    outputFieldsLayout.marginWidth = 10;
    outputFieldsLayout.marginHeight = 10;
    grpOutputField.setLayout(outputFieldsLayout);

    // FELDER
    // MessageID
    lblMessageID = new Label(grpOutputField, SWT.RIGHT);
    props.setLook(lblMessageID);
    FormData fd_lblMessageID = new FormData();
    fd_lblMessageID.left = new FormAttachment(0, 0);
    fd_lblMessageID.top = new FormAttachment(0, margin);
    fd_lblMessageID.right = new FormAttachment(middle, -margin);
    lblMessageID.setLayoutData(fd_lblMessageID);
    lblMessageID.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageID.Label"));

    tMessageID = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tMessageID);
    FormData fd_tMessageID = new FormData();
    fd_tMessageID.top = new FormAttachment(0, margin);
    fd_tMessageID.left = new FormAttachment(middle, 0);
    fd_tMessageID.right = new FormAttachment(100, 0);
    tMessageID.setLayoutData(fd_tMessageID);
    tMessageID.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageID.Tooltip"));

    // MessageBody
    lblMessageBody = new Label(grpOutputField, SWT.RIGHT);
    props.setLook(lblMessageBody);
    FormData fd_lblMessageBody = new FormData();
    fd_lblMessageBody.left = new FormAttachment(0, 0);
    fd_lblMessageBody.top = new FormAttachment(tMessageID, margin);
    fd_lblMessageBody.right = new FormAttachment(middle, -margin);
    lblMessageBody.setLayoutData(fd_lblMessageBody);
    lblMessageBody.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageBody.Label"));

    tMessageBody = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tMessageBody);
    FormData fd_tMessageBody = new FormData();
    fd_tMessageBody.top = new FormAttachment(tMessageID, margin);
    fd_tMessageBody.left = new FormAttachment(middle, 0);
    fd_tMessageBody.right = new FormAttachment(100, 0);
    tMessageBody.setLayoutData(fd_tMessageBody);
    tMessageBody.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.MessageBody.Tooltip"));

    // ReceiptHandle
    lblReceiptHandle = new Label(grpOutputField, SWT.RIGHT);
    props.setLook(lblReceiptHandle);
    FormData fd_lblReceiptHandle = new FormData();
    fd_lblReceiptHandle.left = new FormAttachment(0, 0);
    fd_lblReceiptHandle.top = new FormAttachment(tMessageBody, margin);
    fd_lblReceiptHandle.right = new FormAttachment(middle, -margin);
    lblReceiptHandle.setLayoutData(fd_lblReceiptHandle);
    lblReceiptHandle.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.ReceiptHandle.Label"));

    tReceiptHandle = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tReceiptHandle);
    FormData fd_tReceiptHandle = new FormData();
    fd_tReceiptHandle.top = new FormAttachment(tMessageBody, margin);
    fd_tReceiptHandle.left = new FormAttachment(middle, 0);
    fd_tReceiptHandle.right = new FormAttachment(100, 0);
    tReceiptHandle.setLayoutData(fd_tReceiptHandle);
    tReceiptHandle.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.ReceiptHandle.Tooltip"));

    // BodyMD5
    lblBodyMD5 = new Label(grpOutputField, SWT.RIGHT);
    props.setLook(lblBodyMD5);
    FormData fd_lblBodyMD5 = new FormData();
    fd_lblBodyMD5.left = new FormAttachment(0, 0);
    fd_lblBodyMD5.top = new FormAttachment(tReceiptHandle, margin);
    fd_lblBodyMD5.right = new FormAttachment(middle, -margin);
    lblBodyMD5.setLayoutData(fd_lblBodyMD5);
    lblBodyMD5.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.BodyMD5.Label"));

    tBodyMD5 = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tBodyMD5);
    FormData fd_tBodyMD5 = new FormData();
    fd_tBodyMD5.top = new FormAttachment(tReceiptHandle, margin);
    fd_tBodyMD5.left = new FormAttachment(middle, 0);
    fd_tBodyMD5.right = new FormAttachment(100, 0);
    tBodyMD5.setLayoutData(fd_tBodyMD5);
    tBodyMD5.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.BodyMD5.Tooltip"));

    // SNSMessage
    lblSNSMessage = new Label(grpOutputField, SWT.RIGHT);
    props.setLook(lblSNSMessage);
    FormData fd_lblSNSMessage = new FormData();
    fd_lblSNSMessage.left = new FormAttachment(0, 0);
    fd_lblSNSMessage.top = new FormAttachment(tBodyMD5, margin);
    fd_lblSNSMessage.right = new FormAttachment(middle, -margin);
    lblSNSMessage.setLayoutData(fd_lblSNSMessage);
    lblSNSMessage.setText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.SNSMessage.Label"));

    tSNSMessage = new TextVar(variables, grpOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(tSNSMessage);
    FormData fd_tSNSMessage = new FormData();
    fd_tSNSMessage.top = new FormAttachment(tBodyMD5, margin);
    fd_tSNSMessage.left = new FormAttachment(middle, 0);
    fd_tSNSMessage.right = new FormAttachment(100, 0);
    tSNSMessage.setLayoutData(fd_tSNSMessage);
    tSNSMessage.setToolTipText(
        BaseMessages.getString(PKG, "SQSReaderTransform.ReaderOutput.SNSMessage.Tooltip"));

    Control[] readerOutputTabList =
        new Control[] {tMessageID, tMessageBody, tReceiptHandle, tBodyMD5, tSNSMessage};
    grpOutputField.setTabList(readerOutputTabList);

    readerOutputComp.pack();
    Rectangle ReaderOutputBounds = readerOutputComp.getBounds();

    scrlreaderOutputComp.setContent(readerOutputComp);
    scrlreaderOutputComp.setExpandHorizontal(true);
    scrlreaderOutputComp.setExpandVertical(true);
    scrlreaderOutputComp.setMinWidth(ReaderOutputBounds.width);
    scrlreaderOutputComp.setMinHeight(ReaderOutputBounds.height);
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

      for (Iterator<Region> i = snsRegions.iterator(); i.hasNext(); ) {
        Region region = i.next();
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

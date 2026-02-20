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

package org.apache.hop.pipeline.transforms.aws.sns;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.sns.AmazonSNS;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class SnsNotifyDialog extends BaseTransformDialog {

  /**
   * The PKG member is used when looking up internationalized strings. The properties file with
   * localized keys is expected to reside in {the package of the class
   * specified}/messages/messages_{locale}.properties
   */
  private static final Class<?> PKG = SnsNotifyMeta.class; // for i18n purposes

  // this is the object the stores the transform's settings
  // the dialog reads the settings from it when opening
  // the dialog writes the settings to it when confirmed
  private SnsNotifyMeta meta;

  // text field holding the name of the field to add to the row stream

  private Label lblAWSKey;
  private TextVar tAWSKey;
  private Label lblAWSKeySecret;
  private PasswordTextVar tAWSKeySecret;
  private Label lblAWSRegion;
  private ComboVar tAWSRegion;
  private Combo tnotifyPoint;
  private TextVar tMessageID;
  private ColumnInfo fieldColumn;
  private TableView tTableNotifyProps;

  private ComboVar tAWSCredChain;

  /**
   * The constructor should simply invoke super() and save the incoming meta object to a local
   * variable, so it can conveniently read and write settings from/to it.
   *
   * @param parent the SWT shell to open the dialog in
   * @param variables in the meta object holding the transform's settings
   * @param pipelineMeta pipeline description
   * @param transformMeta the transform name
   */
  public SnsNotifyDialog(
      Shell parent, IVariables variables, SnsNotifyMeta transformMeta, PipelineMeta pipelineMeta) {
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
  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SNSNotify.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    CTabFolder tabFolder;

    // store some convenient SWT variables
    Shell parent = getParent();
    Display display = parent.getDisplay();

    // Save the value of the changed flag on the meta object. If the user cancels
    // the dialog, it will be restored to this saved value.
    // The "changed" variable is inherited from BaseTransformDialog
    changed = meta.hasChanged();

    // The ModifyListener used on all controls. It will update the meta object to
    // indicate that changes are being made.
    ModifyListener lsMod = e -> meta.setChanged();

    wTransformName.addModifyListener(lsMod);

    // ------------------------------------------------------- //
    // TABULATOREN START //
    // ------------------------------------------------------- //

    // TABS - ANFANG
    tabFolder = new CTabFolder(shell, SWT.BORDER);
    FormData fdTabFolder = new FormData();
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.bottom = new FormAttachment(100, -50);
    tabFolder.setLayoutData(fdTabFolder);
    PropsUi.setLook(tabFolder);

    // ------------------------------------------------------- //
    // - TAB Settings START //
    // ------------------------------------------------------- //

    // Settings-TAB - ANFANG
    CTabItem tbtmSettings = new CTabItem(tabFolder, SWT.NONE);
    tbtmSettings.setText(BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.Title"));

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
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSCredChain.Label"));

    tAWSCredChain = new ComboVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSCredChain);
    FormData fdTAWSCredChain = new FormData();
    fdTAWSCredChain.top = new FormAttachment(0, margin);
    fdTAWSCredChain.left = new FormAttachment(middle, 0);
    fdTAWSCredChain.right = new FormAttachment(100, 0);
    tAWSCredChain.setLayoutData(fdTAWSCredChain);
    tAWSCredChain.setToolTipText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSCredChain.Tooltip"));
    tAWSCredChain.addModifyListener(arg0 -> changeCredentialChainSelection());

    // AWS Key
    lblAWSKey = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSKey);
    FormData fdLblAWSKey = new FormData();
    fdLblAWSKey.left = new FormAttachment(0, 0);
    fdLblAWSKey.top = new FormAttachment(tAWSCredChain, margin);
    fdLblAWSKey.right = new FormAttachment(middle, -margin);
    lblAWSKey.setLayoutData(fdLblAWSKey);
    lblAWSKey.setText(BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSKey.Label"));

    tAWSKey = new TextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSKey);
    FormData fdTAWSKey = new FormData();
    fdTAWSKey.top = new FormAttachment(tAWSCredChain, margin);
    fdTAWSKey.left = new FormAttachment(middle, 0);
    fdTAWSKey.right = new FormAttachment(100, 0);
    tAWSKey.setLayoutData(fdTAWSKey);
    tAWSKey.setToolTipText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSKey.Tooltip"));

    // AWS Key Secret
    lblAWSKeySecret = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSKeySecret);
    FormData fdLblAWSKeySecret = new FormData();
    fdLblAWSKeySecret.left = new FormAttachment(0, 0);
    fdLblAWSKeySecret.top = new FormAttachment(tAWSKey, margin);
    fdLblAWSKeySecret.right = new FormAttachment(middle, -margin);
    lblAWSKeySecret.setLayoutData(fdLblAWSKeySecret);
    lblAWSKeySecret.setText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSKeySecret.Label"));

    tAWSKeySecret =
        new PasswordTextVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSKeySecret);
    FormData fdTAWSKeySecret = new FormData();
    fdTAWSKeySecret.top = new FormAttachment(tAWSKey, margin);
    fdTAWSKeySecret.left = new FormAttachment(middle, 0);
    fdTAWSKeySecret.right = new FormAttachment(100, 0);
    tAWSKeySecret.setLayoutData(fdTAWSKeySecret);
    tAWSKeySecret.setToolTipText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSKeySecret.Tooltip"));

    // AWS Region
    lblAWSRegion = new Label(settingsComp, SWT.RIGHT);
    PropsUi.setLook(lblAWSRegion);
    FormData fdLblAWSRegion = new FormData();
    fdLblAWSRegion.left = new FormAttachment(0, 0);
    fdLblAWSRegion.top = new FormAttachment(tAWSKeySecret, margin);
    fdLblAWSRegion.right = new FormAttachment(middle, -margin);
    lblAWSRegion.setLayoutData(fdLblAWSRegion);
    lblAWSRegion.setText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSRegion.Label"));

    tAWSRegion = new ComboVar(variables, settingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tAWSRegion);
    FormData fdTAWSRegion = new FormData();
    fdTAWSRegion.top = new FormAttachment(tAWSKeySecret, margin);
    fdTAWSRegion.left = new FormAttachment(middle, 0);
    fdTAWSRegion.right = new FormAttachment(100, 0);
    tAWSRegion.setLayoutData(fdTAWSRegion);
    tAWSRegion.setToolTipText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Settings.AWSRegion.Tooltip"));
    populateAWSRegion(tAWSRegion);

    Control[] queueTabList = new Control[] {tAWSKey, tAWSKeySecret, tAWSRegion};
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
    // - TAB Notifications START //
    // ------------------------------------------------------- //

    // Notifications-TAB - ANFANG
    CTabItem tbtmNotifications = new CTabItem(tabFolder, SWT.NONE);
    tbtmNotifications.setText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.Title"));

    ScrolledComposite scrlNotificationsComp =
        new ScrolledComposite(tabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrlNotificationsComp.setLayout(new FillLayout());
    PropsUi.setLook(scrlNotificationsComp);

    Composite notificationsComp = new Composite(scrlNotificationsComp, SWT.NONE);
    PropsUi.setLook(notificationsComp);

    FormLayout notificationsLayout = new FormLayout();
    notificationsLayout.marginWidth = 3;
    notificationsLayout.marginHeight = 3;
    notificationsComp.setLayout(notificationsLayout);

    // FELDER
    // Notification Point
    Label lblnotifyPoint = new Label(notificationsComp, SWT.RIGHT);
    PropsUi.setLook(lblnotifyPoint);
    FormData fdLblnotifyPoint = new FormData();
    fdLblnotifyPoint.left = new FormAttachment(0, 0);
    fdLblnotifyPoint.top = new FormAttachment(0, margin);
    fdLblnotifyPoint.right = new FormAttachment(middle, -margin);
    lblnotifyPoint.setLayoutData(fdLblnotifyPoint);
    lblnotifyPoint.setText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.notifyPoint.Label"));

    tnotifyPoint = new Combo(notificationsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(tnotifyPoint);
    FormData fdTnotifyPoint = new FormData();
    fdTnotifyPoint.top = new FormAttachment(0, margin);
    fdTnotifyPoint.left = new FormAttachment(middle, 0);
    fdTnotifyPoint.right = new FormAttachment(100, 0);
    tnotifyPoint.setLayoutData(fdTnotifyPoint);
    tnotifyPoint.setToolTipText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.notifyPoint.Tooltip"));
    tnotifyPoint.setItems(meta.getNotifyPointValues());

    // MessageID
    Label lblMessageID = new Label(notificationsComp, SWT.RIGHT);
    PropsUi.setLook(lblMessageID);
    FormData fdLblMessageID = new FormData();
    fdLblMessageID.left = new FormAttachment(0, 0);
    fdLblMessageID.top = new FormAttachment(tnotifyPoint, margin);
    fdLblMessageID.right = new FormAttachment(middle, -margin);
    lblMessageID.setLayoutData(fdLblMessageID);
    lblMessageID.setText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.MessageID.Label"));

    tMessageID = new TextVar(variables, notificationsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(tMessageID);
    FormData fdTMessageID = new FormData();
    fdTMessageID.top = new FormAttachment(tnotifyPoint, margin);
    fdTMessageID.left = new FormAttachment(middle, 0);
    fdTMessageID.right = new FormAttachment(100, 0);
    tMessageID.setLayoutData(fdTMessageID);
    tMessageID.setToolTipText(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.MessageID.Tooltip"));

    // Notification-Value-Settings-Table
    // Properties Table
    int keyWidgetCols = 4;
    int keyWidgetRows = 3;

    // Create columns
    ColumnInfo[] ciNotifyProps = new ColumnInfo[keyWidgetCols];
    ciNotifyProps[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.Property.Label"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciNotifyProps[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.InField.Label"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {"Y", "N"},
            false);
    ciNotifyProps[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.Field.Label"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {},
            false);
    ciNotifyProps[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.Value.Label"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    ciNotifyProps[0].setToolTip(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.Property.Tooltip"));
    ciNotifyProps[0].setReadOnly(true);
    ciNotifyProps[1].setToolTip(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.InField.Tooltip"));
    ciNotifyProps[2].setToolTip(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.Field.Tooltip"));
    ciNotifyProps[3].setToolTip(
        BaseMessages.getString(PKG, "SNSNotifyTransform.Notifications.ValueDef.Value.Tooltip"));
    ciNotifyProps[3].setUsingVariables(true);

    fieldColumn = ciNotifyProps[2];

    // Create Table
    tTableNotifyProps =
        new TableView(
            variables,
            notificationsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciNotifyProps,
            keyWidgetRows,
            lsMod,
            props);

    FormData fdTableNotifyProps = new FormData();
    fdTableNotifyProps.left = new FormAttachment(0, 0);
    fdTableNotifyProps.top = new FormAttachment(tMessageID, margin);
    fdTableNotifyProps.right = new FormAttachment(100, 0);
    fdTableNotifyProps.bottom = new FormAttachment(100, -margin);
    tTableNotifyProps.setLayoutData(fdTableNotifyProps);

    Control[] notificationTabList = new Control[] {tnotifyPoint, tMessageID, tTableNotifyProps};
    notificationsComp.setTabList(notificationTabList);

    notificationsComp.pack();
    Rectangle notificationsBounds = notificationsComp.getBounds();

    scrlNotificationsComp.setContent(notificationsComp);
    scrlNotificationsComp.setExpandHorizontal(true);
    scrlNotificationsComp.setExpandVertical(true);
    scrlNotificationsComp.setMinWidth(notificationsBounds.width);
    scrlNotificationsComp.setMinHeight(notificationsBounds.height);
    // Notifications-TAB - Ende

    scrlSettingsComp.layout();
    tbtmSettings.setControl(scrlSettingsComp);

    scrlNotificationsComp.layout();
    tbtmNotifications.setControl(scrlNotificationsComp);

    tabFolder.setSelection(0);

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

    // Set/Restore the dialog size based on last position on screen
    // The setSize() method is inherited from BaseTransformDialog
    setSize();

    // populate the dialog with the values from the meta object
    populateYesNoSelection();
    populateDialog();

    // restore the changed flag to original value, as the modify listeners fire during dialog
    // population
    meta.setChanged(changed);

    focusTransformName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
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
  }

  /** This methods set the Input-Fields in Column Field for each table-row */
  private void setComboValues() {
    Runnable fieldLoader =
        () -> {
          IRowMeta prevFields;

          try {
            prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
          } catch (HopException e) {
            prevFields = new RowMeta();
            logError(BaseMessages.getString(PKG, "SNSNotifyTransform.ErrorText.NoPrevFields"));
          }
          String[] prevTransformFieldNames = prevFields.getFieldNames();
          Arrays.sort(prevTransformFieldNames);
          fieldColumn.setComboValues(prevTransformFieldNames);
        };
    new Thread(fieldLoader).start();
  }

  /** This method fills the CombarVar with all available AWS Regions */
  private void populateAWSRegion(ComboVar tAWSRegion2) {

    tAWSRegion2.removeAll();

    try {

      List<Region> snsRegions = RegionUtils.getRegionsForService(AmazonSNS.ENDPOINT_PREFIX);

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
    tAWSCredChain.setText(meta.getAwsCredChain());
    tAWSKey.setText(meta.getAwsKey());
    tAWSKeySecret.setText(meta.getAwsKeySecret());
    tAWSRegion.setText(meta.getAwsRegion());
    tnotifyPoint.setText(meta.getNotifyPoint());
    tMessageID.setText(meta.getTFldMessageID());

    // Populate NotifyProperties
    setComboValues();

    tTableNotifyProps.setText("topicARN", 1, 0);
    tTableNotifyProps.setText(meta.getCInputtopicArn(), 2, 0);
    tTableNotifyProps.setText(meta.getTFldtopicARN(), 3, 0);
    tTableNotifyProps.setText(meta.getTValuetopicARN(), 4, 0);

    tTableNotifyProps.setText("Subject", 1, 1);
    tTableNotifyProps.setText(meta.getCInputSubject(), 2, 1);
    tTableNotifyProps.setText(meta.getTFldSubject(), 3, 1);
    tTableNotifyProps.setText(meta.getTValueSubject(), 4, 1);

    tTableNotifyProps.setText("Message", 1, 2);
    tTableNotifyProps.setText(meta.getCInputMessage(), 2, 2);
    tTableNotifyProps.setText(meta.getTFldMessage(), 3, 2);
    tTableNotifyProps.setText(meta.getTValueMessage(), 4, 2);
  }

  /** Called when the user cancels the dialog. */
  private void cancel() {
    // The "transformName" variable will be the return value for the open() method.
    // Setting to null to indicate that dialog was cancelled.
    transformName = null;
    // Restoring original "changed" flag on the meta object
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
    meta.setNotifyPoint(tnotifyPoint.getText());
    meta.setTFldMessageID(tMessageID.getText());

    int nrKeys = tTableNotifyProps.nrNonEmpty();

    for (int i = 0; i < nrKeys; i++) {
      TableItem item = tTableNotifyProps.getNonEmpty(i);
      if (item.getText(1).equals("topicARN")) {
        meta.setCInputtopicArn(item.getText(2).isEmpty() ? "N" : item.getText(2));
        meta.setTFldtopicARN(item.getText(3));
        meta.setTValuetopicARN(item.getText(4));
      }
      if (item.getText(1).equals("Subject")) {
        meta.setCInputSubject(item.getText(2).isEmpty() ? "N" : item.getText(2));
        meta.setTFldSubject(item.getText(3));
        meta.setTValueSubject(item.getText(4));
      }
      if (item.getText(1).equals("Message")) {
        meta.setCInputMessage(item.getText(2).isEmpty() ? "N" : item.getText(2));
        meta.setTFldMessage(item.getText(3));
        meta.setTValueMessage(item.getText(4));
      }
    }

    // close the SWT dialog window
    dispose();
  }
}

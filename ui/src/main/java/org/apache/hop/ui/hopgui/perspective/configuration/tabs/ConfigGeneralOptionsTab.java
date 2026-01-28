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
 *
 */

package org.apache.hop.ui.hopgui.perspective.configuration.tabs;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.configuration.ConfigurationPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.ExpandBar;
import org.eclipse.swt.widgets.ExpandItem;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

@GuiPlugin
public class ConfigGeneralOptionsTab {
  private static final Class<?> PKG = BaseDialog.class;

  private Text wDefaultPreview;
  private Button wUseCache;
  private Button wOpenLast;
  private Button wReloadFileOnChange;
  private Button wAutoSave;
  private Button wAutoSplit;
  private Button wCopyDistribute;
  private Button wExitWarning;
  private Button wToolTip;
  private Button wResolveVarsInTips;
  private Button wbUseGlobalFileBookmarks;
  private Button wSortFieldByName;
  private Text wMaxExecutionLoggingTextSize;
  private Button wResetDialogPositions;

  private boolean isReloading = false; // Flag to prevent saving during reload

  public ConfigGeneralOptionsTab() {
    // This instance is created in the GuiPlugin system by calling this constructor, after which it
    // calls the addGeneralOptionsTab() method.
  }

  /**
   * Reload values from PropsUi into the widgets. This is useful when values are changed outside of
   * the options dialog (e.g., "Do not ask this again" checkboxes).
   */
  public void reloadValues() {
    if (wDefaultPreview == null || wDefaultPreview.isDisposed()) {
      return; // Tab not yet initialized or already disposed
    }

    // Set flag to prevent saveValues from being triggered during reload
    isReloading = true;

    try {
      PropsUi props = PropsUi.getInstance();

      // Reload all checkbox and text values from PropsUi
      wDefaultPreview.setText(Integer.toString(props.getDefaultPreviewSize()));
      wUseCache.setSelection(props.useDBCache());
      wOpenLast.setSelection(props.openLastFile());
      wReloadFileOnChange.setSelection(props.isReloadingFilesOnChange());
      wAutoSave.setSelection(!props.getAutoSave()); // Inverted logic
      wCopyDistribute.setSelection(props.showCopyOrDistributeWarning());
      wExitWarning.setSelection(props.showExitWarning());
      wAutoSplit.setSelection(!props.getAutoSplit()); // Inverted logic
      wToolTip.setSelection(props.showToolTips());
      wResolveVarsInTips.setSelection(props.resolveVariablesInToolTips());
      wSortFieldByName.setSelection(props.isSortFieldByName());
      wbUseGlobalFileBookmarks.setSelection(props.useGlobalFileBookmarks());
      wMaxExecutionLoggingTextSize.setText(
          Integer.toString(props.getMaxExecutionLoggingTextSize()));

      // Only reload if widget is initialized
      if (wResetDialogPositions != null && !wResetDialogPositions.isDisposed()) {
        wResetDialogPositions.setSelection(props.getResetDialogPositionsOnRestart());
      }
    } finally {
      // Always reset the flag
      isReloading = false;
    }
  }

  @GuiTab(
      id = "10000-config-perspective-general-options-tab",
      parentId = ConfigurationPerspective.CONFIG_PERSPECTIVE_TABS,
      description = "General options tab")
  public void addGeneralOptionsTab(CTabFolder wTabFolder) {
    Shell shell = wTabFolder.getShell();
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setImage(GuiResource.getInstance().getImageOptions());
    PropsUi.setLook(wGeneralTab);
    wGeneralTab.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.General.Label"));

    ScrolledComposite sGeneralComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    sGeneralComp.setLayout(new FillLayout());

    Composite wGeneralComp = new Composite(sGeneralComp, SWT.NONE);
    PropsUi.setLook(wGeneralComp);
    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = PropsUi.getFormMargin();
    generalLayout.marginHeight = PropsUi.getFormMargin();
    wGeneralComp.setLayout(generalLayout);

    // The name of the Hop configuration filename
    Control[] filenameControls =
        createTextField(
            wGeneralComp,
            "EnterOptionsDialog.ConfigFilename.Label",
            null,
            Const.NVL(HopConfig.getInstance().getConfigFilename(), ""),
            null,
            margin);
    Text wFilename = (Text) filenameControls[1];
    // Disable the field if config folder is set via HOP_CONFIG_FOLDER environment variable
    String configFolder = System.getProperty("HOP_CONFIG_FOLDER");
    boolean isConfigFromEnv = configFolder != null && !configFolder.trim().isEmpty();
    wFilename.setEnabled(!isConfigFromEnv);
    Control lastControl = wFilename;

    // Explain HOP_CONFIG
    Label wlWhatIsHopConfig = new Label(wGeneralComp, SWT.LEFT);
    wlWhatIsHopConfig.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.WhatIsHopConfigSize.Label"));
    PropsUi.setLook(wlWhatIsHopConfig);
    FormData fdlWhatIsHopConfig = new FormData();
    fdlWhatIsHopConfig.left = new FormAttachment(0, 0);
    fdlWhatIsHopConfig.right = new FormAttachment(100, 0);
    fdlWhatIsHopConfig.top = new FormAttachment(lastControl, margin);
    wlWhatIsHopConfig.setLayoutData(fdlWhatIsHopConfig);
    lastControl = wlWhatIsHopConfig;

    // The default preview size
    Control[] previewControls =
        createTextField(
            wGeneralComp,
            "EnterOptionsDialog.DefaultPreviewSize.Label",
            null,
            Integer.toString(props.getDefaultPreviewSize()),
            lastControl,
            margin);
    wDefaultPreview = (Text) previewControls[1];
    // Add hint text for empty field
    wDefaultPreview.setMessage(BaseMessages.getString(PKG, "EnterOptionsDialog.EnterNumber.Hint"));
    // Add validation to only allow integer values
    wDefaultPreview.addListener(
        SWT.Verify,
        e -> {
          String currentText = ((Text) e.widget).getText();
          String newText =
              currentText.substring(0, e.start) + e.text + currentText.substring(e.end);
          if (!newText.isEmpty() && !newText.matches("\\d+")) {
            e.doit = false; // Reject the input if it's not a valid integer
          }
        });
    lastControl = wDefaultPreview;

    // Use DB Cache?
    wUseCache =
        createCheckbox(
            wGeneralComp,
            "EnterOptionsDialog.UseDatabaseCache.Label",
            null,
            props.useDBCache(),
            lastControl,
            margin);
    lastControl = wUseCache;

    // Auto load last file at startup?
    wOpenLast =
        createCheckbox(
            wGeneralComp,
            "EnterOptionsDialog.OpenLastFileStartup.Label",
            null,
            props.openLastFile(),
            lastControl,
            margin);
    lastControl = wOpenLast;

    // Reload file if changed on filesystem?
    wReloadFileOnChange =
        createCheckbox(
            wGeneralComp,
            "EnterOptionsDialog.ReloadFileOnChange.Label",
            "EnterOptionsDialog.ReloadFileOnChange.ToolTip",
            props.isReloadingFilesOnChange(),
            lastControl,
            margin);
    lastControl = wReloadFileOnChange;

    // Sort field by name
    wSortFieldByName =
        createCheckbox(
            wGeneralComp,
            "EnterOptionsDialog.SortFieldByName.Label",
            "EnterOptionsDialog.SortFieldByName.ToolTip",
            props.isSortFieldByName(),
            lastControl,
            margin);
    lastControl = wSortFieldByName;

    // Use global file bookmarks?
    wbUseGlobalFileBookmarks =
        createCheckbox(
            wGeneralComp,
            "EnterOptionsDialog.UseGlobalFileBookmarks.Label",
            null,
            props.useGlobalFileBookmarks(),
            lastControl,
            margin);
    lastControl = wbUseGlobalFileBookmarks;

    // Maximum execution logging text size
    Control[] loggingControls =
        createTextField(
            wGeneralComp,
            "EnterOptionsDialog.MaxExecutionLoggingTextSizeSize.Label",
            "EnterOptionsDialog.MaxExecutionLoggingTextSizeSize.ToolTip",
            Integer.toString(props.getMaxExecutionLoggingTextSize()),
            lastControl,
            margin);
    wMaxExecutionLoggingTextSize = (Text) loggingControls[1];
    // Add hint text for empty field
    wMaxExecutionLoggingTextSize.setMessage(
        BaseMessages.getString(PKG, "EnterOptionsDialog.EnterNumber.Hint"));
    // Add validation to only allow integer values
    wMaxExecutionLoggingTextSize.addListener(
        SWT.Verify,
        e -> {
          String currentText = ((Text) e.widget).getText();
          String newText =
              currentText.substring(0, e.start) + e.text + currentText.substring(e.end);
          if (!newText.isEmpty() && !newText.matches("\\d+")) {
            e.doit = false; // Reject the input if it's not a valid integer
          }
        });
    lastControl = wMaxExecutionLoggingTextSize;

    // Confirmation dialogs section (at the bottom) - using ExpandBar
    ExpandBar expandBar = new ExpandBar(wGeneralComp, SWT.V_SCROLL);
    PropsUi.setLook(expandBar);

    FormData fdExpandBar = new FormData();
    fdExpandBar.left = new FormAttachment(0, 0);
    fdExpandBar.right = new FormAttachment(100, 0);
    fdExpandBar.top = new FormAttachment(lastControl, margin);
    expandBar.setLayoutData(fdExpandBar);

    // Create expandable item for confirmation dialogs
    Composite confirmationContent = new Composite(expandBar, SWT.NONE);
    PropsUi.setLook(confirmationContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    confirmationContent.setLayout(contentLayout);

    // Exit warning checkbox inside the expandable content
    Control lastConfirmControl = null;
    wExitWarning =
        createCheckbox(
            confirmationContent,
            "EnterOptionsDialog.AskOnExit.Label",
            null,
            props.showExitWarning(),
            lastConfirmControl,
            margin);
    lastConfirmControl = wExitWarning;

    // Show warning for copy / distribute...
    wCopyDistribute =
        createCheckbox(
            confirmationContent,
            "EnterOptionsDialog.CopyOrDistributeDialog.Label",
            "EnterOptionsDialog.CopyOrDistributeDialog.Tooltip",
            props.showCopyOrDistributeWarning(),
            lastConfirmControl,
            margin);
    lastConfirmControl = wCopyDistribute;

    // Show confirmation when splitting hops (inverted logic from autoSplit)
    wAutoSplit =
        createCheckbox(
            confirmationContent,
            "EnterOptionsDialog.SplitHopsConfirm.Label",
            "EnterOptionsDialog.SplitHopsConfirm.Tooltip",
            !props.getAutoSplit(),
            lastConfirmControl,
            margin);
    lastConfirmControl = wAutoSplit;

    // Show confirmation to save file when starting pipeline or workflow (inverted logic from
    // autoSave)
    wAutoSave =
        createCheckbox(
            confirmationContent,
            "EnterOptionsDialog.SaveConfirm.Label",
            "EnterOptionsDialog.SaveConfirm.Tooltip",
            !props.getAutoSave(),
            lastConfirmControl,
            margin);
    lastConfirmControl = wAutoSave;

    // Reset button - enables all confirmation dialogs
    Control[] resetButtonControls =
        createButton(
            confirmationContent,
            "EnterOptionsDialog.ResetConfirmations.Label",
            "EnterOptionsDialog.ResetConfirmations.Tooltip",
            lastConfirmControl,
            margin);
    Button wResetConfirmations = (Button) resetButtonControls[0];
    wResetConfirmations.addListener(
        SWT.Selection,
        e -> {
          // Enable all confirmation checkboxes
          wExitWarning.setSelection(true);
          wCopyDistribute.setSelection(true);
          wAutoSplit.setSelection(true);
          wAutoSave.setSelection(true);

          // Clear all custom parameters to re-enable transform warnings
          try {
            props.clearCustomParameters();
          } catch (Exception ex) {
            new ErrorDialog(shell, "Error", "Error clearing transform warning preferences", ex);
          }

          // Trigger save to persist the changes
          saveValues(null);
        });

    // Create the expand item
    ExpandItem confirmationItem = new ExpandItem(expandBar, SWT.NONE);
    confirmationItem.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.Section.ConfirmationDialogs"));
    confirmationItem.setControl(confirmationContent);
    confirmationItem.setHeight(confirmationContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    confirmationItem.setExpanded(true); // Start expanded

    // Tooltips section - using ExpandBar
    Composite tooltipsContent = new Composite(expandBar, SWT.NONE);
    PropsUi.setLook(tooltipsContent);
    FormLayout tooltipsLayout = new FormLayout();
    tooltipsLayout.marginWidth = PropsUi.getFormMargin();
    tooltipsLayout.marginHeight = PropsUi.getFormMargin();
    tooltipsContent.setLayout(tooltipsLayout);

    // Display tooltips checkbox
    Control lastTooltipControl = null;
    wToolTip =
        createCheckbox(
            tooltipsContent,
            "EnterOptionsDialog.ToolTipsEnabled.Label",
            null,
            props.showToolTips(),
            lastTooltipControl,
            margin);
    lastTooltipControl = wToolTip;

    // Resolve variables in tooltips
    wResolveVarsInTips =
        createCheckbox(
            tooltipsContent,
            "EnterOptionsDialog.ResolveVarsInTips.Label",
            null,
            props.resolveVariablesInToolTips(),
            lastTooltipControl,
            margin);
    lastTooltipControl = wResolveVarsInTips;

    // Reset button - enables all tooltip options
    Control[] resetTooltipControls =
        createButton(
            tooltipsContent,
            "EnterOptionsDialog.ResetTooltips.Label",
            "EnterOptionsDialog.ResetTooltips.Tooltip",
            lastTooltipControl,
            margin);
    Button wResetTooltips = (Button) resetTooltipControls[0];
    wResetTooltips.addListener(
        SWT.Selection,
        e -> {
          // Enable all tooltip checkboxes
          wToolTip.setSelection(true);
          wResolveVarsInTips.setSelection(true);

          // Trigger save to persist the changes
          saveValues(null);
        });

    // Create the tooltips expand item
    ExpandItem tooltipsItem = new ExpandItem(expandBar, SWT.NONE);
    tooltipsItem.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Section.Tooltips"));
    tooltipsItem.setControl(tooltipsContent);
    tooltipsItem.setHeight(tooltipsContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    tooltipsItem.setExpanded(true); // Start expanded

    // Dialog Positioning section - using ExpandBar
    Composite dialogPosContent = new Composite(expandBar, SWT.NONE);
    PropsUi.setLook(dialogPosContent);
    FormLayout dialogPosLayout = new FormLayout();
    dialogPosLayout.marginWidth = PropsUi.getFormMargin();
    dialogPosLayout.marginHeight = PropsUi.getFormMargin();
    dialogPosContent.setLayout(dialogPosLayout);

    // Reset dialog positions checkbox
    Control lastDialogPosControl = null;
    wResetDialogPositions =
        createCheckbox(
            dialogPosContent,
            "EnterOptionsDialog.ResetDialogPositions.Label",
            "EnterOptionsDialog.ResetDialogPositions.Tooltip",
            props.getResetDialogPositionsOnRestart(),
            lastDialogPosControl,
            margin);
    lastDialogPosControl = wResetDialogPositions;

    // Clear dialog positions button
    Control[] clearDialogPosControls =
        createButton(
            dialogPosContent,
            "EnterOptionsDialog.ClearDialogPositions.Label",
            "EnterOptionsDialog.ClearDialogPositions.Tooltip",
            lastDialogPosControl,
            margin);
    Button wClearDialogPositions = (Button) clearDialogPosControls[0];
    wClearDialogPositions.addListener(
        SWT.Selection,
        e -> {
          // Clear both session and persistent dialog positions
          props.clearSessionScreens();
          props.clearPersistedDialogScreens();

          // Show confirmation message
          org.apache.hop.ui.core.dialog.MessageBox mb =
              new org.apache.hop.ui.core.dialog.MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage(
              BaseMessages.getString(PKG, "EnterOptionsDialog.ClearDialogPositions.Confirmation"));
          mb.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.ClearDialogPositions.Title"));
          mb.open();
        });

    // Create the dialog positioning expand item
    ExpandItem dialogPosItem = new ExpandItem(expandBar, SWT.NONE);
    dialogPosItem.setText(
        BaseMessages.getString(PKG, "EnterOptionsDialog.Section.DialogPositioning"));
    dialogPosItem.setControl(dialogPosContent);
    dialogPosItem.setHeight(dialogPosContent.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
    dialogPosItem.setExpanded(true); // Start expanded

    // Add expand/collapse listeners for space reclamation
    expandBar.addListener(
        SWT.Expand,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wGeneralComp.isDisposed() && !sGeneralComp.isDisposed()) {
                        wGeneralComp.layout();
                        sGeneralComp.setMinHeight(
                            wGeneralComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));
    expandBar.addListener(
        SWT.Collapse,
        e ->
            Display.getDefault()
                .asyncExec(
                    () -> {
                      if (!wGeneralComp.isDisposed() && !sGeneralComp.isDisposed()) {
                        wGeneralComp.layout();
                        sGeneralComp.setMinHeight(
                            wGeneralComp.computeSize(SWT.DEFAULT, SWT.DEFAULT).y);
                      }
                    }));

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 100);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.pack();

    Rectangle bounds = wGeneralComp.getBounds();

    sGeneralComp.setContent(wGeneralComp);
    sGeneralComp.setExpandHorizontal(true);
    sGeneralComp.setExpandVertical(true);
    sGeneralComp.setMinWidth(bounds.width);
    sGeneralComp.setMinHeight(bounds.height);

    wGeneralTab.setControl(sGeneralComp);
  }

  /**
   * Creates a text field with label above it.
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param tooltipKey Optional tooltip message key (can be null)
   * @param initialValue The initial text value
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return An array containing [Label, Text] controls
   */
  private Control[] createTextField(
      Composite parent,
      String labelKey,
      String tooltipKey,
      String initialValue,
      Control lastControl,
      int margin) {
    // Label above
    Label label = new Label(parent, SWT.LEFT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdLabel.top = new FormAttachment(lastControl, margin);
    } else {
      fdLabel.top = new FormAttachment(0, margin);
    }
    label.setLayoutData(fdLabel);

    // Text field below label
    Text text = new Text(parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.setText(initialValue);
    if (tooltipKey != null) {
      text.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }
    text.addListener(SWT.Modify, this::saveValues);

    FormData fdText = new FormData();
    fdText.left = new FormAttachment(0, 0);
    fdText.right = new FormAttachment(100, 0);
    fdText.top = new FormAttachment(label, margin / 2);
    text.setLayoutData(fdText);

    return new Control[] {label, text};
  }

  /**
   * Creates a button with image in front and text label behind it (like checkboxes).
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param tooltipKey Optional tooltip message key (can be null)
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return An array containing [Button, Label] controls
   */
  private Control[] createButton(
      Composite parent, String labelKey, String tooltipKey, Control lastControl, int margin) {
    // Button with image
    Button button = new Button(parent, SWT.PUSH);
    PropsUi.setLook(button);

    // Try to set image, otherwise use text
    Image buttonImage = GuiResource.getInstance().getImageResetOption();
    if (buttonImage != null) {
      button.setImage(buttonImage);
      button.setBackground(GuiResource.getInstance().getColorWhite());
    } else {
      button.setText(BaseMessages.getString(PKG, "EnterOptionsDialog.Button.Reset"));
    }

    if (tooltipKey != null) {
      button.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }

    // Calculate proper button height based on image and zoom factor
    int buttonHeight = (int) (32 * PropsUi.getInstance().getZoomFactor());
    if (buttonImage != null) {
      // Ensure button is at least as tall as the image with some padding
      buttonHeight = Math.max(buttonHeight, buttonImage.getBounds().height + 8);
    }

    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment(0, 0);
    fdButton.height = buttonHeight;
    if (lastControl != null) {
      fdButton.top = new FormAttachment(lastControl, margin);
    } else {
      fdButton.top = new FormAttachment(0, margin);
    }
    button.setLayoutData(fdButton);

    // Label with text behind the button
    Label label = new Label(parent, SWT.LEFT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));

    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(button, margin);
    fdLabel.top = new FormAttachment(button, 0, SWT.CENTER);
    fdLabel.right = new FormAttachment(100, 0);
    label.setLayoutData(fdLabel);

    return new Control[] {button, label};
  }

  /**
   * Creates a checkbox with the checkbox in front of the label text (not centered).
   *
   * @param parent The parent composite
   * @param labelKey The message key for the label text
   * @param tooltipKey Optional tooltip message key (can be null)
   * @param selected Whether the checkbox is initially selected
   * @param lastControl The last control to attach to
   * @param margin The margin to use
   * @return The created Button (checkbox)
   */
  private Button createCheckbox(
      Composite parent,
      String labelKey,
      String tooltipKey,
      boolean selected,
      Control lastControl,
      int margin) {
    Button checkbox = new Button(parent, SWT.CHECK);
    PropsUi.setLook(checkbox);
    checkbox.setText(BaseMessages.getString(PKG, labelKey));
    if (tooltipKey != null) {
      checkbox.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    }
    checkbox.setSelection(selected);
    checkbox.addListener(SWT.Selection, this::saveValues);

    FormData fdCheckbox = new FormData();
    fdCheckbox.left = new FormAttachment(0, 0);
    fdCheckbox.right = new FormAttachment(100, 0);
    if (lastControl != null) {
      fdCheckbox.top = new FormAttachment(lastControl, margin);
    } else {
      fdCheckbox.top = new FormAttachment(0, margin);
    }
    checkbox.setLayoutData(fdCheckbox);

    return checkbox;
  }

  private void saveValues(Event event) {
    // Don't save if we're currently reloading values
    if (isReloading) {
      return;
    }

    PropsUi props = PropsUi.getInstance();

    props.setDefaultPreviewSize(
        Const.toInt(wDefaultPreview.getText(), props.getDefaultPreviewSize()));
    props.setUseDBCache(wUseCache.getSelection());
    props.setOpenLastFile(wOpenLast.getSelection());
    props.setReloadingFilesOnChange(wReloadFileOnChange.getSelection());
    props.setAutoSave(
        !wAutoSave
            .getSelection()); // Inverted: checkbox is "show confirmation", property is "auto save"
    props.setAutoSplit(
        !wAutoSplit
            .getSelection()); // Inverted: checkbox is "show confirmation", property is "auto split"
    props.setShowCopyOrDistributeWarning(wCopyDistribute.getSelection());
    props.setExitWarningShown(wExitWarning.getSelection());
    props.setShowToolTips(wToolTip.getSelection());
    props.setResolveVariablesInToolTips(wResolveVarsInTips.getSelection());
    props.setSortFieldByName(wSortFieldByName.getSelection());
    props.setUseGlobalFileBookmarks(wbUseGlobalFileBookmarks.getSelection());
    props.setMaxExecutionLoggingTextSize(
        Const.toInt(
            wMaxExecutionLoggingTextSize.getText(),
            PropsUi.DEFAULT_MAX_EXECUTION_LOGGING_TEXT_SIZE));

    // Only save if widget is initialized (it's created after other widgets)
    if (wResetDialogPositions != null && !wResetDialogPositions.isDisposed()) {
      props.setResetDialogPositionsOnRestart(wResetDialogPositions.getSelection());
    }

    try {
      HopConfig.getInstance().saveToFile();
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Error saving configuration to file", e);
    }
  }
}

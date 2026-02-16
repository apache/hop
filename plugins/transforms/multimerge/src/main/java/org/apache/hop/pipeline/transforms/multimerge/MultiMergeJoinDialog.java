/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.multimerge;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class MultiMergeJoinDialog extends BaseTransformDialog {
  private static final Class<?> PKG = MultiMergeJoinMeta.class;

  public static final String STRING_SORT_WARNING_PARAMETER = "MultiMergeJoinSortWarning";

  private final CCombo[] wInputTransformArray;
  private CCombo joinTypeCombo;
  private final Text[] keyValTextBox;

  private final List<String> inputFields = new ArrayList<>();
  private IRowMeta prev;
  private ColumnInfo[] ciKeys;

  private final int margin = PropsUi.getMargin();
  private final int middle = props.getMiddlePct();

  private final MultiMergeJoinMeta joinMeta;
  private String[] allInputTransforms;

  public MultiMergeJoinDialog(
      Shell parent,
      IVariables variables,
      MultiMergeJoinMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    joinMeta = transformMeta;

    allInputTransforms = getInputTransformNames();

    int availableTransforms = allInputTransforms.length;
    int configuredTransforms =
        (joinMeta.getInputTransforms() != null) ? joinMeta.getInputTransforms().size() : 0;
    int numInputs = Math.max(2, Math.max(availableTransforms, configuredTransforms));

    wInputTransformArray = new CCombo[numInputs];
    keyValTextBox = new Text[numInputs];
  }

  private String[] getInputTransformNames() {
    ArrayList<String> nameList = new ArrayList<>();
    String[] prevTransformNames = pipelineMeta.getPrevTransformNames(transformName);
    if (prevTransformNames != null) {
      String prevTransformName;
      for (String name : prevTransformNames) {
        prevTransformName = name;
        if (nameList.contains(prevTransformName)) {
          continue;
        }
        nameList.add(prevTransformName);
      }
    }
    return nameList.toArray(new String[0]);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.transform.ITransformDialog#open()
   */
  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, joinMeta);

    final ModifyListener lsMod = e -> joinMeta.setChanged();
    backupChanged = joinMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MultiMergeJoinDialog.Shell.Label"));

    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "MultiMergeJoinDialog.TransformName.Label"));
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
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // create widgets for input stream and join key selections
    createInputStreamWidgets(lsMod);

    // create widgets for Join type
    createJoinTypeWidget(lsMod);

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // Add listeners
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk.addListener(SWT.Selection, e -> ok());

    // get the data
    getData();
    joinMeta.setChanged(backupChanged);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /**
   * Create widgets for join type selection
   *
   * @param lsMod the modify listener
   */
  private void createJoinTypeWidget(final ModifyListener lsMod) {
    Label joinTypeLabel = new Label(shell, SWT.RIGHT);
    joinTypeLabel.setText(BaseMessages.getString(PKG, "MultiMergeJoinDialog.Type.Label"));
    PropsUi.setLook(joinTypeLabel);
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    if (wInputTransformArray.length > 0) {
      fdlType.top =
          new FormAttachment(wInputTransformArray[wInputTransformArray.length - 1], margin * 3);
    } else {
      fdlType.top = new FormAttachment(wTransformName, margin * 3);
    }
    joinTypeLabel.setLayoutData(fdlType);
    joinTypeCombo = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(joinTypeCombo);

    joinTypeCombo.setItems(MultiMergeJoinMeta.joinTypes);

    joinTypeCombo.addModifyListener(lsMod);
    FormData fdType = new FormData();
    fdType.top = new FormAttachment(joinTypeLabel, 0, SWT.CENTER);
    fdType.left = new FormAttachment(joinTypeLabel, margin);
    fdType.right = new FormAttachment(60, 0);
    joinTypeCombo.setLayoutData(fdType);
  }

  /**
   * create widgets for input stream and join keys
   *
   * @param lsMod the modify listener
   */
  private void createInputStreamWidgets(final ModifyListener lsMod) {
    String[] inputTransforms = allInputTransforms;
    for (int index = 0; index < wInputTransformArray.length; index++) {
      Label wlTransform;
      FormData fdlTransform;
      FormData fdTransform1;

      wlTransform = new Label(shell, SWT.RIGHT);
      wlTransform.setText(
          BaseMessages.getString(PKG, "MultiMergeJoinMeta.InputTransform") + (index + 1));
      PropsUi.setLook(wlTransform);
      fdlTransform = new FormData();
      fdlTransform.left = new FormAttachment(0, 0);
      fdlTransform.right = new FormAttachment(middle, -margin);
      if (index == 0) {
        fdlTransform.top = new FormAttachment(wTransformName, margin * 3);
      } else {
        fdlTransform.top = new FormAttachment(wInputTransformArray[index - 1], margin * 3);
      }

      wlTransform.setLayoutData(fdlTransform);
      wInputTransformArray[index] = new CCombo(shell, SWT.BORDER);
      PropsUi.setLook(wInputTransformArray[index]);

      wInputTransformArray[index].setItems(inputTransforms);

      wInputTransformArray[index].addModifyListener(lsMod);

      fdTransform1 = new FormData();
      fdTransform1.left = new FormAttachment(wlTransform, margin);
      fdTransform1.top = new FormAttachment(wlTransform, 0, SWT.CENTER);
      fdTransform1.right = new FormAttachment(60);
      wInputTransformArray[index].setLayoutData(fdTransform1);

      Label keyLabel = new Label(shell, SWT.LEFT);
      keyLabel.setText(BaseMessages.getString(PKG, "MultiMergeJoinMeta.JoinKeys"));
      PropsUi.setLook(keyLabel);
      FormData keyTransform = new FormData();
      keyTransform.left = new FormAttachment(wInputTransformArray[index], margin * 2);
      keyTransform.top = new FormAttachment(wlTransform, 0, SWT.CENTER);
      keyLabel.setLayoutData(keyTransform);

      keyValTextBox[index] = new Text(shell, SWT.READ_ONLY | SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      PropsUi.setLook(keyValTextBox[index]);
      keyValTextBox[index].setText("");
      keyValTextBox[index].addModifyListener(lsMod);
      FormData keyData = new FormData();
      keyData.left = new FormAttachment(keyLabel, margin);
      keyData.top = new FormAttachment(wlTransform, 0, SWT.CENTER);
      keyValTextBox[index].setLayoutData(keyData);

      Button button = new Button(shell, SWT.PUSH);
      button.setText(BaseMessages.getString(PKG, "MultiMergeJoinMeta.SelectKeys"));
      // add listener
      button.addListener(
          SWT.Selection, new ConfigureKeyButtonListener(this, keyValTextBox[index], index, lsMod));
      FormData buttonData = new FormData();
      buttonData.right = new FormAttachment(100, -margin);
      buttonData.top = new FormAttachment(wlTransform, 0, SWT.CENTER);
      button.setLayoutData(buttonData);
      keyData.right = new FormAttachment(button, -margin);
    }
  }

  /**
   * "Configure join key" shell
   *
   * @param keyValTextBox the text widget that needs to be added
   * @param lsMod modify listener
   */
  private void configureKeys(
      final Text keyValTextBox, final int inputStreamIndex, ModifyListener lsMod) {

    final Shell subShell = new Shell(shell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    final FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 5;
    formLayout.marginHeight = 5;
    subShell.setLayout(formLayout);
    subShell.setMinimumSize(300, 300);
    subShell.setSize(400, 300);
    subShell.setText(BaseMessages.getString(PKG, "MultiMergeJoinMeta.JoinKeys"));
    subShell.setImage(GuiResource.getInstance().getImageHop());
    Label wlKeys = new Label(subShell, SWT.NONE);
    wlKeys.setText(BaseMessages.getString(PKG, "MultiMergeJoinDialog.Keys"));
    FormData fdlKeys = new FormData();
    fdlKeys.left = new FormAttachment(0, 0);
    fdlKeys.right = new FormAttachment(50, -margin);
    fdlKeys.top = new FormAttachment(0, margin);
    wlKeys.setLayoutData(fdlKeys);

    String[] keys = keyValTextBox.getText().split(",");
    int nrKeyRows = (keys != null ? keys.length : 1);

    ciKeys =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "MultiMergeJoinDialog.ColumnInfo.KeyField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
        };

    final TableView wKeys =
        new TableView(
            variables,
            subShell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKeys,
            nrKeyRows,
            lsMod,
            props);

    FormData fdKeys = new FormData();
    fdKeys.top = new FormAttachment(wlKeys, margin);
    fdKeys.left = new FormAttachment(0, 0);
    fdKeys.bottom = new FormAttachment(100, -70);
    fdKeys.right = new FormAttachment(100, -margin);
    wKeys.setLayoutData(fdKeys);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          try {
            CCombo wInputTransform = wInputTransformArray[inputStreamIndex];
            String transformName = wInputTransform.getText();
            TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
            if (transformMeta != null) {
              prev = pipelineMeta.getTransformFields(variables, transformMeta);
              if (prev != null) {
                // Remember these fields...
                for (int i = 0; i < prev.size(); i++) {
                  inputFields.add(prev.getValueMeta(i).getName());
                }
                setComboBoxes();
                inputFields.clear();
              }
            }
          } catch (HopException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
          }
        };
    HopGui.getInstance().getDisplay().asyncExec(runnable);

    Button getKeyButton = new Button(subShell, SWT.PUSH);
    getKeyButton.setText(BaseMessages.getString(PKG, "MultiMergeJoinDialog.KeyFields.Button"));
    FormData fdbKeys = new FormData();
    fdbKeys.top = new FormAttachment(wKeys, margin);
    fdbKeys.left = new FormAttachment(0, 0);
    fdbKeys.right = new FormAttachment(100, -margin);
    getKeyButton.setLayoutData(fdbKeys);
    getKeyButton.addListener(
        SWT.Selection,
        e ->
            BaseTransformDialog.getFieldsFromPrevious(
                prev, wKeys, 1, new int[] {1}, new int[] {}, -1, -1, null));

    Listener onOk =
        e -> {
          int nrKeys = wKeys.nrNonEmpty();
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < nrKeys; i++) {
            TableItem item = wKeys.getNonEmpty(i);
            sb.append(item.getText(1));
            if (nrKeys > 1 && i != nrKeys - 1) {
              sb.append(",");
            }
          }
          keyValTextBox.setText(sb.toString());
          subShell.close();
        };

    // Some buttons
    Button okButton = new Button(subShell, SWT.PUSH);
    okButton.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    okButton.addListener(SWT.Selection, onOk);
    Button cancelButton = new Button(subShell, SWT.PUSH);
    cancelButton.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    cancelButton.addListener(SWT.Selection, e -> subShell.close());

    this.setButtonPositions(new Button[] {okButton, cancelButton}, margin, null);

    for (int i = 0; i < keys.length; i++) {
      TableItem item = wKeys.table.getItem(i);
      if (keys[i] != null) {
        item.setText(1, keys[i]);
      }
    }

    BaseDialog.defaultShellHandling(subShell, x -> onOk.handleEvent(null), x -> {});
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciKeys[0].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    // If no inputs configured and at least 2 transforms are attached, auto-fill from prev
    if (joinMeta.getInputTransforms() == null || joinMeta.getInputTransforms().isEmpty()) {
      String[] prev = pipelineMeta.getPrevTransformNames(transformName);
      if (prev != null && prev.length >= 2) {
        List<String> list = new ArrayList<>();
        for (String p : prev) {
          list.add(p);
        }
        joinMeta.setInputTransforms(list);
      }
    }
    // Sync from hops (rename, insert-in-the-middle) and resolve streams
    joinMeta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());

    List<String> inputTransformNames = joinMeta.getInputTransforms();
    if (inputTransformNames != null) {
      String inputTransformName;
      List<String> keyFields = joinMeta.getKeyFields();
      String keyField;

      // Create a set of available transforms for quick lookup
      List<String> availableTransforms = java.util.Arrays.asList(allInputTransforms);

      // Load transforms from metadata, but only if they still exist in the pipeline
      int widgetIndex = 0;
      for (int i = 0;
          i < inputTransformNames.size() && widgetIndex < wInputTransformArray.length;
          i++) {
        inputTransformName = Const.NVL(inputTransformNames.get(i), "");

        // Skip transforms that no longer exist in the pipeline
        if (!Utils.isEmpty(inputTransformName)
            && !availableTransforms.contains(inputTransformName)) {
          continue;
        }

        wInputTransformArray[widgetIndex].setText(inputTransformName);

        keyField = Const.NVL(i < keyFields.size() ? keyFields.get(i) : null, "");
        keyValTextBox[widgetIndex].setText(keyField);

        widgetIndex++;
      }

      while (widgetIndex < wInputTransformArray.length) {
        wInputTransformArray[widgetIndex].setText("");
        keyValTextBox[widgetIndex].setText("");
        widgetIndex++;
      }

      String joinType = joinMeta.getJoinType();
      if (!Utils.isEmpty(joinType)) {
        joinTypeCombo.setText(joinType);
      } else {
        joinTypeCombo.setText(MultiMergeJoinMeta.joinTypes[0]);
      }
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    joinMeta.setChanged(backupChanged);
    dispose();
  }

  private void getMeta(MultiMergeJoinMeta meta) {
    ArrayList<String> inputTransformNameList = new ArrayList<>();
    ArrayList<String> keyList = new ArrayList<>();
    CCombo wInputTransform;
    String inputTransformName;

    for (int i = 0; i < wInputTransformArray.length; i++) {
      wInputTransform = wInputTransformArray[i];
      inputTransformName = wInputTransform.getText();

      if (Utils.isEmpty(inputTransformName)) {
        continue;
      }

      inputTransformNameList.add(inputTransformName);
      keyList.add(keyValTextBox[i].getText());
    }

    meta.setInputTransforms(inputTransformNameList);
    meta.setKeyFields(keyList);
    meta.setJoinType(joinTypeCombo.getText());
    meta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    getMeta(joinMeta);
    // Show a warning (optional)
    if ("Y".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y"))) {
      MessageDialogWithToggle md =
          new MessageDialogWithToggle(
              shell,
              BaseMessages.getString(PKG, "MultiMergeJoinDialog.InputNeedSort.DialogTitle"),
              BaseMessages.getString(
                      PKG, "MultiMergeJoinDialog.InputNeedSort.DialogMessage", Const.CR)
                  + Const.CR,
              SWT.ICON_WARNING,
              new String[] {
                BaseMessages.getString(PKG, "MultiMergeJoinDialog.InputNeedSort.Option1")
              },
              BaseMessages.getString(PKG, "MultiMergeJoinDialog.InputNeedSort.Option2"),
              "N".equalsIgnoreCase(props.getCustomParameter(STRING_SORT_WARNING_PARAMETER, "Y")));
      md.open();
      props.setCustomParameter(STRING_SORT_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
    }
    transformName = wTransformName.getText(); // return value
    dispose();
  }

  /** Listener for Configure Keys button */
  private static class ConfigureKeyButtonListener implements Listener {
    MultiMergeJoinDialog dialog;
    Text textBox;
    int inputStreamIndex;
    ModifyListener listener;

    public ConfigureKeyButtonListener(
        MultiMergeJoinDialog dialog, Text textBox, int streamIndex, ModifyListener lsMod) {
      this.dialog = dialog;
      this.textBox = textBox;
      this.listener = lsMod;
      this.inputStreamIndex = streamIndex;
    }

    @Override
    public void handleEvent(Event event) {
      dialog.configureKeys(textBox, inputStreamIndex, listener);
    }
  }
}

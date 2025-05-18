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

package org.apache.hop.pipeline.transforms.script;

import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.script.ScriptMeta.SScript;
import org.apache.hop.pipeline.transforms.script.ScriptMeta.ScriptType;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ScriptStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.TreeEditor;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

public class ScriptDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ScriptDialog.class;

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  private CCombo wEngines;

  private TextComposite wScript;

  private ModifyListener lsMod;

  private TableView wFields;

  private Label wlPosition;

  private Tree wTree;
  private TreeItem wTreeScriptsItem;

  private Image imageActiveScript;
  private Image imageInactiveScript;
  private Image imageActiveStartScript;
  private Image imageActiveEndScript;

  private CTabFolder folder;
  private Menu cMenu;
  private Menu tMenu;

  // Support for Rename Tree
  private TreeItem[] lastItem;
  private TreeEditor editor;

  private static final int DELETE_ITEM = 0;
  private static final int ADD_ITEM = 1;
  private static final int RENAME_ITEM = 2;
  private static final int SET_ACTIVE_ITEM = 3;

  private static final int ADD_COPY = 2;
  private static final int ADD_BLANK = 1;
  private static final int ADD_DEFAULT = 0;

  private String strActiveScript;
  private String strActiveStartScript;
  private String strActiveEndScript;

  private ScriptMeta input;

  private TreeItem itemInput;

  private TreeItem itemOutput;

  private IRowMeta rowPrevStepFields;

  public ScriptDialog(
      Shell parent, IVariables variables, ScriptMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;

    try {
      imageActiveScript = GuiResource.getInstance().getImage("ui/images/script-active.svg");
      imageInactiveScript = GuiResource.getInstance().getImage("ui/images/script-inactive.svg");
      imageActiveStartScript = GuiResource.getInstance().getImage("ui/images/script-start.svg");
      imageActiveEndScript = GuiResource.getInstance().getImage("ui/images/script-end.svg");

    } catch (Exception e) {
      imageActiveScript = GuiResource.getInstance().getImageEmpty();
      imageInactiveScript = GuiResource.getInstance().getImageEmpty();
      imageActiveStartScript = GuiResource.getInstance().getImageEmpty();
      imageActiveEndScript = GuiResource.getInstance().getImageEmpty();
    }
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    lsMod =
        e -> {
          input.setChanged();
        };
    changed = input.hasChanged();

    shell.setLayout(props.createFormLayout());
    shell.setText(BaseMessages.getString(PKG, "ScriptDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(
        new Button[] {
          wOk, wCancel,
        },
        margin,
        null);

    // Filename line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "ScriptDialog.TransformName.Label"));
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

    // Script engines line
    Label wlEngines = new Label(shell, SWT.RIGHT);
    wlEngines.setText(BaseMessages.getString(PKG, "ScriptDialog.ScriptEngine.Label"));
    PropsUi.setLook(wlEngines);
    FormData fdlEngines = new FormData();
    fdlEngines.left = new FormAttachment(0, 0);
    fdlEngines.right = new FormAttachment(middle, -margin);
    fdlEngines.top = new FormAttachment(wTransformName, margin);
    wlEngines.setLayoutData(fdlEngines);
    wEngines = new CCombo(shell, SWT.LEFT | SWT.READ_ONLY | SWT.BORDER);
    List<String> scriptEngineNames = ScriptUtils.getInstance().getScriptLanguageNames();
    if (scriptEngineNames != null) {
      Collections.sort(scriptEngineNames);

      for (String engineName : scriptEngineNames) {
        wEngines.add(engineName);
      }
    }
    wEngines.select(0);

    PropsUi.setLook(wEngines);
    wEngines.addModifyListener(lsMod);
    FormData fdEngines = new FormData();
    fdEngines.left = new FormAttachment(middle, 0);
    fdEngines.top = new FormAttachment(wTransformName, margin);
    fdEngines.right = new FormAttachment(100, 0);
    wEngines.setLayoutData(fdEngines);

    SashForm wSash = new SashForm(shell, SWT.VERTICAL);
    wSash.setLayout(new FormLayout());
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment(0, 0);
    fdSashForm.top = new FormAttachment(wEngines, 0);
    fdSashForm.right = new FormAttachment(100, 0);
    fdSashForm.bottom = new FormAttachment(100, 0);
    wSash.setLayoutData(fdSashForm);

    // Top sash form
    //
    Composite wTop = new Composite(wSash, SWT.NONE);
    PropsUi.setLook(wTop);
    wTop.setLayout(props.createFormLayout());

    // Script line
    Label wlScriptFunctions = new Label(wTop, SWT.NONE);
    wlScriptFunctions.setText(BaseMessages.getString(PKG, "ScriptDialog.ScriptFunctions.Label"));
    PropsUi.setLook(wlScriptFunctions);
    FormData fdlScriptFunctions = new FormData();
    fdlScriptFunctions.left = new FormAttachment(0, 0);
    fdlScriptFunctions.top = new FormAttachment(0, 0);
    wlScriptFunctions.setLayoutData(fdlScriptFunctions);

    // Tree View Test
    wTree = new Tree(wTop, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wTree);
    FormData fdlTree = new FormData();
    fdlTree.left = new FormAttachment(0, 0);
    fdlTree.top = new FormAttachment(wlScriptFunctions, margin);
    fdlTree.right = new FormAttachment(20, 0);
    fdlTree.bottom = new FormAttachment(100, -margin);
    wTree.setLayoutData(fdlTree);

    // Script line
    Label wlScript = new Label(wTop, SWT.NONE);
    wlScript.setText(BaseMessages.getString(PKG, "ScriptDialog.Script.Label"));
    PropsUi.setLook(wlScript);
    FormData fdlScript = new FormData();
    fdlScript.left = new FormAttachment(wTree, margin);
    fdlScript.top = new FormAttachment(0, 0);
    wlScript.setLayoutData(fdlScript);

    folder = new CTabFolder(wTop, SWT.BORDER | SWT.RESIZE);
    PropsUi.setLook(folder, Props.WIDGET_STYLE_TAB);

    // This function does not allowed in the web that will lead to error
    // folder.setSimple(false);
    folder.setUnselectedImageVisible(true);
    folder.setUnselectedCloseVisible(true);
    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment(wTree, margin);
    fdScript.top = new FormAttachment(wlScript, margin);
    fdScript.right = new FormAttachment(100, -5);
    fdScript.bottom = new FormAttachment(100, -50);
    folder.setLayoutData(fdScript);

    wlPosition = new Label(wTop, SWT.NONE);
    wlPosition.setText(BaseMessages.getString(PKG, "ScriptDialog.Position.Label"));
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(wTree, margin);
    fdlPosition.right = new FormAttachment(30, 0);
    fdlPosition.top = new FormAttachment(folder, margin);
    wlPosition.setLayoutData(fdlPosition);

    Text wlHelpLabel = new Text(wTop, SWT.V_SCROLL | SWT.LEFT);
    wlHelpLabel.setEditable(false);
    wlHelpLabel.setText("Hallo");
    PropsUi.setLook(wlHelpLabel);
    FormData fdHelpLabel = new FormData();
    fdHelpLabel.left = new FormAttachment(wlPosition, margin);
    fdHelpLabel.top = new FormAttachment(folder, margin);
    fdHelpLabel.right = new FormAttachment(100, -5);
    fdHelpLabel.bottom = new FormAttachment(100, 0);
    wlHelpLabel.setLayoutData(fdHelpLabel);
    wlHelpLabel.setVisible(false);

    FormData fdTop = new FormData();
    fdTop.left = new FormAttachment(0, 0);
    fdTop.top = new FormAttachment(0, 0);
    fdTop.right = new FormAttachment(100, 0);
    fdTop.bottom = new FormAttachment(100, 0);
    wTop.setLayoutData(fdTop);

    Composite wBottom = new Composite(wSash, SWT.NONE);
    PropsUi.setLook(wBottom);

    wBottom.setLayout(props.createFormLayout());

    Label wSeparator = new Label(wBottom, SWT.SEPARATOR | SWT.HORIZONTAL);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(0, 0);
    fdSeparator.right = new FormAttachment(100, 0);
    fdSeparator.top = new FormAttachment(0, -margin + 2);
    wSeparator.setLayoutData(fdSeparator);

    Label wlFields = new Label(wBottom, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ScriptDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wSeparator, 0);
    wlFields.setLayoutData(fdlFields);

    final int nrFields = input.getFields().size();

    ColumnInfo scriptResultColumn =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.ReturnValue"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.Filename"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.RenameTo"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ScriptDialog.ColumnInfo.Replace"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YES_NO_COMBO),
          scriptResultColumn,
        };

    wFields =
        new TableView(
            variables,
            wBottom,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            nrFields,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment(0, 0);
    fdBottom.top = new FormAttachment(0, 0);
    fdBottom.right = new FormAttachment(100, 0);
    fdBottom.bottom = new FormAttachment(100, 0);
    wBottom.setLayoutData(fdBottom);

    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(wEngines, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(wOk, -2 * margin);
    wSash.setLayoutData(fdSash);

    wSash.setWeights(new int[] {75, 25});

    // Add listeners
    folder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            CTabItem cItem = (CTabItem) event.item;
            event.doit = false;
            if (cItem != null && folder.getItemCount() > 1) {
              MessageBox messageBox = new MessageBox(shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES);
              messageBox.setText(BaseMessages.getString(PKG, "ScriptDialog.DeleteItem.Label"));
              messageBox.setMessage(
                  BaseMessages.getString(
                      PKG, "ScriptDialog.ConfirmDeleteItem.Label", cItem.getText()));
              if (messageBox.open() == SWT.YES) {
                modifyScriptTree(cItem, DELETE_ITEM);
                event.doit = true;
              }
            }
          }
        });

    cMenu = new Menu(shell, SWT.POP_UP);
    buildingFolderMenu();
    tMenu = new Menu(shell, SWT.POP_UP);
    buildingTreeMenu();

    // Adding the Default Transform Scripts Item to the Tree
    wTreeScriptsItem = new TreeItem(wTree, SWT.NULL);
    wTreeScriptsItem.setImage(GuiResource.getInstance().getImageFolder());
    wTreeScriptsItem.setText(BaseMessages.getString(PKG, "ScriptDialog.TransformScript.Label"));

    // Set the shell size, based upon previous time...
    setSize();
    getData();

    // Adding the Rest (Functions, InputItems, etc.) to the Tree
    buildSpecialFunctionsTree();

    // Input Fields
    itemInput = new TreeItem(wTree, SWT.NULL);
    itemInput.setImage(GuiResource.getInstance().getImageInput());
    itemInput.setText(BaseMessages.getString(PKG, "ScriptDialog.InputFields.Label"));
    // Output Fields
    itemOutput = new TreeItem(wTree, SWT.NULL);
    itemOutput.setImage(GuiResource.getInstance().getImageOutput());
    itemOutput.setText(BaseMessages.getString(PKG, "ScriptDialog.OutputFields.Label"));

    // Display waiting message for input
    TreeItem itemWaitFieldsIn = new TreeItem(itemInput, SWT.NULL);
    itemWaitFieldsIn.setText(BaseMessages.getString(PKG, "ScriptDialog.GettingFields.Label"));
    itemWaitFieldsIn.setForeground(GuiResource.getInstance().getColorDirectory());
    itemInput.setExpanded(true);

    // Display waiting message for output
    TreeItem itemWaitFieldsOut = new TreeItem(itemOutput, SWT.NULL);
    itemWaitFieldsOut.setText(BaseMessages.getString(PKG, "ScriptDialog.GettingFields.Label"));
    itemWaitFieldsOut.setForeground(GuiResource.getInstance().getColorDirectory());
    itemOutput.setExpanded(true);

    //
    // Search the fields in the background
    //
    final Runnable runnable =
        () -> {
          TransformMeta stepMeta = pipelineMeta.findTransform(transformName);
          if (stepMeta != null) {
            try {
              rowPrevStepFields = pipelineMeta.getPrevTransformFields(variables, stepMeta);
              if (rowPrevStepFields != null) {
                setInputOutputFields();
              } else {
                // Can not get fields...end of wait message
                itemInput.removeAll();
                itemOutput.removeAll();
              }
            } catch (Exception e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    addRenameToTreeScriptItems();
    input.setChanged(changed);

    // Create the drag source on the tree
    DragSource ds = new DragSource(wTree, DND.DROP_MOVE);
    ds.setTransfer(TextTransfer.getInstance());
    ds.addDragListener(
        new DragSourceAdapter() {

          @Override
          public void dragStart(DragSourceEvent event) {
            TreeItem item = wTree.getSelection()[0];

            // Qualification where the Drag Request Comes from
            if (item != null && item.getParentItem() != null) {
              if (item.getParentItem().equals(wTreeScriptsItem)) {
                event.doit = false;
              } else if (!item.getData().equals("Function")) {
                String strInsert = (String) item.getData();
                event.doit = strInsert.equals("jsFunction");
              } else {
                event.doit = false;
              }
            } else {
              event.doit = false;
            }
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            // Set the data to be the first selected item's text
            event.data = wTree.getSelection()[0].getText();
          }
        });

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setActiveCTab(String strName) {
    if (StringUtils.isEmpty(strName)) {
      folder.setSelection(0);
    } else {
      folder.setSelection(getCTabPosition(strName));
    }
  }

  private void addCTab(String cScriptName, String strScript, int iType) {
    CTabItem item = new CTabItem(folder, SWT.CLOSE);

    if (iType == ADD_DEFAULT) {
      item.setText(Const.NVL(cScriptName, ""));
    } else {
      item.setText(getNextName(cScriptName));
    }
    if (EnvironmentUtils.getInstance().isWeb()) {
      wScript =
          new StyledTextComp(
              variables,
              item.getParent(),
              SWT.MULTI | SWT.LEFT | SWT.H_SCROLL | SWT.V_SCROLL,
              false);
    } else {
      wScript =
          new ScriptStyledTextComp(
              variables,
              item.getParent(),
              SWT.MULTI | SWT.LEFT | SWT.H_SCROLL | SWT.V_SCROLL,
              false);
      wScript.addLineStyleListener(wEngines.getText());
    }

    wScript.setText(Const.NVL(strScript, ""));

    item.setImage(imageInactiveScript);
    PropsUi.setLook(wScript, Props.WIDGET_STYLE_FIXED);

    wEngines.addModifyListener(
        e -> {
          wScript.addLineStyleListener(wEngines.getText());
          wScript.setText(wScript.getText() + " ");
        });

    wScript.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wScript.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wScript.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    wScript.addModifyListener(lsMod);
    item.setControl(wScript);

    // Adding new Item to Tree
    modifyScriptTree(item, ADD_ITEM);
  }

  private void modifyScriptTree(CTabItem ctabitem, int iModType) {

    switch (iModType) {
      case DELETE_ITEM:
        TreeItem dItem = getTreeItemByName(ctabitem.getText());
        if (dItem != null) {
          dItem.dispose();
          input.setChanged();
        }
        break;
      case ADD_ITEM:
        TreeItem item = new TreeItem(wTreeScriptsItem, SWT.NULL);
        item.setImage(imageActiveScript);
        item.setText(ctabitem.getText());
        input.setChanged();
        break;

      case RENAME_ITEM:
        input.setChanged();
        break;
      case SET_ACTIVE_ITEM:
        input.setChanged();
        break;
      default:
        break;
    }
  }

  private TreeItem getTreeItemByName(String strTabName) {
    TreeItem[] tItems = wTreeScriptsItem.getItems();
    for (TreeItem tItem : tItems) {
      if (tItem.getText().equals(strTabName)) {
        return tItem;
      }
    }
    return null;
  }

  private int getCTabPosition(String strTabName) {
    CTabItem[] cItems = folder.getItems();
    for (int i = 0; i < cItems.length; i++) {
      if (cItems[i].getText().equals(strTabName)) {
        return i;
      }
    }
    return -1;
  }

  private CTabItem getCTabItemByName(String strTabName) {
    CTabItem[] cItems = folder.getItems();
    for (CTabItem cItem : cItems) {
      if (cItem.getText().equals(strTabName)) {
        return cItem;
      }
    }
    return null;
  }

  private void modifyCTabItem(TreeItem tItem, int iModType, String strOption) {

    switch (iModType) {
      case DELETE_ITEM:
        CTabItem dItem = folder.getItem(getCTabPosition(tItem.getText()));
        if (dItem != null) {
          dItem.dispose();
          input.setChanged();
        }
        break;

      case RENAME_ITEM:
        CTabItem rItem = folder.getItem(getCTabPosition(tItem.getText()));
        if (rItem != null) {
          rItem.setText(strOption);
          input.setChanged();
          if (rItem.getImage().equals(imageActiveScript)) {
            strActiveScript = strOption;
          } else if (rItem.getImage().equals(imageActiveStartScript)) {
            strActiveStartScript = strOption;
          } else if (rItem.getImage().equals(imageActiveEndScript)) {
            strActiveEndScript = strOption;
          }
        }
        break;
      case SET_ACTIVE_ITEM:
        CTabItem aItem = folder.getItem(getCTabPosition(tItem.getText()));
        if (aItem != null) {
          input.setChanged();
          strActiveScript = tItem.getText();
          for (int i = 0; i < folder.getItemCount(); i++) {
            if (folder.getItem(i).equals(aItem)) {
              aItem.setImage(imageActiveScript);
            } else {
              folder.getItem(i).setImage(imageInactiveScript);
            }
          }
        }
        break;
      default:
        break;
    }
  }

  private TextComposite getStyledTextComp() {
    CTabItem item = folder.getSelection();
    if (item.getControl().isDisposed()) {
      return null;
    } else {
      return (TextComposite) item.getControl();
    }
  }

  private TextComposite getStyledTextComp(CTabItem item) {
    return (TextComposite) item.getControl();
  }

  private String getNextName(String strActualName) {
    String strRC = "";
    if (strActualName.isEmpty()) {
      strActualName = "Item";
    }

    int i = 0;
    strRC = strActualName + "_" + i;
    while (getCTabItemByName(strRC) != null) {
      i++;
      strRC = strActualName + "_" + i;
    }
    return strRC;
  }

  public void setPosition() {
    TextComposite wScript = getStyledTextComp();
    if (wScript == null) {
      return;
    }
    int lineNumber = wScript.getLineNumber();
    int columnNumber = wScript.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(PKG, "ScriptDialog.Position.Label2")
            + lineNumber
            + ", "
            + columnNumber);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    String engineName = input.getLanguageName();
    if (StringUtils.isEmpty(engineName)
        || !ScriptUtils.getInstance().getScriptLanguageNames().contains(engineName)) {
      wEngines.select(0);
    } else {
      wEngines.setText(engineName);
    }

    for (int i = 0; i < input.getFields().size(); i++) {
      ScriptMeta.SField field = input.getFields().get(i);

      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getRename(), ""));
      item.setText(3, Const.NVL(field.getType(), ""));
      if (field.getLength() >= 0) {
        item.setText(4, "" + field.getLength());
      }
      if (field.getPrecision() >= 0) {
        item.setText(5, "" + field.getPrecision());
      }
      item.setText(6, field.isReplace() ? YES_NO_COMBO[1] : YES_NO_COMBO[0]);
      item.setText(7, field.isScriptResult() ? YES_NO_COMBO[1] : YES_NO_COMBO[0]);
    }

    List<SScript> jsScripts = input.getScripts();
    if (!jsScripts.isEmpty()) {
      for (int i = 0; i < jsScripts.size(); i++) {
        SScript script = jsScripts.get(i);
        if (script.isTransformScript()) {
          strActiveScript = script.getScriptName();
        } else if (script.isStartScript()) {
          strActiveStartScript = script.getScriptName();
        } else if (script.isEndScript()) {
          strActiveEndScript = script.getScriptName();
        }
        addCTab(script.getScriptName(), script.getScript(), ADD_DEFAULT);
      }
    } else {
      addCTab("script1", "", ADD_DEFAULT);
    }
    setActiveCTab(strActiveScript);
    refresh();

    wFields.setRowNums();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  // Setting default active Script

  private void refresh() {
    for (int i = 0; i < folder.getItemCount(); i++) {
      CTabItem item = folder.getItem(i);
      if (item.getText().equals(strActiveScript)) {
        item.setImage(imageActiveScript);
      } else if (item.getText().equals(strActiveStartScript)) {
        item.setImage(imageActiveStartScript);
      } else if (item.getText().equals(strActiveEndScript)) {
        item.setImage(imageActiveEndScript);
      } else {
        item.setImage(imageInactiveScript);
      }
    }
  }

  private boolean cancel() {
    if (input.hasChanged()) {
      MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.APPLICATION_MODAL);
      box.setText(BaseMessages.getString(PKG, "ScriptDialog.WarningDialogChanged.Title"));
      box.setMessage(
          BaseMessages.getString(PKG, "ScriptDialog.WarningDialogChanged.Message", Const.CR));
      int answer = box.open();

      if (answer == SWT.NO) {
        return false;
      }
    }
    transformName = null;
    input.setChanged(changed);
    dispose();
    return true;
  }

  private void getInfo(ScriptMeta meta) {
    meta.setLanguageName(wEngines.getText());
    meta.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      ScriptMeta.SField field = new ScriptMeta.SField();

      field.setName(item.getText(1));
      field.setRename(item.getText(2));
      if (field.getRename() == null
          || field.getRename().isEmpty()
          || field.getRename().equalsIgnoreCase(field.getName())) {
        field.setRename(field.getName());
      }
      field.setType(item.getText(3));
      String slen = item.getText(4);
      String sprc = item.getText(5);
      field.setLength(Const.toInt(slen, -1));
      field.setPrecision(Const.toInt(sprc, -1));
      field.setReplace(YES_NO_COMBO[1].equalsIgnoreCase(item.getText(6)));
      field.setScriptResult(YES_NO_COMBO[1].equalsIgnoreCase(item.getText(7)));

      meta.getFields().add(field);
    }

    meta.getScripts().clear();
    CTabItem[] cTabs = folder.getItems();
    if (cTabs.length > 0) {
      for (int i = 0; i < cTabs.length; i++) {
        SScript jsScript =
            new SScript(
                ScriptType.NORMAL_SCRIPT,
                cTabs[i].getText(),
                getStyledTextComp(cTabs[i]).getText());
        if (cTabs[i].getImage().equals(imageActiveScript)) {
          jsScript.setScriptType(ScriptType.TRANSFORM_SCRIPT);
        } else if (cTabs[i].getImage().equals(imageActiveStartScript)) {
          jsScript.setScriptType(ScriptType.START_SCRIPT);
        } else if (cTabs[i].getImage().equals(imageActiveEndScript)) {
          jsScript.setScriptType(ScriptType.END_SCRIPT);
        }
        meta.getScripts().add(jsScript);
      }
    }
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    boolean bInputOK = false;

    // Check if Active Script has set, otherwise Ask
    if (getCTabItemByName(strActiveScript) == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.CANCEL | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "ScriptDialog.NoActiveScriptSet"));
      mb.setText(BaseMessages.getString(PKG, "ScriptDialog.ERROR.Label"));
      switch (mb.open()) {
        case SWT.OK:
          strActiveScript = folder.getItem(0).getText();
          refresh();
          bInputOK = true;
          break;
        case SWT.CANCEL:
        default:
          break;
      }
    } else {
      bInputOK = true;
    }

    if (bInputOK) {
      getInfo(input);
      dispose();
    }
  }

  private void buildSpecialFunctionsTree() {

    TreeItem item = new TreeItem(wTree, SWT.NULL);
    item.setImage(GuiResource.getInstance().getImageFolder());
    item.setText(BaseMessages.getString(PKG, "ScriptDialog.TransformConstant.Label"));
    TreeItem itemT = new TreeItem(item, SWT.NULL);
    itemT.setImage(GuiResource.getInstance().getImageLabel());
    itemT.setText("SKIP_TRANSFORMATION");
    itemT.setData("SKIP_TRANSFORMATION");
    itemT = new TreeItem(item, SWT.NULL);
    itemT.setImage(GuiResource.getInstance().getImageLabel());
    itemT.setText("ERROR_TRANSFORMATION");
    itemT.setData("ERROR_TRANSFORMATION");
    itemT = new TreeItem(item, SWT.NULL);
    itemT.setImage(GuiResource.getInstance().getImageLabel());
    itemT.setText("CONTINUE_TRANSFORMATION");
    itemT.setData("CONTINUE_TRANSFORMATION");
  }

  public boolean checkTreeItemExist(TreeItem itemToCheck, String strItemName) {
    boolean bRC = false;
    if (itemToCheck.getItemCount() > 0) {
      TreeItem[] items = itemToCheck.getItems();
      for (int i = 0; i < items.length; i++) {
        if (items[i].getText().equals(strItemName)) {
          return true;
        }
      }
    }
    return bRC;
  }

  private void setInputOutputFields() {
    shell
        .getDisplay()
        .syncExec(
            () -> {
              // fields are got...end of wait message
              itemInput.removeAll();
              itemOutput.removeAll();

              String strItemInToAdd = "";
              String strItemToAddOut = "";

              if (rowPrevStepFields != null) {

                for (int i = 0; i < rowPrevStepFields.size(); i++) {
                  IValueMeta v = rowPrevStepFields.getValueMeta(i);
                  strItemToAddOut = v.getName();
                  strItemInToAdd = v.getName();

                  TreeItem itemFields = new TreeItem(itemInput, SWT.NULL);
                  itemFields.setImage(GuiResource.getInstance().getImage(v));
                  itemFields.setText(strItemInToAdd);
                  itemFields.setData(strItemInToAdd);

                  itemFields = new TreeItem(itemOutput, SWT.NULL);
                  itemFields.setImage(GuiResource.getInstance().getImage(v));
                  itemFields.setText(strItemToAddOut);
                  itemFields.setData(strItemToAddOut);
                }

                for (int i = 0; i < wFields.nrNonEmpty(); i++) {
                  TableItem item = wFields.getNonEmpty(i);
                  String fieldName = item.getText(1);
                  String renameField = item.getText(2);
                  strItemToAddOut = StringUtils.isEmpty(renameField) ? fieldName : renameField;
                  TreeItem itemFields = new TreeItem(itemOutput, SWT.NULL);
                  itemFields.setImage(GuiResource.getInstance().getImageLabel());
                  itemFields.setText(strItemToAddOut);
                  itemFields.setData(strItemToAddOut);
                }
              }
            });
  }

  private void buildingFolderMenu() {
    MenuItem addNewItem = new MenuItem(cMenu, SWT.PUSH);
    addNewItem.setText(BaseMessages.getString(PKG, "ScriptDialog.AddNewTab"));
    addNewItem.addListener(SWT.Selection, e -> addCTab("", "", ADD_BLANK));

    MenuItem copyItem = new MenuItem(cMenu, SWT.PUSH);
    copyItem.setText(BaseMessages.getString(PKG, "ScriptDialog.AddCopy"));
    copyItem.addListener(
        SWT.Selection,
        e -> {
          CTabItem item = folder.getSelection();
          StyledTextComp st = (StyledTextComp) item.getControl();
          addCTab(item.getText(), st.getText(), ADD_COPY);
        });
    new MenuItem(cMenu, SWT.SEPARATOR);

    MenuItem setActiveScriptItem = new MenuItem(cMenu, SWT.PUSH);
    setActiveScriptItem.setText(BaseMessages.getString(PKG, "ScriptDialog.SetTransformScript"));
    setActiveScriptItem.addListener(
        SWT.Selection,
        e -> {
          CTabItem item = folder.getSelection();
          for (int i = 0; i < folder.getItemCount(); i++) {
            if (folder.getItem(i).equals(item)) {
              if (item.getImage().equals(imageActiveScript)) {
                strActiveScript = "";
              } else if (item.getImage().equals(imageActiveStartScript)) {
                strActiveStartScript = "";
              } else if (item.getImage().equals(imageActiveEndScript)) {
                strActiveEndScript = "";
              }
              item.setImage(imageActiveScript);
              strActiveScript = item.getText();
            } else if (folder.getItem(i).getImage().equals(imageActiveScript)) {
              folder.getItem(i).setImage(imageInactiveScript);
            }
          }
          modifyScriptTree(item, SET_ACTIVE_ITEM);
        });

    MenuItem setStartScriptItem = new MenuItem(cMenu, SWT.PUSH);
    setStartScriptItem.setText(BaseMessages.getString(PKG, "ScriptDialog.SetStartScript"));
    setStartScriptItem.addListener(
        SWT.Selection,
        e -> {
          CTabItem item = folder.getSelection();
          for (int i = 0; i < folder.getItemCount(); i++) {
            if (folder.getItem(i).equals(item)) {
              if (item.getImage().equals(imageActiveScript)) {
                strActiveScript = "";
              } else if (item.getImage().equals(imageActiveStartScript)) {
                strActiveStartScript = "";
              } else if (item.getImage().equals(imageActiveEndScript)) {
                strActiveEndScript = "";
              }
              item.setImage(imageActiveStartScript);
              strActiveStartScript = item.getText();
            } else if (folder.getItem(i).getImage().equals(imageActiveStartScript)) {
              folder.getItem(i).setImage(imageInactiveScript);
            }
          }
          modifyScriptTree(item, SET_ACTIVE_ITEM);
        });

    MenuItem setEndScriptItem = new MenuItem(cMenu, SWT.PUSH);
    setEndScriptItem.setText(BaseMessages.getString(PKG, "ScriptDialog.SetEndScript"));
    setEndScriptItem.addListener(
        SWT.Selection,
        e -> {
          CTabItem item = folder.getSelection();
          for (int i = 0; i < folder.getItemCount(); i++) {
            if (folder.getItem(i).equals(item)) {
              if (item.getImage().equals(imageActiveScript)) {
                strActiveScript = "";
              } else if (item.getImage().equals(imageActiveStartScript)) {
                strActiveStartScript = "";
              } else if (item.getImage().equals(imageActiveEndScript)) {
                strActiveEndScript = "";
              }
              item.setImage(imageActiveEndScript);
              strActiveEndScript = item.getText();
            } else if (folder.getItem(i).getImage().equals(imageActiveEndScript)) {
              folder.getItem(i).setImage(imageInactiveScript);
            }
          }
          modifyScriptTree(item, SET_ACTIVE_ITEM);
        });
    new MenuItem(cMenu, SWT.SEPARATOR);
    MenuItem setRemoveScriptItem = new MenuItem(cMenu, SWT.PUSH);
    setRemoveScriptItem.setText(BaseMessages.getString(PKG, "ScriptDialog.RemoveScriptType"));
    setRemoveScriptItem.addListener(
        SWT.Selection,
        e -> {
          CTabItem item = folder.getSelection();
          input.setChanged(true);
          if (item.getImage().equals(imageActiveScript)) {
            strActiveScript = "";
          } else if (item.getImage().equals(imageActiveStartScript)) {
            strActiveStartScript = "";
          } else if (item.getImage().equals(imageActiveEndScript)) {
            strActiveEndScript = "";
          }
          item.setImage(imageInactiveScript);
        });

    folder.setMenu(cMenu);
  }

  private void buildingTreeMenu() {
    MenuItem addDeleteItem = new MenuItem(tMenu, SWT.PUSH);
    addDeleteItem.setText(BaseMessages.getString(PKG, "ScriptDialog.Delete.Label"));
    addDeleteItem.addListener(
        SWT.Selection,
        e -> {
          if (wTree.getSelectionCount() <= 0) {
            return;
          }

          TreeItem tItem = wTree.getSelection()[0];
          if (tItem != null) {
            MessageBox messageBox = new MessageBox(shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES);
            messageBox.setText(BaseMessages.getString(PKG, "ScriptDialog.DeleteItem.Label"));
            messageBox.setMessage(
                BaseMessages.getString(
                    PKG, "ScriptDialog.ConfirmDeleteItem.Label", tItem.getText()));
            if (messageBox.open() == SWT.YES) {
              modifyCTabItem(tItem, DELETE_ITEM, "");
              tItem.dispose();
              input.setChanged();
            }
          }
        });

    MenuItem renItem = new MenuItem(tMenu, SWT.PUSH);
    renItem.setText(BaseMessages.getString(PKG, "ScriptDialog.Rename.Label"));
    renItem.addListener(SWT.Selection, e -> renameFunction(wTree.getSelection()[0]));

    wTree.addListener(
        SWT.MouseDown,
        e -> {
          if (wTree.getSelectionCount() <= 0) {
            return;
          }

          TreeItem tItem = wTree.getSelection()[0];
          if (tItem != null) {
            TreeItem pItem = tItem.getParentItem();

            if (pItem != null && pItem.equals(wTreeScriptsItem)) {
              tMenu.getItem(0).setEnabled(folder.getItemCount() > 1);
              tMenu.getItem(1).setEnabled(true);
            } else {
              tMenu.getItem(0).setEnabled(false);
              tMenu.getItem(1).setEnabled(false);
            }
          }
        });
    wTree.setMenu(tMenu);
  }

  private void addRenameToTreeScriptItems() {
    lastItem = new TreeItem[1];
    editor = new TreeEditor(wTree);
    editor.horizontalAlignment = SWT.LEFT;
    editor.grabHorizontal = true;

    wTree.addListener(
        SWT.Selection,
        event -> {
          final TreeItem item = (TreeItem) event.item;
          renameFunction(item);
        });
  }

  // This function is for a Windows Like renaming inside the tree
  private void renameFunction(TreeItem item) {
    if (item != null
        && item.getParentItem() != null
        && item.getParentItem().equals(wTreeScriptsItem)
        && item == lastItem[0]) {

      final Text text = new Text(wTree, SWT.BORDER);
      text.addListener(
          SWT.FocusOut,
          event -> {
            if (!text.getText().isEmpty() && getCTabItemByName(text.getText()) == null) {
              // Check if the name Exists
              modifyCTabItem(item, RENAME_ITEM, text.getText());
              item.setText(text.getText());
            }
            text.dispose();
          });
      text.addListener(
          SWT.Traverse,
          event -> {
            switch (event.detail) {
              case SWT.TRAVERSE_RETURN:
                if (!text.getText().isEmpty() && getCTabItemByName(text.getText()) == null) {
                  // Check if the name Exists
                  modifyCTabItem(item, RENAME_ITEM, text.getText());
                  item.setText(text.getText());
                }
                text.dispose();
                break;
              case SWT.TRAVERSE_ESCAPE:
                text.dispose();
                event.doit = false;
                break;
              default:
                break;
            }
          });

      editor.setEditor(text, item);
      text.setText(item.getText());
      text.selectAll();
      text.setFocus();
    }
    lastItem[0] = item;
  }

  private void checkAndUpdateScript() {
    if (wScript != null) {
      wScript.addLineStyleListener(wEngines.getText());
    }
  }
}

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

package org.apache.hop.ui.core.dialog;

import java.util.Stack;
import java.util.StringTokenizer;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.DropTargetListener;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowData;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;

/**
 * This dialogs allows you to select a ordered number of items from a list of strings.
 */
public class EnterOrderedListDialog extends Dialog {
  private static final Class<?> PKG = EnterOrderedListDialog.class;

  public static final int LARGE_MARGIN = 15;

  private String[] input;
  private String[] retval;

  private Stack<String> selection = new Stack<>();

  private Shell shell;
  private List wListSource;
  private List wListTarget;
  private Button wButtonAdd;
  private Button wButtonAddAll;
  private Button wButtonRemoveAll;
  private Button wButtonRemove;
  private Button wButtonUp;
  private Button wDuttonDown;

  private boolean opened;

  public EnterOrderedListDialog(Shell parent, int style, String[] input) {
    super(parent, style);

    this.input = input;
    this.retval = null;
    this.opened = false;
  }

  public String[] open() {
    PropsUi props = PropsUi.getInstance();
    
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "EnterOrderedListDialog.Title"));
    shell.setMinimumSize(500, 380);
    shell.setLayout(new FormLayout());

    // *******************************************************************
    // Top & Bottom regions.
    // *******************************************************************
    Composite top = new Composite(shell, SWT.NONE);
    FormLayout topLayout = new FormLayout();
    topLayout.marginHeight = LARGE_MARGIN;
    topLayout.marginWidth = LARGE_MARGIN;
    top.setLayout(topLayout);
    top.setLayoutData(new FormDataBuilder().top().bottom(100, -50).left().right(100, 0).result());
    props.setLook(top);

    Composite bottom = new Composite(shell, SWT.NONE);
    FormLayout bottomLayout = new FormLayout();
    bottomLayout.marginHeight = LARGE_MARGIN;
    bottomLayout.marginWidth = LARGE_MARGIN;
    bottom.setLayout(bottomLayout);
    bottom.setLayoutData(new FormDataBuilder().top(top, 0).bottom().right().result());
    props.setLook(bottom);

    // *******************************************************************
    // LEFT PANE
    // *******************************************************************

    Composite leftPane = new Composite(top, SWT.NONE);
    leftPane.setLayout(new FormLayout());
    leftPane.setLayoutData(new FormDataBuilder().top().left().bottom(100, 0).right(50, -36).result());
    props.setLook(leftPane);

    // Source list to the left...
    Label lblListSource = new Label(leftPane, SWT.NONE);
    lblListSource.setText(BaseMessages.getString(PKG, "EnterOrderedListDialog.AvailableItems.Label"));
    lblListSource.setLayoutData(new FormDataBuilder().top().left().result());
    props.setLook(lblListSource);

    wListSource = new List(leftPane, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    wListSource.setLayoutData(new FormDataBuilder().top(lblListSource, ConstUi.SMALL_MARGIN).left().bottom(100, 0).right(100, 0).result());
    wListSource.addListener(SWT.Selection, e -> updateButton()); 
    wListSource.addListener(SWT.DefaultSelection, e -> addToSelection(wListSource.getSelection()));
    wListSource.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent event) {
            if (event.character == SWT.CR) {
              addToSelection(wListSource.getSelection());
            }
          }
        });    
    props.setLook(wListSource);

    // *******************************************************************
    // MIDDLE
    // *******************************************************************

    Composite middlePane = new Composite(top, SWT.NONE);
    middlePane.setLayout(new FormLayout());
    middlePane.setLayoutData(
        new FormDataBuilder().top().left(leftPane, -36).bottom(100, 0).right(50, 36).result());
    props.setLook(middlePane);

    Label label = new Label(middlePane, SWT.NONE);
    // label.setText("TEST");
    label.setLayoutData(new FormDataBuilder().top().left().result());
    props.setLook(label);

    Composite gButtonGroup = new Composite(middlePane, SWT.NONE);
    RowLayout layout = new RowLayout();
    layout.wrap = false;
    layout.type = SWT.VERTICAL;
    layout.marginTop = 0;
    layout.marginLeft = LARGE_MARGIN;
    layout.marginRight = LARGE_MARGIN;

    gButtonGroup.setLayout(layout);
    gButtonGroup.setLayoutData(
        new FormDataBuilder()
            .top(label, ConstUi.SMALL_MARGIN)
            .left()
            .bottom(100, 0)
            .right(100, 0)
            .result());
    props.setLook(gButtonGroup);

    wButtonAdd = new Button(gButtonGroup, SWT.PUSH);
    wButtonAdd.setImage(GuiResource.getInstance().getImageAddSingle());
    wButtonAdd.setToolTipText(BaseMessages.getString(PKG, "EnterOrderedListDialog.AddOne.Tooltip"));
    wButtonAdd.setLayoutData(new RowData(32, 32));
    wButtonAdd.addListener(SWT.Selection, e -> addToSelection(wListSource.getSelection()));

    wButtonAddAll = new Button(gButtonGroup, SWT.PUSH);
    wButtonAddAll.setImage(GuiResource.getInstance().getImageAddAll());
    wButtonAddAll.setToolTipText(BaseMessages.getString(PKG, "EnterOrderedListDialog.AddAll.Tooltip"));
    wButtonAddAll.setLayoutData(new RowData(32, 32));
    wButtonAddAll.addListener(SWT.Selection, e -> addToSelection(wListSource.getItems()));

    wButtonRemove = new Button(gButtonGroup, SWT.PUSH);
    wButtonRemove.setImage(GuiResource.getInstance().getImageRemoveSingle());
    wButtonRemove.setToolTipText(BaseMessages.getString(PKG, "EnterOrderedListDialog.RemoveOne.Tooltip"));
    wButtonRemove.setLayoutData(new RowData(32, 32));
    wButtonRemove.addListener(SWT.Selection, e -> removeFromSelection(wListTarget.getSelection()));

    wButtonRemoveAll = new Button(gButtonGroup, SWT.PUSH);
    wButtonRemoveAll.setImage(GuiResource.getInstance().getImageRemoveAll());
    wButtonRemoveAll.setToolTipText(BaseMessages.getString(PKG, "EnterOrderedListDialog.RemoveAll.Tooltip"));
    wButtonRemoveAll.setLayoutData(new RowData(32, 32));
    wButtonRemoveAll.addListener(SWT.Selection, e -> removeFromSelection(wListTarget.getItems()));

    wButtonUp = new Button(gButtonGroup, SWT.PUSH);
    wButtonUp.setImage(GuiResource.getInstance().getImageUp());
    wButtonUp.setToolTipText(BaseMessages.getString(PKG, "EnterOrderedListDialog.Up.Tooltip"));
    wButtonUp.setLayoutData(new RowData(32, 32));
    wButtonUp.addListener(SWT.Selection, e -> upToSelection(wListTarget.getSelection()));

    wDuttonDown = new Button(gButtonGroup, SWT.PUSH);
    wDuttonDown.setImage(GuiResource.getInstance().getImageDown());
    wDuttonDown.setToolTipText(BaseMessages.getString(PKG, "EnterOrderedListDialog.Down.Tooltip"));
    wDuttonDown.setLayoutData(new RowData(32, 32));
    wDuttonDown.addListener(SWT.Selection, e -> downToSelection(wListTarget.getSelection()));

    /* Compute the offset */
    wButtonAddAll.pack();
    int offset = wButtonAddAll.computeSize(SWT.DEFAULT, SWT.DEFAULT).x / 2;

    FormData fdButtonGroup = new FormData();
    wButtonAddAll.pack(); // get a size
    fdButtonGroup.left = new FormAttachment(50, -(wButtonAddAll.getSize().x / 2) - 5);
    fdButtonGroup.top = new FormAttachment(50, -offset);
    gButtonGroup.setLayoutData(
        new FormDataBuilder().top(label, ConstUi.SMALL_MARGIN).left(50, -offset).result());

    // *******************************************************************
    // RIGHT
    // *******************************************************************
    Composite rightPane = new Composite(top, SWT.NONE);
    rightPane.setLayout(new FormLayout());
    rightPane.setLayoutData(new FormDataBuilder().top().left(middlePane, 0).bottom(100, 0).right(100, 0).result());
    props.setLook(rightPane);

    Label lblListTarget = new Label(rightPane, SWT.NONE);
    lblListTarget.setText(BaseMessages.getString(PKG, "EnterOrderedListDialog.Selection.Label"));
    lblListTarget.setLayoutData(new FormDataBuilder().top().left().result());
    props.setLook(lblListTarget);

    wListTarget = new List(rightPane, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL);
    wListTarget.setLayoutData(new FormDataBuilder().top(lblListTarget, ConstUi.SMALL_MARGIN).left().bottom(100, 0).right(100, 0).result());
    wListTarget.addListener(SWT.Selection, e -> updateButton()); 
    wListTarget.addListener(SWT.DefaultSelection, e -> removeFromSelection(wListTarget.getSelection()));
    wListTarget.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent event) {
            if (event.character == SWT.CR) {
              removeFromSelection(wListTarget.getSelection());
            }
          }
        });    
    props.setLook(wListTarget);

    // *******************************************************************
    // THE BOTTOM BUTTONS...
    // *******************************************************************

    Button wCancel = new Button(bottom, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> dispose());

    Button wOK = new Button(bottom, SWT.PUSH);
    wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOK.addListener(SWT.Selection, e -> ok());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOK, wCancel}, props.getMargin(), null);
    
    // Drag & Drop for steps
    Transfer[] ttypes = new Transfer[] {TextTransfer.getInstance()};

    DragSource ddSource = new DragSource(wListSource, DND.DROP_MOVE | DND.DROP_COPY);
    ddSource.setTransfer(ttypes);
    ddSource.addDragListener(
        new DragSourceListener() {
          public void dragStart(DragSourceEvent event) {}

          public void dragSetData(DragSourceEvent event) {
            String[] ti = wListSource.getSelection();
            String data = new String();
            for (int i = 0; i < ti.length; i++) {
              data += ti[i] + Const.CR;
            }
            event.data = data;
          }

          public void dragFinished(DragSourceEvent event) {}
        });
    DropTarget ddTarget = new DropTarget(wListTarget, DND.DROP_MOVE | DND.DROP_COPY);
    ddTarget.setTransfer(ttypes);
    ddTarget.addDropListener(
        new DropTargetListener() {
          public void dragEnter(DropTargetEvent event) {}

          public void dragLeave(DropTargetEvent event) {}

          public void dragOperationChanged(DropTargetEvent event) {}

          @Override
          public void dragOver(DropTargetEvent event) {
            event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_SCROLL | DND.FEEDBACK_INSERT_AFTER;
          }

          public void drop(DropTargetEvent event) {
            if (event.data == null) { // no data to copy, indicate failure
              // in event.detail
              event.detail = DND.DROP_NONE;
              return;
            }
            StringTokenizer strtok = new StringTokenizer((String) event.data, Const.CR);
            while (strtok.hasMoreTokens()) {
              String source = strtok.nextToken();
              addToSelection(source);
            }
          }

          public void dropAccept(DropTargetEvent event) {}
        });

    opened = true;
    update();

    BaseTransformDialog.setSize(shell);

    shell.open();
    Display display = shell.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }

    return retval;
  }

  protected void update() {
    if (!opened) {
      return;
    }

    wListSource.removeAll();
    for (String element : input) {
      // Not selected: show in source only!
      if (selection.indexOf(element) < 0) {
        wListSource.add(element);
      }
    }

    String[] currentSelection = wListTarget.getSelection();
    wListTarget.removeAll();
    for (String element : selection) {
      wListTarget.add(element);
    }
    wListTarget.setSelection(currentSelection);

    this.updateButton();
  }

  protected void updateButton() {
    // Update button
    int index = wListTarget.getSelectionIndex();

    wButtonAdd.setEnabled(wListSource.getSelectionIndex() >= 0);
    wButtonAddAll.setEnabled(wListSource.getItemCount() > 0);

    wButtonRemove.setEnabled(index >= 0);
    wButtonRemoveAll.setEnabled(selection.size() > 0);

    wButtonUp.setEnabled(selection.size() > 1 && index > 0);
    wDuttonDown.setEnabled(selection.size() > 1 && index >= 0 && index < selection.size() - 1);
  }

  public void addToSelection(String... elements) {
    for (String element : elements) {

      if (Utils.isEmpty(element)) continue;
      selection.push(element);
    }

    update();
  }

  public void removeFromSelection(String... elements) {
    for (String element : elements) {
      selection.remove(element);
    }

    update();
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);
    PropsUi.getInstance().setScreen(winprop);
    shell.dispose();
  }

  public void ok() {
    retval = wListTarget.getItems();
    dispose();
  }

  /** Moves the currently selected item up. */
  private void upToSelection(String... elements) {

    for (String element : elements) {
      int index = selection.indexOf(element);
      if (index > 0) {
        selection.remove(index);
        selection.add(index - 1, element);
      } else break;
    }
    update();
  }

  /** Moves the currently selected item down. */
  private void downToSelection(String... elements) {

    for (int i = elements.length - 1; i >= 0; i--) {
      String element = elements[i];
      int index = selection.indexOf(element);
      if (index < selection.size() - 1) {
        selection.remove(index);
        selection.add(index + 1, element);
      } else break;
    }
    update();
  }
}

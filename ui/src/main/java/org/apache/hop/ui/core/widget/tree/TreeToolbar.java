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

package org.apache.hop.ui.core.widget.tree;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

/** Created by bmorrise on 6/27/18. */
public class TreeToolbar extends Composite {

  private PropsUi props = PropsUi.getInstance();
  private Text selectionFilter;
  private ToolItem expandAll;
  private ToolItem collapseAll;

  public TreeToolbar(Composite composite, int i) {
    super(composite, i);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 0;
    formLayout.marginHeight = 0;
    formLayout.marginTop = 0;
    formLayout.marginBottom = 0;

    this.setLayout(formLayout);

    Label sep3 = new Label(this, SWT.SEPARATOR | SWT.HORIZONTAL);
    sep3.setBackground(GuiResource.getInstance().getColorWhite());
    FormData fdSep3 = new FormData();
    fdSep3.left = new FormAttachment(0, 0);
    fdSep3.right = new FormAttachment(100, 0);
    fdSep3.top = new FormAttachment(0);
    sep3.setLayoutData(fdSep3);

    ToolBar treeTb = new ToolBar(this, SWT.HORIZONTAL | SWT.FLAT);
    props.setLook(treeTb, Props.WIDGET_STYLE_TOOLBAR);
    /*
     This contains a map with all the unnamed pipeline (just a filename)
    */
    expandAll = new ToolItem(treeTb, SWT.PUSH);
    expandAll.setImage(GuiResource.getInstance().getImageExpandAll());
    collapseAll = new ToolItem(treeTb, SWT.PUSH);
    collapseAll.setImage(GuiResource.getInstance().getImageCollapseAll());

    FormData fdTreeToolbar = new FormData();
    if (Const.isLinux()) {
      fdTreeToolbar.top = new FormAttachment(sep3, 3);
    } else {
      fdTreeToolbar.top = new FormAttachment(sep3, 5);
    }
    fdTreeToolbar.right = new FormAttachment(100, -10);
    treeTb.setLayoutData(fdTreeToolbar);

    ToolBar selectionFilterTb = new ToolBar(this, SWT.HORIZONTAL | SWT.FLAT);
    props.setLook(selectionFilterTb, Props.WIDGET_STYLE_TOOLBAR);

    ToolItem clearSelectionFilter = new ToolItem(selectionFilterTb, SWT.PUSH);
    clearSelectionFilter.setImage(GuiResource.getInstance().getImageClearText());
    // clearSelectionFilter.setDisabledImage( GuiResource.getInstance().getImageClearTextDisabled()
    // );

    FormData fdSelectionFilterToolbar = new FormData();
    if (Const.isLinux()) {
      fdSelectionFilterToolbar.top = new FormAttachment(sep3, 3);
    } else {
      fdSelectionFilterToolbar.top = new FormAttachment(sep3, 5);
    }
    fdSelectionFilterToolbar.right = new FormAttachment(treeTb, -20);
    selectionFilterTb.setLayoutData(fdSelectionFilterToolbar);

    selectionFilter = new Text(this, SWT.SINGLE | SWT.BORDER | SWT.LEFT | SWT.SEARCH);
    FormData fdSelectionFilter = new FormData();
    int offset = -(GuiResource.getInstance().getImageClearText().getBounds().height + 6);
    if (Const.isLinux()) {
      offset = -(GuiResource.getInstance().getImageClearText().getBounds().height + 13);
    }

    fdSelectionFilter.top = new FormAttachment(selectionFilterTb, offset);
    fdSelectionFilter.right = new FormAttachment(selectionFilterTb, 0);
    fdSelectionFilter.left = new FormAttachment(0, 10);
    selectionFilter.setLayoutData(fdSelectionFilter);

    clearSelectionFilter.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            selectionFilter.setText("");
          }
        });

    clearSelectionFilter.setEnabled(!Utils.isEmpty(selectionFilter.getText()));

    selectionFilter.addModifyListener(
        modifyEvent -> {
          clearSelectionFilter.setEnabled(!Utils.isEmpty(selectionFilter.getText()));
        });

    Label sep4 = new Label(this, SWT.SEPARATOR | SWT.HORIZONTAL);
    sep4.setBackground(GuiResource.getInstance().getColorWhite());
    FormData fdSep4 = new FormData();
    fdSep4.left = new FormAttachment(0, 0);
    fdSep4.right = new FormAttachment(100, 0);
    fdSep4.top = new FormAttachment(treeTb, 5);
    sep4.setLayoutData(fdSep4);
  }

  public void setSearchTooltip(String tooltip) {
    selectionFilter.setToolTipText(tooltip);
  }

  public void setSearchPlaceholder(String searchPlaceholder) {
    selectionFilter.setMessage(searchPlaceholder);
  }

  public void addSearchModifyListener(ModifyListener modifyListener) {
    selectionFilter.addModifyListener(modifyListener);
  }

  public void addExpandAllListener(SelectionAdapter selectionAdapter) {
    expandAll.addSelectionListener(selectionAdapter);
  }

  public void addCollapseAllListener(SelectionAdapter selectionAdapter) {
    collapseAll.addSelectionListener(selectionAdapter);
  }

  public String getSearchText() {
    return selectionFilter.getText();
  }

  @Override
  public boolean setFocus() {
    return selectionFilter.setFocus();
  }

  public void clear() {
    selectionFilter.setText("");
  }
}

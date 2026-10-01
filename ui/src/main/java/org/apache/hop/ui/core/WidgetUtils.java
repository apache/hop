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

package org.apache.hop.ui.core;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ComboVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ToolBar;

public abstract class WidgetUtils {
  private static final String TOP_RIGHT_ALIGNING = "hop.alignTopRight";

  private WidgetUtils() {}

  public static void setFormLayout(Composite composite, int margin) {
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = margin;
    formLayout.marginHeight = margin;
    composite.setLayout(formLayout);
  }

  /**
   * creates a ComboVar populated with fields from the previous transform.
   *
   * @param parentComposite - the composite in which the widget will be placed
   * @param props - PropsUi props for L&amp;F
   * @param transformMeta - transformMeta of the current transform
   * @param formData - FormData to use for placement
   */
  public static ComboVar createFieldDropDown(
      Composite parentComposite,
      PropsUi props,
      IVariables variables,
      BaseTransformMeta transformMeta,
      FormData formData) {
    PipelineMeta pipelineMeta = transformMeta.getParentTransformMeta().getParentPipelineMeta();
    ComboVar fieldDropDownCombo =
        new ComboVar(variables, parentComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(fieldDropDownCombo);
    fieldDropDownCombo.addModifyListener(e -> transformMeta.setChanged());

    fieldDropDownCombo.setLayoutData(formData);
    Listener focusListener =
        e -> {
          String current = fieldDropDownCombo.getText();
          fieldDropDownCombo.getCComboWidget().removeAll();
          fieldDropDownCombo.setText(current);

          try {
            IRowMeta rmi =
                pipelineMeta.getPrevTransformFields(
                    variables, transformMeta.getParentTransformMeta().getName());
            List ls = rmi.getValueMetaList();
            for (Object l : ls) {
              ValueMetaBase vmb = (ValueMetaBase) l;
              fieldDropDownCombo.add(vmb.getName());
            }
          } catch (HopTransformException ex) {
            // can be ignored, since previous transform may not be set yet.
            transformMeta.logDebug(ex.getMessage(), ex);
          }
        };
    fieldDropDownCombo.getCComboWidget().addListener(SWT.FocusIn, focusListener);
    return fieldDropDownCombo;
  }

  /**
   * Creates a FormData object specifying placement below anchorControl, with pixelsBetweeenAnchor
   * variables between anchor and the control.
   */
  public static FormData formDataBelow(Control anchorControl, int width, int pixelsBetweenAnchor) {
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(0, 0);
    fdMessageField.top = new FormAttachment(anchorControl, pixelsBetweenAnchor);
    fdMessageField.right = new FormAttachment(0, width);
    return fdMessageField;
  }

  public static CTabFolder createTabFolder(Composite composite, FormData fd, String... titles) {
    Composite container = new Composite(composite, SWT.NONE);
    WidgetUtils.setFormLayout(container, 0);
    container.setLayoutData(fd);

    CTabFolder tabFolder = new CTabFolder(container, SWT.NONE);
    tabFolder.setLayoutData(new FormDataBuilder().fullSize().result());

    for (String title : titles) {
      if (title.length() < 8) {
        title = StringUtils.rightPad(title, 8);
      }
      Composite tab = new Composite(tabFolder, SWT.NONE);
      WidgetUtils.setFormLayout(tab, ConstUi.MEDIUM_MARGIN);

      CTabItem tabItem = new CTabItem(tabFolder, SWT.NONE);
      tabItem.setFont(GuiResource.getInstance().getFontDefault());
      tabItem.setText(title);
      tabItem.setControl(tab);
    }

    tabFolder.setSelection(0);
    return tabFolder;
  }

  public static FormData firstColumn(Control top) {
    return new FormDataBuilder().top(top, ConstUi.MEDIUM_MARGIN).percentWidth(47).result();
  }

  public static FormData secondColumn(Control top) {
    return new FormDataBuilder().top(top, ConstUi.MEDIUM_MARGIN).right().left(53, 0).result();
  }

  /**
   * Create a flat toolbar for {@link CTabFolder#setTopRight(Control, int)} that stays vertically
   * centered in the tab header.
   *
   * <p>{@code CTabFolder} only centers top-right controls when tab height is left at the default.
   * Hop sets a fixed height ({@code setTabHeight(28)}), and in that mode the folder pins the
   * control to {@code y = 1}. This method listens for that layout and moves the toolbar to the
   * vertical center of the tab strip.
   *
   * @param tabFolder the folder that hosts the toolbar
   * @return the toolbar to add items to; call {@link CTabFolder#setTabHeight(int)} after adding
   *     items so the header is at least as tall as the toolbar
   */
  public static ToolBar createCenteredTopRightToolBar(CTabFolder tabFolder) {
    ToolBar toolBar = new ToolBar(tabFolder, SWT.FLAT);
    PropsUi.setLook(toolBar);
    toolBar.setBackground(tabFolder.getBackground());
    tabFolder.setTopRight(toolBar, SWT.RIGHT);

    Listener align = e -> alignTopRightInTabHeader(tabFolder, toolBar);
    tabFolder.addListener(SWT.Resize, align);
    toolBar.addListener(SWT.Move, align);
    return toolBar;
  }

  private static void alignTopRightInTabHeader(CTabFolder tabFolder, Control control) {
    if (tabFolder.isDisposed() || control.isDisposed()) {
      return;
    }
    if (Boolean.TRUE.equals(control.getData(TOP_RIGHT_ALIGNING))) {
      return;
    }

    Rectangle bounds = control.getBounds();
    if (bounds.width <= 0 || bounds.height <= 0) {
      return;
    }

    int headerY = 0;
    int headerHeight = tabFolder.getTabHeight();
    if (tabFolder.getItemCount() > 0) {
      Rectangle tab = tabFolder.getItem(0).getBounds();
      if (tab.height > 0) {
        headerY = tab.y;
        headerHeight = tab.height;
      }
    }

    // Windows ToolBar preferred height includes empty space below the 16px icons. Center on
    // the icon row so the buttons line up with the tab text instead of sitting on the top edge.
    int iconHeight = (int) Math.round(ConstUi.SMALL_ICON_SIZE * PropsUi.getNativeZoomFactor()) + 4;
    int contentHeight = Math.min(bounds.height, iconHeight);
    int y = headerY + Math.max(0, (headerHeight - contentHeight) / 2);
    if (bounds.y == y) {
      return;
    }

    control.setData(TOP_RIGHT_ALIGNING, Boolean.TRUE);
    try {
      control.setLocation(bounds.x, y);
    } finally {
      control.setData(TOP_RIGHT_ALIGNING, Boolean.FALSE);
    }
  }
}

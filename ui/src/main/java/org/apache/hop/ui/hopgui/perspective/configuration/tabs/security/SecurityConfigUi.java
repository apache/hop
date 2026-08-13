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

package org.apache.hop.ui.hopgui.perspective.configuration.tabs.security;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.perspective.configuration.tabs.ConfigSecurityTab;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/** Shared layout helpers for Security configuration sub-tabs. */
public final class SecurityConfigUi {

  public static final String[] AUTH_MODES = {"NONE", "EXTERNAL", "BASIC", "OAUTH2"};
  public static final String[] HOP_ROLE_IDS = {"admin", "user", "operator", "readonly"};
  public static final String[] YES_NO = {"Y", "N"};

  private static final Class<?> PKG = ConfigSecurityTab.class;

  private SecurityConfigUi() {}

  /**
   * Create a scrolled form composite inside a new tab item.
   *
   * @param wTabFolder parent folder
   * @param titleKey i18n key under ConfigSecurityTab messages
   * @return content composite (form layout, margins applied)
   */
  public static Composite createTabContent(CTabFolder wTabFolder, String titleKey) {
    CTabItem wTab = new CTabItem(wTabFolder, SWT.NONE);
    wTab.setFont(org.apache.hop.ui.core.gui.GuiResource.getInstance().getFontDefault());
    wTab.setText(BaseMessages.getString(PKG, titleKey));

    ScrolledComposite scrolled = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    scrolled.setLayout(new FillLayout());

    Composite content = new Composite(scrolled, SWT.NONE);
    PropsUi.setLook(content);
    FormLayout layout = new FormLayout();
    layout.marginWidth = PropsUi.getFormMargin();
    layout.marginHeight = PropsUi.getFormMargin();
    content.setLayout(layout);

    scrolled.setContent(content);
    scrolled.setExpandHorizontal(true);
    scrolled.setExpandVertical(true);
    wTab.setControl(scrolled);

    content.addListener(
        SWT.Resize,
        e -> {
          if (!content.isDisposed() && !scrolled.isDisposed()) {
            scrolled.setMinSize(content.computeSize(SWT.DEFAULT, SWT.DEFAULT));
          }
        });

    return content;
  }

  public static void finishTabLayout(Composite content) {
    if (content == null || content.isDisposed()) {
      return;
    }
    content.layout(true, true);
    content.pack();
    if (content.getParent() instanceof ScrolledComposite scrolled) {
      scrolled.setMinSize(content.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    }
  }

  public static Label addHint(Composite parent, Control above, String messageKey, int margin) {
    Label hint = new Label(parent, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(hint);
    hint.setText(BaseMessages.getString(PKG, messageKey));
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    if (above == null) {
      fd.top = new FormAttachment(0, 0);
    } else {
      fd.top = new FormAttachment(above, margin);
    }
    hint.setLayoutData(fd);
    return hint;
  }

  /**
   * Label + single-line text field.
   *
   * @return the text field
   */
  public static Text addLabeledText(
      Composite parent, String labelKey, Control above, int margin, int middlePct) {
    Label label = new Label(parent, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(label);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middlePct, 0);
    if (above == null) {
      fdl.top = new FormAttachment(0, 0);
    } else {
      fdl.top = new FormAttachment(above, margin);
    }
    label.setLayoutData(fdl);

    Text text = new Text(parent, SWT.BORDER | SWT.SINGLE);
    PropsUi.setLook(text);
    FormData fdt = new FormData();
    fdt.left = new FormAttachment(middlePct, margin);
    fdt.right = new FormAttachment(100, 0);
    if (above == null) {
      fdt.top = new FormAttachment(0, 0);
    } else {
      fdt.top = new FormAttachment(above, margin);
    }
    text.setLayoutData(fdt);
    return text;
  }
}

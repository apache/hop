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

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dialog to enter a text. (descriptions etc.)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ShowMessageDialog extends Dialog {
  private static final Class<?> PKG = ShowMessageDialog.class; // For Translator

  private static final Map<Integer, String> buttonTextByFlagDefaults = new LinkedHashMap<>();

  static {
    buttonTextByFlagDefaults.put(SWT.OK, BaseMessages.getString(PKG, "System.Button.OK"));
    buttonTextByFlagDefaults.put(SWT.CANCEL, BaseMessages.getString(PKG, "System.Button.Cancel"));
    buttonTextByFlagDefaults.put(SWT.YES, BaseMessages.getString(PKG, "System.Button.Yes"));
    buttonTextByFlagDefaults.put(SWT.NO, BaseMessages.getString(PKG, "System.Button.No"));
  }

  private String title, message;

  private Shell shell;
  private PropsUi props;

  private int flags;
  private final Map<Integer, String> buttonTextByFlag;

  private int returnValue;
  private int type;

  private Shell parent;

  private boolean scroll;
  private boolean hasIcon;

  /** Timeout of dialog in seconds */
  private int timeOut;

  private List<Button> buttons;

  private List<SelectionAdapter> adapters;

  private FormLayout formLayout;
  private FormData fdlDesc;

  private Label wIcon;

  private Text wlDesc;

  /**
   * Dialog to allow someone to show a text with an icon in front
   *
   * @param parent The parent shell to use
   * @param flags the icon to show using SWT flags: SWT.ICON_WARNING, SWT.ICON_ERROR, ... Also
   *     SWT.OK, SWT.CANCEL is allowed.
   * @param title The dialog title
   * @param message The message to display
   */
  public ShowMessageDialog(Shell parent, int flags, String title, String message) {
    this(parent, flags, title, message, false);
  }

  /**
   * Dialog to allow someone to show a text with an icon in front
   *
   * @param parent The parent shell to use
   * @param flags the icon to show using SWT flags: SWT.ICON_WARNING, SWT.ICON_ERROR, ... Also
   *     SWT.OK, SWT.CANCEL is allowed.
   * @param title The dialog title
   * @param message The message to display
   * @param scroll Set the dialog to a default size and enable scrolling
   */
  public ShowMessageDialog(Shell parent, int flags, String title, String message, boolean scroll) {
    this(parent, flags, buttonTextByFlagDefaults, title, message, scroll);
  }

  /**
   * Dialog to allow someone to show a text with an icon in front
   *
   * @param parent The parent shell to use
   * @param flags the icon to show using SWT flags: SWT.ICON_WARNING, SWT.ICON_ERROR, ... Also
   *     SWT.OK, SWT.CANCEL is allowed.
   * @param buttonTextByFlag Custom text to display for each button by flag i.e. key: SWT.OK, value:
   *     "Custom OK" Note - controls button order, use an ordered map to maintain button order.
   * @param title The dialog title
   * @param message The message to display
   * @param scroll Set the dialog to a default size and enable scrolling
   */
  public ShowMessageDialog(
      Shell parent,
      int flags,
      Map<Integer, String> buttonTextByFlag,
      String title,
      String message,
      boolean scroll) {
    super(parent, SWT.NONE);
    this.buttonTextByFlag = buttonTextByFlag;
    this.parent = parent;
    this.flags = flags;
    this.title = title;
    this.message = message;
    this.scroll = scroll;

    props = PropsUi.getInstance();
  }

  public int open() {
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE);

    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    formLayout = new FormLayout();
    shell.setLayout(formLayout);

    shell.setText(title);

    hasIcon =
        (flags & SWT.ICON_WARNING) != 0
            || (flags & SWT.ICON_INFORMATION) != 0
            || (flags & SWT.ICON_QUESTION) != 0
            || (flags & SWT.ICON_ERROR) != 0
            || (flags & SWT.ICON_WORKING) != 0;

    Image image = null;
    if ((flags & SWT.ICON_WARNING) != 0) {
      image = display.getSystemImage(SWT.ICON_WARNING);
    }
    if ((flags & SWT.ICON_INFORMATION) != 0) {
      image = display.getSystemImage(SWT.ICON_INFORMATION);
    }
    if ((flags & SWT.ICON_QUESTION) != 0) {
      image = display.getSystemImage(SWT.ICON_QUESTION);
    }
    if ((flags & SWT.ICON_ERROR) != 0) {
      image = display.getSystemImage(SWT.ICON_ERROR);
    }
    if ((flags & SWT.ICON_WORKING) != 0) {
      image = display.getSystemImage(SWT.ICON_WORKING);
    }

    hasIcon = hasIcon && image != null;
    wIcon = null;

    if (hasIcon) {
      wIcon = new Label(shell, SWT.NONE);
      props.setLook(wIcon);
      wIcon.setImage(image);
      FormData fdIcon = new FormData();
      fdIcon.left = new FormAttachment(0, 0);
      fdIcon.top = new FormAttachment(0, 0);
      fdIcon.right = new FormAttachment(0, image.getBounds().width);
      fdIcon.bottom = new FormAttachment(0, image.getBounds().height);
      wIcon.setLayoutData(fdIcon);
    }

    // The message
    fdlDesc = new FormData();

    if (scroll) {
      wlDesc = new Text(shell, SWT.MULTI | SWT.READ_ONLY | SWT.V_SCROLL | SWT.H_SCROLL);
      shell.setSize(550, 350);
      fdlDesc.bottom = new FormAttachment(100, -70);
      fdlDesc.right = new FormAttachment(100, 0);
    } else {
      wlDesc = new Text(shell, SWT.MULTI | SWT.READ_ONLY);
      fdlDesc.right = new FormAttachment(100, 0);
    }

    wlDesc.setText(message);
    props.setLook(wlDesc);

    wlDesc.setLayoutData(fdlDesc);

    buttons = new ArrayList<>();
    adapters = new ArrayList<>();

    for (Map.Entry<Integer, String> entry : buttonTextByFlag.entrySet()) {
      Integer buttonFlag = entry.getKey();
      if ((flags & buttonFlag) != 0) {
        Button button = new Button(shell, SWT.PUSH);
        button.setText(entry.getValue());
        SelectionAdapter selectionAdapter =
            new SelectionAdapter() {
              public void widgetSelected(SelectionEvent event) {
                quit(buttonFlag);
              }
            };
        button.addSelectionListener(selectionAdapter);
        adapters.add(selectionAdapter);
        buttons.add(button);
      }
    }

    setLayoutAccordingToType();

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    if (!scroll) {
      BaseTransformDialog.setSize(shell);
    }

    final Button button = buttons.get(0);
    final SelectionAdapter selectionAdapter = adapters.get(0);
    final String ok = button.getText();
    long startTime = new Date().getTime();

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();

        if (timeOut > 0) {
          long time = new Date().getTime();
          long diff = (time - startTime) / 1000;
          button.setText(ok + " (" + (timeOut - diff) + ")");

          if (diff >= timeOut) {
            selectionAdapter.widgetSelected(null);
          }
        }
      }
    }
    return returnValue;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void cancel() {
    if ((flags & SWT.NO) > 0) {
      quit(SWT.NO);
    } else {
      quit(SWT.CANCEL);
    }
  }

  private void quit(int returnValue) {
    this.returnValue = returnValue;
    dispose();
  }

  /** Handles any variances in the UI from the default. */
  private void setLayoutAccordingToType() {
    int margin = props.getMargin();
    switch (type) {
      case Const.SHOW_MESSAGE_DIALOG_DB_TEST_SUCCESS:
        formLayout.marginWidth = 15;
        formLayout.marginHeight = 15;
        setFdlDesc(margin * 3, 0, 0, margin);
        BaseTransformDialog.positionBottomButtons(
            shell,
            buttons.toArray(new Button[buttons.size()]),
            0,
            BaseTransformDialog.BUTTON_ALIGNMENT_RIGHT,
            wlDesc);
        break;
      default:
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;
        setFdlDesc(margin * 2, margin, 0, margin);
        BaseTransformDialog.positionBottomButtons(
            shell, buttons.toArray(new Button[buttons.size()]), margin, wlDesc);
        break;
    }
  }

  private void setFdlDesc(
      int leftOffsetHasIcon, int topOffsetHasIcon, int leftOffsetNoIcon, int topOffsetNoIcon) {
    if (hasIcon) {
      fdlDesc.left = new FormAttachment(wIcon, leftOffsetHasIcon);
      fdlDesc.top = new FormAttachment(0, topOffsetHasIcon);
    } else {
      fdlDesc.left = new FormAttachment(0, leftOffsetNoIcon);
      fdlDesc.top = new FormAttachment(0, topOffsetNoIcon);
    }
  }

  /** @return the timeOut */
  public int getTimeOut() {
    return timeOut;
  }

  /** @param timeOut the timeOut to set */
  public void setTimeOut(int timeOut) {
    this.timeOut = timeOut;
  }

  public void setType(int type) {
    this.type = type;
  }
}

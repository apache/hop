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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectorySelectedExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** A base dialog class containing a body and a configurable button panel. */
public abstract class BaseDialog extends Dialog {
  private static final Class<?> PKG = BaseDialog.class; // For Translator

  public static final int MARGIN_SIZE = 15;
  public static final int LABEL_SPACING = 5;
  public static final int ELEMENT_SPACING = 10;
  public static final int MEDIUM_FIELD = 250;
  public static final int MEDIUM_SMALL_FIELD = 150;
  public static final int SMALL_FIELD = 50;
  public static final int SHELL_WIDTH_OFFSET = 16;
  public static final int VAR_ICON_WIDTH =
      GuiResource.getInstance().getImageVariable().getBounds().width;
  public static final int VAR_ICON_HEIGHT =
      GuiResource.getInstance().getImageVariable().getBounds().height;

  protected Map<String, Listener> buttons = new HashMap();

  protected Shell shell;

  protected PropsUi props;
  protected int width = -1;
  protected String title;

  private int footerTopPadding = BaseDialog.ELEMENT_SPACING * 4;

  public BaseDialog(final Shell shell) {
    this(shell, null, -1);
  }

  public BaseDialog(final Shell shell, final String title, final int width) {
    super(shell, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.title = title;
    this.width = width;
  }

  public static final String presentFileDialog(
      Shell shell, String[] filterExtensions, String[] filterNames, boolean folderAndFile) {
    return presentFileDialog(
        false, shell, null, null, null, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      boolean save,
      Shell shell,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        save, shell, null, null, null, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      Shell shell,
      TextVar textVar,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        false, shell, textVar, null, fileObject, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      boolean save,
      Shell shell,
      TextVar textVar,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        save, shell, textVar, null, fileObject, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      Shell shell,
      TextVar textVar,
      IVariables variables,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        false, shell, textVar, variables, null, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      boolean save,
      Shell shell,
      TextVar textVar,
      IVariables variables,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        save, shell, textVar, variables, null, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      Shell shell,
      TextVar textVar,
      IVariables variables,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        false, shell, textVar, variables, fileObject, filterExtensions, filterNames, folderAndFile);
  }

  public static final String presentFileDialog(
      boolean save,
      Shell shell,
      TextVar textVar,
      IVariables variables,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {

    boolean useNativeFileDialog =
        "Y"
            .equalsIgnoreCase(
                HopGui.getInstance().getVariables().getVariable("HOP_USE_NATIVE_FILE_DIALOG", "N"));

    IFileDialog dialog;

    if (useNativeFileDialog) {
      FileDialog fileDialog = new FileDialog(shell, save ? SWT.SAVE : SWT.OPEN);
      dialog = new NativeFileDialog(fileDialog);
    } else {
      HopVfsFileDialog vfsDialog = new HopVfsFileDialog(shell, variables, fileObject, false, save);
      if (save) {
        if (fileObject != null) {
          vfsDialog.setSaveFilename(fileObject.getName().getBaseName());
          try {
            vfsDialog.setFilterPath(HopVfs.getFilename(fileObject.getParent()));
          } catch (FileSystemException fse) {
            // This wasn't a valid filename, ignore the error to reduce spamming
          }
        } else {
          // Take the first extension with "filename" prepended
          //
          if (filterExtensions != null && filterExtensions.length > 0) {
            String filterExtension = filterExtensions[0];
            String extension = filterExtension.substring(filterExtension.lastIndexOf("."));
            vfsDialog.setSaveFilename("filename" + extension);
          }
        }
      }
      dialog = vfsDialog;
    }

    if (save) {
      dialog.setText(BaseMessages.getString(PKG, "BaseDialog.SaveFile"));
    } else {
      dialog.setText(BaseMessages.getString(PKG, "BaseDialog.OpenFile"));
    }
    if (filterExtensions == null || filterNames == null) {
      dialog.setFilterExtensions(new String[] {"*.*"});
      dialog.setFilterNames(new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")});
    } else {
      dialog.setFilterExtensions(filterExtensions);
      dialog.setFilterNames(filterNames);
    }
    if (fileObject != null) {
      dialog.setFileName(HopVfs.getFilename(fileObject));
    }
    if (variables != null && textVar != null && textVar.getText() != null) {
      dialog.setFileName(variables.resolve(textVar.getText()));
    }

    AtomicBoolean doIt = new AtomicBoolean(true);
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.UI,
          variables,
          HopGuiExtensionPoint.HopGuiFileOpenDialog.id,
          new HopGuiFileDialogExtension(doIt, dialog));
    } catch (Exception xe) {
      LogChannel.UI.logError("Error handling extension point 'HopGuiFileOpenDialog'", xe);
    }

    String filename = null;
    if (!doIt.get() || dialog.open() != null) {
      if (folderAndFile) {
        filename = FilenameUtils.concat(dialog.getFilterPath(), dialog.getFileName());
      } else {
        filename = dialog.getFileName();
      }

      try {
        HopGuiFileOpenedExtension openedExtension =
            new HopGuiFileOpenedExtension(dialog, variables, filename);
        ExtensionPointHandler.callExtensionPoint(
            LogChannel.UI,
            variables,
            HopGuiExtensionPoint.HopGuiFileOpenedDialog.id,
            openedExtension);
        if (openedExtension.filename != null) {
          filename = openedExtension.filename;
        }
      } catch (Exception xe) {
        LogChannel.UI.logError("Error handling extension point 'HopGuiFileOpenDialog'", xe);
      }

      if (textVar != null) {
        textVar.setText(filename);
      }
    }
    return filename;
  }

  public static String presentDirectoryDialog(Shell shell, IVariables variables) {
    return presentDirectoryDialog(shell, null, null);
  }

  public static String presentDirectoryDialog(Shell shell, TextVar textVar, IVariables variables) {
    return presentDirectoryDialog(shell, textVar, null, variables);
  }

  public static String presentDirectoryDialog(
      Shell shell, TextVar textVar, String message, IVariables variables) {

    boolean useNativeFileDialog =
        "Y"
            .equalsIgnoreCase(
                HopGui.getInstance().getVariables().getVariable("HOP_USE_NATIVE_FILE_DIALOG", "N"));

    IDirectoryDialog directoryDialog;
    if (useNativeFileDialog) {
      directoryDialog = new NativeDirectoryDialog(new DirectoryDialog(shell, SWT.OPEN));
    } else {
      directoryDialog = new HopVfsFileDialog(shell, variables, null, true, false);
    }

    if (StringUtils.isNotEmpty(message)) {
      directoryDialog.setMessage(message);
    }
    directoryDialog.setText(BaseMessages.getString(PKG, "BaseDialog.OpenDirectory"));
    if (textVar != null && variables != null && textVar.getText() != null) {
      directoryDialog.setFilterPath(variables.resolve(textVar.getText()));
    }
    String directoryName = null;

    AtomicBoolean doIt = new AtomicBoolean(true);
    try {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.UI,
          variables,
          HopGuiExtensionPoint.HopGuiFileDirectoryDialog.id,
          new HopGuiDirectoryDialogExtension(doIt, directoryDialog));
    } catch (Exception xe) {
      LogChannel.UI.logError("Error handling extension point 'HopGuiFileDirectoryDialog'", xe);
    }

    if (!doIt.get() || directoryDialog.open() != null) {
      directoryName = directoryDialog.getFilterPath();
      try {
        HopGuiDirectorySelectedExtension ext =
            new HopGuiDirectorySelectedExtension(directoryDialog, variables, directoryName);
        ExtensionPointHandler.callExtensionPoint(
            LogChannel.UI, variables, HopGuiExtensionPoint.HopGuiDirectorySelected.id, ext);
        if (ext.folderName != null) {
          directoryName = ext.folderName;
        }
      } catch (Exception xe) {
        LogChannel.UI.logError("Error handling extension point 'HopGuiDirectorySelected'", xe);
      }

      // Set the text box to the new selection
      if (textVar != null && directoryName != null) {
        textVar.setText(directoryName);
      }
    }

    return directoryName;
  }

  /**
   * Returns a {@link org.eclipse.swt.events.SelectionAdapter} that is used to "submit" the dialog.
   */
  private Display prepareLayout() {

    // Prep the parent shell and the dialog shell
    final Shell parent = getParent();
    final Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.APPLICATION_MODAL | SWT.SHEET);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    props.setLook(shell);
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            dispose();
          }
        });

    final FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = MARGIN_SIZE;
    formLayout.marginHeight = MARGIN_SIZE;

    shell.setLayout(formLayout);
    shell.setText(this.title);
    return display;
  }

  /**
   * Returns the last element in the body - the one to which the buttons should be attached.
   *
   * @return
   */
  protected abstract Control buildBody();

  public int open() {
    final Display display = prepareLayout();

    final Control lastBodyElement = buildBody();
    buildFooter(lastBodyElement);

    open(display);

    return 1;
  }

  private void open(final Display display) {
    shell.pack();
    if (width > 0) {
      final int height = shell.computeSize(width, SWT.DEFAULT).y;
      // for some reason the actual width and minimum width are smaller than what is requested - add
      // the
      // SHELL_WIDTH_OFFSET to get the desired size
      shell.setMinimumSize(width + SHELL_WIDTH_OFFSET, height);
      shell.setSize(width + SHELL_WIDTH_OFFSET, height);
    }

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  protected void buildFooter(final Control anchorElement) {

    final Button[] buttonArr = new Button[buttons == null ? 0 : buttons.size()];
    int index = 0;
    if (buttons != null) {
      for (final String buttonName : buttons.keySet()) {
        final Button button = new Button(shell, SWT.PUSH);
        button.setText(buttonName);
        final Listener listener = buttons.get(buttonName);
        if (listener != null) {
          button.addListener(SWT.Selection, listener);
        } else {
          // fall back on simply closing the dialog
          button.addListener(
              SWT.Selection,
              event -> {
                dispose();
              });
        }
        buttonArr[index++] = button;
      }
    }

    // traverse the buttons backwards to position them to the right
    Button previousButton = null;
    for (int i = buttonArr.length - 1; i >= 0; i--) {
      final Button button = buttonArr[i];
      if (previousButton == null) {
        button.setLayoutData(
            new FormDataBuilder().top(anchorElement, footerTopPadding).right(100, 0).result());
      } else {
        button.setLayoutData(
            new FormDataBuilder()
                .top(anchorElement, footerTopPadding)
                .right(previousButton, Const.isOSX() ? 0 : -BaseDialog.LABEL_SPACING)
                .result());
      }
      previousButton = button;
    }
  }

  public void setFooterTopPadding(final int footerTopPadding) {
    this.footerTopPadding = footerTopPadding;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void setButtons(final Map<String, Listener> buttons) {
    this.buttons = buttons;
  }
}

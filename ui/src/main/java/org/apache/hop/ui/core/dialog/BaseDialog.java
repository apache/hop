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

package org.apache.hop.ui.core.dialog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Setter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variable;
import org.apache.hop.core.variables.VariableScope;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectorySelectedExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** A base dialog class containing a body and a configurable button panel. */
public abstract class BaseDialog extends Dialog {
  private static final Class<?> PKG = BaseDialog.class;

  public static final String NO_DEFAULT_HANDLER = "NoDefaultHandler";

  @Variable(
      scope = VariableScope.APPLICATION,
      value = "N",
      description =
          "Set this value to 'Y' if you want to use the system file open/save dialog when browsing files.")
  public static final String HOP_USE_NATIVE_FILE_DIALOG = "HOP_USE_NATIVE_FILE_DIALOG";

  public static final int MARGIN_SIZE = 15;
  public static final int LABEL_SPACING = 5;
  public static final int ELEMENT_SPACING = 10;
  public static final int MEDIUM_FIELD = 250;
  public static final int MEDIUM_SMALL_FIELD = 150;
  public static final int SMALL_FIELD = 50;
  public static final int SHELL_WIDTH_OFFSET = 16;

  /**
   * @deprecated
   */
  @Deprecated(since = "2.10")
  public static final int VAR_ICON_WIDTH =
      GuiResource.getInstance().getImageVariableMini().getBounds().width;

  /**
   * @deprecated
   */
  @Deprecated(since = "2.10")
  public static final int VAR_ICON_HEIGHT =
      GuiResource.getInstance().getImageVariableMini().getBounds().height;

  @Setter protected Map<String, Listener> buttons = new HashMap<>();

  protected Shell shell;

  protected PropsUi props;
  protected int width;
  protected String baseDialogTitle;

  @Setter private int footerTopPadding = BaseDialog.ELEMENT_SPACING * 4;

  protected BaseDialog(final Shell shell) {
    this(shell, null, -1);
  }

  protected BaseDialog(final Shell shell, final String baseDialogTitle, final int width) {
    super(shell, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.baseDialogTitle = baseDialogTitle;
    this.width = width;
  }

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param shell the Shell to attach the dialog to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
      Shell shell, String[] filterExtensions, String[] filterNames, boolean folderAndFile) {
    return presentFileDialog(
        false, shell, null, null, null, filterExtensions, filterNames, folderAndFile);
  }

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param save boolean to indicate if it's save or open dialog
   * @param shell the Shell to attach the dialog to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
      boolean save,
      Shell shell,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        save, shell, null, null, null, filterExtensions, filterNames, folderAndFile);
  }

  /**
   * @deprecated use
   *     <p>Open a File browser dialog containing bookmarks. This dialog can be used for both
   *     opening and saving files When Saving it wel prepend filename.extension where extension is
   *     the first extension provided in the filterExtensions.
   * @param shell the Shell to attach the dialog to
   * @param textVar the textVar component that will contain the filename + path
   * @param fileObject the FileObject to navigate to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  @Deprecated(since = "2.13")
  public static String presentFileDialog(
      Shell shell,
      TextVar textVar,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        false, shell, textVar, null, fileObject, filterExtensions, filterNames, folderAndFile);
  }

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param save boolean to indicate if it's save or open dialog
   * @param shell the Shell to attach the dialog to
   * @param textVar the textVar component that will contain the filename + path
   * @param fileObject the FileObject to navigate to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
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

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param shell the Shell to attach the dialog to
   * @param textVar the textVar component that will contain the filename + path
   * @param variables IVariables to resolve variables in the dialog
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
      Shell shell,
      TextVar textVar,
      IVariables variables,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        false, shell, textVar, variables, null, filterExtensions, filterNames, folderAndFile);
  }

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param save boolean to indicate if it's save or open dialog
   * @param shell the Shell to attach the dialog to
   * @param textVar the textVar component that will contain the filename + path
   * @param variables IVariables to resolve variables in the dialog
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
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

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param shell the Shell to attach the dialog to
   * @param textVar the textVar component that will contain the filename + path
   * @param variables IVariables to resolve variables in the dialog
   * @param fileObject the FileObject to navigate to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
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

  /**
   * Open a File browser dialog containing bookmarks. This dialog can be used for both opening and
   * saving files When Saving it wel prepend filename.extension where extension is the first
   * extension provided in the filterExtensions.
   *
   * @param save boolean to indicate if it's save or open dialog
   * @param shell the Shell to attach the dialog to
   * @param textVar the textVar component that will contain the filename + path
   * @param variables IVariables to resolve variables in the dialog
   * @param fileObject the FileObject to navigate to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return filepath of the selected file or folder
   */
  public static String presentFileDialog(
      boolean save,
      Shell shell,
      TextVar textVar,
      IVariables variables,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {

    boolean useNativeFileDialog =
        HopGui.getInstance().getVariables().getVariableBoolean(HOP_USE_NATIVE_FILE_DIALOG, false);

    IFileDialog dialog;

    if (useNativeFileDialog) {
      FileDialog fileDialog = new FileDialog(shell, save ? SWT.SAVE : SWT.OPEN);
      dialog = new NativeFileDialog(fileDialog);
    } else {
      HopVfsFileDialog vfsDialog =
          new HopVfsFileDialog(shell, variables, fileObject, false, save, folderAndFile);
      if (save) {
        // check if textVar contains a valid path
        if (textVar != null && !textVar.getText().isEmpty()) {
          try {
            fileObject = HopVfs.getFileObject(variables.resolve(textVar.getText()));
            if (!fileObject.exists() && fileObject.getParent().exists()) {
              fileObject = fileObject.getParent();
            } else if (!fileObject.exists()) {
              fileObject = null;
            }

            if (fileObject != null && fileObject.isFile()) {
              vfsDialog.setSaveFilename(fileObject.getName().getBaseName());
              vfsDialog.setFilterPath(HopVfs.getFilename(fileObject));
            } else {

              // Take the first extension with "filename" prepended
              //
              if (filterExtensions != null && filterExtensions.length > 0) {
                String filterExtension = filterExtensions[0];
                String extension = filterExtension.substring(filterExtension.lastIndexOf("."));
                vfsDialog.setSaveFilename("filename" + extension);
              }
            }

          } catch (Exception e) {
            fileObject = null;
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
    if (filterExtensions == null
        || filterNames == null
        || filterExtensions.length == 0
        || filterNames.length == 0) {
      dialog.setFilterExtensions(new String[] {"*.*"});
      dialog.setFilterNames(new String[] {BaseMessages.getString(PKG, "System.FileType.AllFiles")});
    } else {
      dialog.setFilterExtensions(filterExtensions);
      dialog.setFilterNames(filterNames);
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

    if (fileObject != null) {
      dialog.setFileName(HopVfs.getFilename(fileObject));
      try {
        if (fileObject.isFile()) {
          dialog.setFilterPath(HopVfs.getFilename(fileObject.getParent()));
        } else {
          dialog.setFilterPath(HopVfs.getFilename(fileObject));
        }

      } catch (FileSystemException fse) {
        // This wasn't a valid filename, ignore the error to reduce spamming
      }
    }
    if (variables != null && textVar != null && textVar.getText() != null) {
      dialog.setFileName(variables.resolve(textVar.getText()));
    }

    String filename = null;
    if (!doIt.get() || dialog.open() != null) {
      filename = buildFilename(dialog.getFilterPath(), dialog.getFileName());
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

  private static String buildFilename(String filterPath, String fileName) {
    if (StringUtils.isEmpty(filterPath)) {
      return fileName;
    }
    // Is this reading from a VFS URL?
    //
    if (filterPath.contains("://") || filterPath.contains(":///")) {
      if (filterPath.endsWith("/")) {
        return filterPath + fileName;
      } else {
        return filterPath + "/" + fileName;
      }
    } else {
      return FilenameUtils.concat(filterPath, fileName);
    }
  }

  public static String presentDirectoryDialog(Shell shell) {
    return presentDirectoryDialog(shell, null, null);
  }

  public static String presentDirectoryDialog(Shell shell, TextVar textVar, IVariables variables) {
    return presentDirectoryDialog(shell, textVar, null, variables);
  }

  public static String presentDirectoryDialog(
      Shell shell, TextVar textVar, String message, IVariables variables) {
    String path = null;
    if (textVar != null && textVar.getText() != null) {
      path = textVar.getText();
    }

    String directory = presentDirectoryDialog(shell, path, message, variables);

    // Set the text box to the new selection
    if (textVar != null && directory != null) {
      textVar.setText(directory);
    }

    return directory;
  }

  public static String presentDirectoryDialog(
      Shell shell, String path, String message, IVariables variables) {

    boolean useNativeFileDialog =
        "Y"
            .equalsIgnoreCase(
                HopGui.getInstance().getVariables().getVariable(HOP_USE_NATIVE_FILE_DIALOG, "N"));

    IDirectoryDialog directoryDialog;
    if (useNativeFileDialog) {
      directoryDialog = new NativeDirectoryDialog(new DirectoryDialog(shell, SWT.OPEN));
    } else {
      directoryDialog = new HopVfsFileDialog(shell, variables, null, true, false, true);
    }

    if (StringUtils.isNotEmpty(message)) {
      directoryDialog.setMessage(message);
    }
    directoryDialog.setText(BaseMessages.getString(PKG, "BaseDialog.OpenDirectory"));
    if (variables != null && path != null) {
      directoryDialog.setFilterPath(variables.resolve(path));
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
    PropsUi.setLook(shell);
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
    shell.setText(this.baseDialogTitle);
    return display;
  }

  /**
   * Returns the last element in the body - the one to which the buttons should be attached.
   *
   * @return Returns the last element in the body
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
          button.addListener(SWT.Selection, event -> dispose());
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

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /**
   * Handle the shell specified until the OK (button) is consumed. Set a default icon on the shell,
   * add default selection handlers on fields. Set the appropriate size for the shell. If you have
   * widgets on which you don't want to have this default selection handler to okConsumer, do:
   *
   * <p>widget.setData(NO_DEFAULT_HANDLER, true)
   *
   * @param shell The shell to handle.
   * @param okConsumer What to do when the dialog information needs to be retained after closing.
   * @param cancelConsumer What to do when the dialog is cancelled.
   */
  public static void defaultShellHandling(
      Shell shell, Consumer<Void> okConsumer, Consumer<Void> cancelConsumer) {
    defaultShellHandling(
        shell,
        okConsumer,
        () -> {
          cancelConsumer.accept(null);
          return true;
        });
  }

  public static void defaultShellHandling(
      Shell shell, Consumer<Void> okConsumer, Supplier<Boolean> cancelSupplier) {

    // If the shell is closed, cancel the dialog
    //
    shell.addListener(SWT.Close, e -> e.doit = cancelSupplier.get());

    // Check for enter being pressed in text input fields
    //
    addDefaultListeners(shell, okConsumer);

    // Add spaces on tab items to make them more manageable
    //
    addSpacesOnTabs(shell);

    // Set the size as well...
    //
    BaseTransformDialog.setSize(shell);

    // Open the shell
    //
    shell.open();

    // Handle the event loop until we're done with this shell...
    //
    Display display = shell.getDisplay();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  public static void addSpacesOnTabs(Composite composite) {
    if (composite == null || composite.isDisposed()) {
      return;
    }

    for (Control control : composite.getChildren()) {
      // Some of these are composites, so check first
      //
      if (control instanceof CTabFolder cTabFolder) {
        for (CTabItem item : cTabFolder.getItems()) {
          if (item.getText() != null) {
            item.setText("  " + item.getText() + "  ");
          }
        }
      }
    }
  }

  public static void addDefaultListeners(Composite composite, Consumer<Void> okConsumer) {
    if (composite == null || composite.isDisposed()) {
      return;
    }

    for (Control control : composite.getChildren()) {
      if (control.getData(NO_DEFAULT_HANDLER) != null) {
        continue;
      }
      // Some of these are composites so check first
      //
      if ((control instanceof Text)
          || (control instanceof Combo)
          || (control instanceof CCombo)
          || (control instanceof TextVar)
          || (control instanceof ComboVar)
          || (control instanceof List)) {
        control.addListener(SWT.DefaultSelection, e -> okConsumer.accept(null));
      } else if (control instanceof Composite composite1) {
        // Check all children
        //
        addDefaultListeners(composite1, okConsumer);
      }
    }
  }

  public static int openMessageBox(Shell parent, String title, String message, int flags) {
    MessageBox box = new MessageBox(parent, flags);
    box.setText(title);
    box.setMessage(message);
    return box.open();
  }
}

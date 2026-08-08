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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Setter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopDialogEditGuard;
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
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectorySelectedExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
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
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;

/** A base dialog class containing a body and a configurable button panel. */
public abstract class BaseDialog extends Dialog {
  private static final Class<?> PKG = BaseDialog.class;

  public static final String NO_DEFAULT_HANDLER = "NoDefaultHandler";

  /**
   * Shell data key for the object being edited (transform meta, action, metadata, …). When the
   * value implements {@link org.apache.hop.core.security.IDialogEditable}, {@link
   * #defaultShellHandling} makes the dialog read-only if the current user lacks the required edit
   * permission.
   */
  public static final String DIALOG_SUBJECT = "hop.dialog.subject";

  /**
   * Widget data key: when set to a non-null value, the control stays enabled in read-only mode
   * (typically Cancel / Close / Help).
   */
  public static final String DIALOG_KEEP_ENABLED = "hop.dialog.keepEnabled";

  /**
   * Stack of dialog subjects for open() call sites that do not set {@link #DIALOG_SUBJECT} on the
   * shell (legacy dialogs). Nested dialogs push/pop so sub-dialogs inherit the parent edit
   * permission correctly.
   */
  private static final ThreadLocal<Deque<Object>> DIALOG_SUBJECT_STACK =
      ThreadLocal.withInitial(ArrayDeque::new);

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

  /**
   * Gets the appropriate shell style flags for dialogs, taking into account the environment and
   * platform.
   *
   * <p>Returns different styles based on the runtime environment:
   *
   * <ul>
   *   <li><b>Hop Web (RAP):</b> Returns {@code SWT.DIALOG_TRIM | SWT.RESIZE} without SWT.MAX and
   *       SWT.MIN, as minimize/maximize buttons cause issues in web environments
   *   <li><b>macOS:</b> Returns {@code SWT.APPLICATION_MODAL | SWT.CLOSE | SWT.TITLE | SWT.RESIZE}
   *       to support multi-monitor setups. Using {@code SWT.DIALOG_TRIM} on macOS with child shells
   *       (created with a parent) causes dialogs to disappear or become inaccessible when moved to
   *       another display. The {@code APPLICATION_MODAL} flag combined with these styles provides
   *       proper multi-monitor support while maintaining dialog behavior.
   *   <li><b>Other Desktop Platforms:</b> Returns {@code SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX |
   *       SWT.MIN} with full window controls including minimize and maximize buttons
   * </ul>
   *
   * <p><b>Important macOS Multi-Monitor Note:</b> On macOS, child shells (created with {@code new
   * Shell(parent, style)}) are bound to the parent window's display by the macOS window system.
   * When dialogs need to work across multiple monitors, they should either:
   *
   * <ul>
   *   <li>Use {@code APPLICATION_MODAL} style (as done here) to detach from parent coordinate space
   *   <li>Or be created with {@code new Shell(display, style)} instead of {@code new Shell(parent,
   *       style)} to create independent windows
   * </ul>
   *
   * <p>Note: {@code SWT.DIALOG_TRIM} includes {@code SWT.TITLE}, {@code SWT.CLOSE}, and {@code
   * SWT.BORDER} by default.
   *
   * @return The shell style flags appropriate for the current environment and platform
   */
  public static int getDefaultDialogStyle() {
    if (EnvironmentUtils.getInstance().isWeb()) {
      // For Hop Web, use simpler style without min/max buttons
      return SWT.DIALOG_TRIM | SWT.RESIZE;
    } else if (OsHelper.isMac()) {
      return SWT.PRIMARY_MODAL | SWT.CLOSE | SWT.TITLE | SWT.RESIZE;
    } else {
      // For other desktop platforms, include min/max buttons
      return SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN;
    }
  }

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
    String[] filenames =
        presentFileDialog(
            save,
            shell,
            textVar,
            variables,
            fileObject,
            filterExtensions,
            filterNames,
            folderAndFile,
            false);
    return filenames.length == 0 ? null : filenames[0];
  }

  /**
   * Open a File browser dialog in which the user can select more than one file. Selecting a single
   * file simply results in an array with one element.
   *
   * @param shell the Shell to attach the dialog to
   * @param variables IVariables to resolve variables in the dialog
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return the full paths of the selected files, empty when the dialog was cancelled
   */
  public static String[] presentMultiFileDialog(
      Shell shell,
      IVariables variables,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentMultiFileDialog(
        shell, variables, null, filterExtensions, filterNames, folderAndFile);
  }

  /**
   * Open a File browser dialog in which the user can select more than one file. Selecting a single
   * file simply results in an array with one element.
   *
   * @param shell the Shell to attach the dialog to
   * @param variables IVariables to resolve variables in the dialog
   * @param fileObject the FileObject to navigate to
   * @param filterExtensions String[] containing a list of extensions to filter on
   * @param filterNames String[] names for the filterExtensions
   * @param folderAndFile boolean to enable the dialog to open both files and folders
   * @return the full paths of the selected files, empty when the dialog was cancelled
   */
  public static String[] presentMultiFileDialog(
      Shell shell,
      IVariables variables,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile) {
    return presentFileDialog(
        false,
        shell,
        null,
        variables,
        fileObject,
        filterExtensions,
        filterNames,
        folderAndFile,
        true);
  }

  private static String[] presentFileDialog(
      boolean save,
      Shell shell,
      TextVar textVar,
      IVariables variables,
      FileObject fileObject,
      String[] filterExtensions,
      String[] filterNames,
      boolean folderAndFile,
      boolean multiSelection) {

    boolean useNativeFileDialog =
        HopGui.getInstance().getVariables().getVariableBoolean(HOP_USE_NATIVE_FILE_DIALOG, false);

    IFileDialog dialog;

    if (useNativeFileDialog) {
      // The native dialog only accepts multiple files when it is created with SWT.MULTI.
      //
      int style = save ? SWT.SAVE : SWT.OPEN;
      if (multiSelection) {
        style |= SWT.MULTI;
      }
      FileDialog fileDialog = new FileDialog(shell, style);
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
          // If fileObject is provided, try to extract the filename from it
          //
          try {
            if (fileObject != null) {
              String baseName = fileObject.getName().getBaseName();
              // Check if this looks like a file (has content and possibly an extension)
              // rather than a folder. Even non-existent files should have a basename.
              if (StringUtils.isNotEmpty(baseName)) {
                vfsDialog.setSaveFilename(baseName);
              } else {
                // Take the first extension with "filename" prepended
                //
                if (filterExtensions != null && filterExtensions.length > 0) {
                  String filterExtension = filterExtensions[0];
                  String extension = filterExtension.substring(filterExtension.lastIndexOf("."));
                  vfsDialog.setSaveFilename("filename" + extension);
                }
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
          } catch (Exception e) {
            // If there's an error checking fileObject, fall back to default
            if (filterExtensions != null && filterExtensions.length > 0) {
              String filterExtension = filterExtensions[0];
              String extension = filterExtension.substring(filterExtension.lastIndexOf("."));
              vfsDialog.setSaveFilename("filename" + extension);
            }
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
    dialog.setMultiSelection(multiSelection);

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
      try {
        if (save) {
          // For save dialogs, we've already set saveFilename above
          // Just set the filter path to the parent directory
          if (fileObject.isFile()) {
            dialog.setFilterPath(HopVfs.getFilename(fileObject.getParent()));
          } else {
            // If it doesn't exist or is a folder, try to get the parent
            FileObject parent = fileObject.getParent();
            if (parent != null && parent.exists()) {
              dialog.setFilterPath(HopVfs.getFilename(parent));
            } else {
              // Fall back to the fileObject itself
              dialog.setFilterPath(HopVfs.getFilename(fileObject));
            }
          }
        } else {
          // For open dialogs, set fileName to navigate to that location
          dialog.setFileName(HopVfs.getFilename(fileObject));
          if (fileObject.isFile()) {
            dialog.setFilterPath(HopVfs.getFilename(fileObject.getParent()));
          } else {
            dialog.setFilterPath(HopVfs.getFilename(fileObject));
          }
        }
      } catch (FileSystemException fse) {
        // This wasn't a valid filename, ignore the error to reduce spamming
      }
    }
    if (variables != null && textVar != null && textVar.getText() != null) {
      dialog.setFileName(variables.resolve(textVar.getText()));
    }

    if (doIt.get() && dialog.open() == null) {
      // The dialog was cancelled
      //
      return new String[0];
    }

    // More than one file selected? Otherwise we simply have the one file the dialog reports.
    //
    String[] filenames = dialog.getFileNames();
    if (filenames.length == 0) {
      filenames = new String[] {buildFilename(dialog.getFilterPath(), dialog.getFileName())};
    }

    // Give plugins the chance to rewrite every selected filename, e.g. to make it relative to
    // the project home. This is done per file since the extension point handles one at a time.
    //
    for (int i = 0; i < filenames.length; i++) {
      try {
        HopGuiFileOpenedExtension openedExtension =
            new HopGuiFileOpenedExtension(dialog, variables, filenames[i]);
        ExtensionPointHandler.callExtensionPoint(
            LogChannel.UI,
            variables,
            HopGuiExtensionPoint.HopGuiFileOpenedDialog.id,
            openedExtension);
        if (openedExtension.filename != null) {
          filenames[i] = openedExtension.filename;
        }
      } catch (Exception xe) {
        LogChannel.UI.logError("Error handling extension point 'HopGuiFileOpenDialog'", xe);
      }
    }

    if (textVar != null) {
      textVar.setText(filenames[0]);
    }

    return filenames;
  }

  static String buildFilename(String filterPath, String fileName) {
    if (StringUtils.isEmpty(filterPath)) {
      return fileName;
    }
    // Is this reading from a VFS URL?
    //
    if (filterPath.contains("://") || filterPath.contains(":///")) {
      // Strip filterPath from fileName (these are equal when selecting a folder)
      fileName = fileName.replace(filterPath, "");
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
    defaultShellHandling(shell, okConsumer, cancelConsumer, true);
  }

  /**
   * Like {@link #defaultShellHandling(Shell, Consumer, Consumer)} but controls minimum shell size.
   *
   * @param useStandardMinimumSize when {@code true}, keeps the legacy minimum (650x250) suited to
   *     large editor dialogs; when {@code false}, minimum size follows the laid-out content so
   *     small prompts are not stretched with empty space.
   */
  public static void defaultShellHandling(
      Shell shell,
      Consumer<Void> okConsumer,
      Consumer<Void> cancelConsumer,
      boolean useStandardMinimumSize) {
    defaultShellHandling(
        shell,
        okConsumer,
        () -> {
          cancelConsumer.accept(null);
          return true;
        },
        useStandardMinimumSize);
  }

  public static void defaultShellHandling(
      Shell shell, Consumer<Void> okConsumer, Supplier<Boolean> cancelSupplier) {
    defaultShellHandling(shell, okConsumer, cancelSupplier, true);
  }

  public static void defaultShellHandling(
      Shell shell,
      Consumer<Void> okConsumer,
      Supplier<Boolean> cancelSupplier,
      boolean useStandardMinimumSize) {

    // If the shell is closed, cancel the dialog
    //
    shell.addListener(SWT.Close, e -> e.doit = cancelSupplier.get());

    // Close on Escape (same as cancel)
    //
    shell.addListener(
        SWT.Traverse,
        e -> {
          if (e.detail == SWT.TRAVERSE_ESCAPE) {
            e.doit = false;
            shell.close();
          }
        });

    Object subject = resolveDialogSubject(shell);
    boolean readOnly = HopDialogEditGuard.isReadOnly(subject);

    // Enter must not commit changes when the dialog is read-only
    //
    Consumer<Void> effectiveOk = readOnly ? v -> {} : okConsumer;

    // Check for enter being pressed in text input fields
    //
    addDefaultListeners(shell, effectiveOk);

    // Add spaces on tab items to make them more manageable
    //
    addSpacesOnTabs(shell);

    if (readOnly) {
      applyReadOnlyMode(shell);
    }

    if (useStandardMinimumSize) {
      shell.setMinimumSize(650, 250);
    } else {
      shell.layout(true, true);
      Point natural = shell.computeSize(SWT.DEFAULT, SWT.DEFAULT);
      shell.setMinimumSize(Math.max(1, natural.x), Math.max(1, natural.y));
    }

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

  /**
   * Attach the object being edited to the shell so {@link #defaultShellHandling} can decide whether
   * the dialog should be read-only (when the subject implements {@link
   * org.apache.hop.core.security.IDialogEditable}).
   *
   * @param shell dialog shell
   * @param subject transform meta, action, metadata object, or any {@code IDialogEditable}
   */
  public static void setDialogSubject(Shell shell, Object subject) {
    if (shell != null && !shell.isDisposed()) {
      shell.setData(DIALOG_SUBJECT, subject);
    }
  }

  /**
   * Run {@code action} with {@code subject} on the dialog-subject stack so nested/legacy dialogs
   * that do not call {@link #setDialogSubject} still open read-only when the subject is not
   * editable.
   *
   * @param subject object being edited (typically {@code IDialogEditable})
   * @param action dialog open work
   * @param <T> result type
   * @return result of action
   */
  public static <T> T withDialogSubject(Object subject, Supplier<T> action) {
    Deque<Object> stack = DIALOG_SUBJECT_STACK.get();
    stack.push(subject);
    try {
      return action.get();
    } finally {
      stack.pop();
      if (stack.isEmpty()) {
        DIALOG_SUBJECT_STACK.remove();
      }
    }
  }

  /**
   * Same as {@link #withDialogSubject(Object, Supplier)} for void work.
   *
   * @param subject object being edited
   * @param action dialog open work
   */
  public static void withDialogSubject(Object subject, Runnable action) {
    withDialogSubject(
        subject,
        () -> {
          action.run();
          return null;
        });
  }

  /**
   * Resolve the subject for a shell: explicit shell data first, then the top of the call-site
   * subject stack (legacy dialogs opened via {@link #withDialogSubject}).
   *
   * @param shell dialog shell (may be null)
   * @return subject or null
   */
  public static Object resolveDialogSubject(Shell shell) {
    if (shell != null && !shell.isDisposed()) {
      Object onShell = shell.getData(DIALOG_SUBJECT);
      if (onShell != null) {
        return onShell;
      }
      // Propagate stack subject onto the shell for consistency
      Object fromStack = peekDialogSubject();
      if (fromStack != null) {
        shell.setData(DIALOG_SUBJECT, fromStack);
        return fromStack;
      }
    }
    return peekDialogSubject();
  }

  private static Object peekDialogSubject() {
    Deque<Object> stack = DIALOG_SUBJECT_STACK.get();
    return stack.isEmpty() ? null : stack.peek();
  }

  /**
   * Mark a control so it stays enabled when the dialog is put into read-only mode (Cancel, Close,
   * Help, …).
   *
   * @param control widget to keep enabled
   */
  public static void keepEnabledInReadOnly(Control control) {
    if (control != null && !control.isDisposed()) {
      control.setData(DIALOG_KEEP_ENABLED, Boolean.TRUE);
    }
  }

  /**
   * Disable editing on all input controls in the shell while leaving Cancel/Close/Help usable.
   * Tables become view-only; text fields become non-editable but remain selectable for copy.
   *
   * @param shell dialog shell
   */
  public static void applyReadOnlyMode(Shell shell) {
    if (shell == null || shell.isDisposed()) {
      return;
    }

    String title = shell.getText();
    String suffix = BaseMessages.getString(PKG, "BaseDialog.ReadOnly.TitleSuffix");
    if (title != null && (suffix == null || !title.contains(suffix.trim()))) {
      shell.setText(title + (suffix != null ? suffix : " (read-only)"));
    }

    applyReadOnlyControls(shell);
  }

  /**
   * Disable editing on all input controls under a composite (metadata perspective tabs, nested
   * editor areas, …). Does not change shell titles.
   *
   * @param root composite or shell root
   */
  public static void applyReadOnlyControls(Composite root) {
    setControlsReadOnly(root);
  }

  /**
   * If {@code subject} is not editable for the current user, apply read-only controls under {@code
   * root}.
   *
   * @param root composite containing editor widgets
   * @param subject metadata object or other {@code IDialogEditable}
   * @return true if read-only mode was applied
   */
  public static boolean applyReadOnlyIfNeeded(Composite root, Object subject) {
    if (root == null || root.isDisposed() || !HopDialogEditGuard.isReadOnly(subject)) {
      return false;
    }
    applyReadOnlyControls(root);
    return true;
  }

  private static void setControlsReadOnly(Composite composite) {
    if (composite == null || composite.isDisposed()) {
      return;
    }

    for (Control control : composite.getChildren()) {
      if (control == null || control.isDisposed()) {
        continue;
      }
      if (control.getData(DIALOG_KEEP_ENABLED) != null) {
        continue;
      }

      if (control instanceof TableView tableView) {
        tableView.setReadonly(true);
        continue;
      }

      if (control instanceof Text text) {
        text.setEditable(false);
        continue;
      }

      // StyledText is desktop-only; never reference the class on the RAP classpath
      if (setStyledTextNonEditable(control)) {
        continue;
      }

      if (control instanceof TextVar textVar) {
        textVar.setEditable(false);
        continue;
      }

      if (control instanceof TextComposite textComposite) {
        textComposite.setEditable(false);
        continue;
      }

      if (control instanceof Button button) {
        if (isDismissOrHelpButton(button)) {
          keepEnabledInReadOnly(button);
          continue;
        }
        button.setEnabled(false);
        continue;
      }

      if (control instanceof Combo
          || control instanceof CCombo
          || control instanceof ComboVar
          || control instanceof List
          || control instanceof Spinner
          || control instanceof MetaSelectionLine
          || control instanceof ToolBar) {
        control.setEnabled(false);
        continue;
      }

      // CTabFolder / Group / plain Composite: recurse so tabs stay switchable
      if (control instanceof Composite child) {
        setControlsReadOnly(child);
      }
    }
  }

  /** Cached optional StyledText class (null when unavailable, e.g. RAP / Hop Web). */
  private static final Class<?> STYLED_TEXT_CLASS = loadStyledTextClass();

  private static Class<?> loadStyledTextClass() {
    try {
      return Class.forName("org.eclipse.swt.custom.StyledText");
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return null;
    }
  }

  /**
   * Desktop SWT has {@code StyledText}; RAP does not. Uses a cached reflective look-up so this
   * class never fails with {@link NoClassDefFoundError} on Hop Web.
   *
   * @param control widget under consideration
   * @return true if the control was a StyledText and was made non-editable
   */
  private static boolean setStyledTextNonEditable(Control control) {
    if (STYLED_TEXT_CLASS == null || !STYLED_TEXT_CLASS.isInstance(control)) {
      return false;
    }
    try {
      STYLED_TEXT_CLASS.getMethod("setEditable", boolean.class).invoke(control, false);
      return true;
    } catch (ReflectiveOperationException ignored) {
      return false;
    }
  }

  /**
   * Recognise standard dismiss / help buttons by their (i18n) label so they stay clickable in
   * read-only mode even when not marked with {@link #DIALOG_KEEP_ENABLED}.
   */
  private static boolean isDismissOrHelpButton(Button button) {
    String text = button.getText();
    if (text == null || text.isBlank()) {
      // Icon-only help buttons still need a chance — keep if image present and no text
      return button.getImage() != null;
    }
    String normalized = normalizeButtonLabel(text);
    if (normalized.isEmpty()) {
      return false;
    }
    return normalized.equals(
            normalizeButtonLabel(BaseMessages.getString(PKG, "System.Button.Cancel")))
        || normalized.equals(
            normalizeButtonLabel(BaseMessages.getString(PKG, "System.Button.Close")))
        || normalized.equals(
            normalizeButtonLabel(BaseMessages.getString(PKG, "System.Button.Help")))
        || normalized.equals("cancel")
        || normalized.equals("close")
        || normalized.equals("help");
  }

  private static String normalizeButtonLabel(String text) {
    if (text == null) {
      return "";
    }
    // Strip mnemonic (&), whitespace and punctuation used in System.Button.* messages
    return text.replace("&", "").replaceAll("\\s+", "").toLowerCase();
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

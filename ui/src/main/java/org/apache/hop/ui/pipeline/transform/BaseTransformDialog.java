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

package org.apache.hop.ui.pipeline.transform;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.DialogBoxWithButtons;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Monitor;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** This class provides functionality common to Transform Dialogs. */
public abstract class BaseTransformDialog extends Dialog implements ITransformDialog {
  private static final Class<?> PKG = ITransform.class;

  /** The logging object interface for this dialog. */
  public static final ILoggingObject loggingObject =
      new SimpleLoggingObject("Transform dialog", LoggingObjectType.TRANSFORM_DIALOG, null);

  @Getter protected IVariables variables;

  /** The transform name. */
  protected String transformName;

  /** The Transform name label. */
  protected Label wlTransformName;

  /** The Transform name UI component. */
  protected Text wTransformName;

  /** Horizontal spacer below the transform name line; use for top attachment of dialog content. */
  protected Label wSpacer;

  /** The FormData for the transform name and its label. */
  protected FormData fdlTransformName;

  protected FormData fdTransformName;

  /** Common dialog buttons. */
  protected Button wOk;

  protected Button wGet;
  protected Button wPreview;
  protected Button wSql;
  protected Button wCancel;

  /** FormData for the common dialog buttons. */
  protected FormData fdOk;

  protected FormData fdGet;

  /** The metadata for the associated pipeline. */
  protected PipelineMeta pipelineMeta;

  /** A reference to the parent shell. */
  protected Shell shell;

  /** A listener for dialog resizing. */
  protected Listener lsResize;

  /** Whether the dialog (and its backup) have changed. */
  protected boolean changed;

  protected boolean backupChanged;

  /** The base transform meta. */
  protected ITransformMeta baseTransformMeta;

  /** The UI properties. */
  protected PropsUi props;

  /**
   * The middle percentage for form layouts. Initialized by {@link #createShell(String)} to {@code
   * props.getMiddlePct()}.
   */
  protected int middle;

  /**
   * The margin size for form layouts. Initialized by {@link #createShell(String)} to {@code
   * PropsUi.getMargin()}.
   */
  protected int margin;

  /**
   * Standard modify listener that marks the transform as changed. Initialized by {@link
   * #createShell(String)} to call {@code baseTransformMeta.setChanged()}.
   */
  protected ModifyListener lsMod;

  /** The MetaStore to use */
  @Getter @Setter protected IHopMetadataProvider metadataProvider;

  /** The transform meta for this dialog. */
  protected TransformMeta transformMeta;

  /** The log channel for this dialog. */
  protected LogChannel log;

  /** A constant indicating a center button alignment. */
  protected static final int BUTTON_ALIGNMENT_CENTER = 0;

  /** A constant indicating a left button alignment. */
  protected static final int BUTTON_ALIGNMENT_LEFT = 1;

  /** A constant indicating a right button alignment. */
  public static final int BUTTON_ALIGNMENT_RIGHT = 2;

  /** The button alignment (defaults to center). */
  protected static int buttonAlignment = BUTTON_ALIGNMENT_CENTER;

  static {
    // Get the button alignment
    buttonAlignment = getButtonAlignment();
  }

  /**
   * Instantiates a new base transform dialog.
   *
   * @param parent the parent shell
   * @param baseTransformMeta the associated base transform metadata
   * @param pipelineMeta the associated pipeline metadata
   */
  public BaseTransformDialog(
      Shell parent,
      IVariables variables,
      ITransformMeta baseTransformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, SWT.NONE);

    this.log = new LogChannel(baseTransformMeta);
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.baseTransformMeta = (ITransformMeta) baseTransformMeta;
    this.transformName = baseTransformMeta.getParentTransformMeta().getName();
    this.transformMeta = pipelineMeta.findTransform(transformName);
    this.backupChanged = baseTransformMeta.hasChanged();
    this.props = PropsUi.getInstance();
    this.metadataProvider = HopGui.getInstance().getMetadataProvider();
  }

  /**
   * Creates and initializes the shell for a transform dialog with standard settings and adds the
   * transform name field. This method handles all the common boilerplate:
   *
   * <ul>
   *   <li>Creates the shell with appropriate style flags (web-safe in Hop Web)
   *   <li>Applies PropsUi look and feel
   *   <li>Sets the shell image from the transform metadata
   *   <li>Applies a FormLayout with standard margins
   *   <li>Sets the shell title
   *   <li>Initializes {@link #middle} and {@link #margin} fields for layout calculations
   *   <li>Initializes {@link #lsMod} modify listener for change tracking
   *   <li>Creates the transform name label and text field ({@link #wlTransformName} and {@link
   *       #wTransformName})
   * </ul>
   *
   * <p>After calling this method, the {@code shell} is ready to have more controls added to it. The
   * transform name field is already created and can be used as the first control for layout
   * chaining via the returned Control.
   *
   * <p>Example usage in a transform dialog:
   *
   * <pre>
   * public String open() {
   *   Control lastControl = createShell(BaseMessages.getString(PKG, "MyTransformDialog.DialogTitle"));
   *
   *   // Transform name field is already created! Now add your custom fields:
   *   // middle, margin, and lsMod are available
   *   wlMyField = new Label(shell, SWT.RIGHT);
   *   wlMyField.setText("My Field:");
   *   PropsUi.setLook(wlMyField);
   *   fdlMyField = new FormData();
   *   fdlMyField.left = new FormAttachment(0, 0);
   *   fdlMyField.right = new FormAttachment(middle, -margin);
   *   fdlMyField.top = new FormAttachment(lastControl, margin);
   *   wlMyField.setLayoutData(fdlMyField);
   *
   *   // ... rest of your dialog construction
   *   getData();
   *   focusTransformName();
   *   BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
   *   return transformName;
   * }
   * </pre>
   *
   * @param title The title for the dialog window (typically obtained via BaseMessages.getString())
   * @return The spacer below the transform name line (wSpacer) to use as the first control for
   *     layout chaining (lastControl)
   */
  protected Control createShell(String title) {
    Shell parent = getParent();
    if (OsHelper.isMac()) {
      // On macOS, create independent shell to support multi-monitor
      shell = new Shell(parent.getDisplay(), BaseDialog.getDefaultDialogStyle());
    } else {
      // On other platforms, use parent for proper modal behavior
      shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    }
    PropsUi.setLook(shell);
    setShellImage(shell, baseTransformMeta);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(title);

    // Initialize commonly used layout values
    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // Initialize standard modify listener
    lsMod = e -> baseTransformMeta.setChanged();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
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
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    wSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wTransformName, margin);
    fdSpacer.right = new FormAttachment(100, 0);
    wSpacer.setLayoutData(fdSpacer);

    return wSpacer;
  }

  /**
   * Instantiates a new base transform dialog.
   *
   * @param parent the parent shell
   * @param baseTransformMeta the associated base transform metadata
   * @param pipelineMeta the associated pipeline metadata
   * @param transformName the transform name
   */
  public BaseTransformDialog(
      Shell parent,
      IVariables variables,
      ITransformMeta baseTransformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, SWT.NONE);

    this.log = new LogChannel(baseTransformMeta);
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.transformName = transformName;
    this.transformMeta = pipelineMeta.findTransform(transformName);
    this.baseTransformMeta = baseTransformMeta;
    this.backupChanged = baseTransformMeta.hasChanged();
    this.props = PropsUi.getInstance();
  }

  /**
   * Instantiates a new base transform dialog.
   *
   * @param parent the parent shell
   * @param nr the number of rows
   * @param variables
   * @param in the base transform metadata
   * @param tr the pipeline metadata
   */
  public BaseTransformDialog(
      Shell parent, int nr, IVariables variables, BaseTransformMeta<?, ?> in, PipelineMeta tr) {
    this(parent, variables, in, tr, null);
  }

  /**
   * Sets the shell image.
   *
   * @param shell the shell
   * @param transformMetaInterface the transform meta interface (because of the legacy code)
   */
  public void setShellImage(Shell shell, ITransformMeta transformMetaInterface) {

    setShellImage(shell);

    if (transformMeta.isDeprecated()) {
      addDeprecation();
    }
  }

  public void setActive() {
    if (shell != null && !shell.isDisposed()) {
      shell.setActive();
    }
  }

  private void addDeprecation() {

    if (shell == null) {

      return;
    }
    shell.addShellListener(
        new ShellAdapter() {

          private boolean deprecation = false;

          @Override
          public void shellActivated(ShellEvent shellEvent) {
            super.shellActivated(shellEvent);
            if (!transformMeta.isDeprecated() || deprecation) {
              return;
            }
            String deprecated = BaseMessages.getString(PKG, "System.Deprecated").toLowerCase();
            shell.setText(shell.getText() + " (" + deprecated + ")");
            deprecation = true;
          }
        });
  }

  /** Dispose this dialog. */
  public void dispose() {
    WindowProperty winprop = new WindowProperty(shell);

    // Always save to session storage for immediate reopening during current session
    props.setSessionScreen(winprop);

    // If user wants to persist dialog positions across restarts, also save to persistent storage
    if (!props.getResetDialogPositionsOnRestart()) {
      props.setScreen(winprop);
    }

    shell.dispose();
  }

  public boolean isDisposed() {
    if (shell != null) {
      return shell.isDisposed();
    }
    return true;
  }

  /**
   * Set the shell size, based upon the previous time the geometry was saved in the Properties file.
   */
  public void setSize() {
    setSize(shell);
  }

  /**
   * Sets the transform name field from {@link #transformName}, selects its text and gives it focus.
   * Call this just before {@link BaseDialog#defaultShellHandling(Shell,
   * java.util.function.Consumer, java.util.function.Consumer)} so that when the dialog is shown the
   * transform name has focus. Safe to call if the control is disposed; no-op in that case.
   */
  protected void focusTransformName() {
    if (wTransformName != null && !wTransformName.isDisposed()) {
      if (transformName != null) {
        wTransformName.setText(transformName);
      }
      wTransformName.selectAll();
      wTransformName.setFocus();
    }
  }

  /**
   * Sets the button positions.
   *
   * @param buttons the buttons
   * @param margin the margin between buttons
   * @param lastControl the last control
   */
  protected void setButtonPositions(Button[] buttons, int margin, Control lastControl) {
    BaseTransformDialog.positionBottomButtons(shell, buttons, margin, lastControl);
  }

  /**
   * Position the specified buttons at the bottom of the parent composite. Also, make the buttons
   * all the same width: the width of the largest button.
   *
   * <p>The default alignment for buttons in the system will be used. This is set as an LAF property
   * with the key <code>Button_Position</code> and has the valid values of <code>left, center, right
   * </code> with <code>center</code> being the default.
   *
   * @param composite the composite
   * @param buttons The buttons to position.
   * @param margin The margin between the buttons in pixels
   * @param lastControl the last control
   */
  public static final void positionBottomButtons(
      Composite composite, Button[] buttons, int margin, Control lastControl) {
    // call positionBottomButtons method the system button alignment
    positionBottomButtons(composite, buttons, margin, buttonAlignment, lastControl);
  }

  public static final void positionBottomButtons(
      Composite composite, Button[] buttons, int margin, int alignment, Control lastControl) {
    // Set a default font on all the buttons
    //
    for (Button button : buttons) {
      button.setFont(GuiResource.getInstance().getFontDefault());
    }

    // Determine the largest button in the array
    Rectangle largest = null;
    for (Button value : buttons) {
      value.pack(true);
      Rectangle r = value.getBounds();
      if (largest == null || r.width > largest.width) {
        largest = r;
      }

      // Also, set the tooltip the same as the name if we don't have one...
      if (value.getToolTipText() == null) {
        value.setToolTipText(Const.replace(value.getText(), "&", ""));
      }
    }

    // Make buttons a bit larger... (nicer)
    largest.width += 10;
    if ((largest.width % 2) == 1) {
      largest.width++;
    }

    // Compute the left side of the 1st button
    switch (alignment) {
      case BUTTON_ALIGNMENT_CENTER:
        centerButtons(buttons, largest.width, margin, lastControl);
        break;
      case BUTTON_ALIGNMENT_LEFT:
        leftAlignButtons(buttons, largest.width, margin, lastControl);
        break;
      case BUTTON_ALIGNMENT_RIGHT:
        rightAlignButtons(buttons, largest.width, margin, lastControl);
        break;
      default:
        break;
    }
    if (Const.isOSX()) {
      Shell parentShell = composite.getShell();
      final List<TableView> tableViews = new ArrayList<>();
      getTableViews(parentShell, tableViews);
      for (final Button button : buttons) {
        // We know the table views
        // We also know that if a button is hit, the table loses focus
        // In that case, we can apply the content of an open text editor...
        //
        button.addSelectionListener(
            new SelectionAdapter() {

              @Override
              public void widgetSelected(SelectionEvent e) {
                for (TableView view : tableViews) {
                  view.applyOSXChanges();
                }
              }
            });
      }
    }
  }

  /**
   * Gets the table views.
   *
   * @param parentControl the parent control
   * @param tableViews the table views
   * @return the table views
   */
  private static final void getTableViews(Control parentControl, List<TableView> tableViews) {
    if (parentControl instanceof TableView tableViewParentControl) {
      tableViews.add(tableViewParentControl);
    } else {
      if (parentControl instanceof Composite compositeParentControl) {
        Control[] children = compositeParentControl.getChildren();
        for (Control child : children) {
          getTableViews(child, tableViews);
        }
      } else {
        if (parentControl instanceof Shell shellParentControl) {
          Control[] children = shellParentControl.getChildren();
          for (Control child : children) {
            getTableViews(child, tableViews);
          }
        }
      }
    }
  }

  /**
   * Returns the default alignment for the buttons. This is set in the LAF properties with the key
   * <code>Button_Position</code>. The valid values are:
   *
   * <UL>
   *   <LI><code>left</code>
   *   <LI><code>center</code>
   *   <LI><code>right</code>
   * </UL>
   *
   * NOTE: if the alignment is not provided or contains an invalid value, <code>center</code> will
   * be used as a default
   *
   * @return a constant which indicates the button alignment
   */
  protected static int getButtonAlignment() {
    String buttonAlign = BasePropertyHandler.getProperty("Button_Position", "center").toLowerCase();
    if ("center".equals(buttonAlign)) {
      return BUTTON_ALIGNMENT_CENTER;
    } else if ("left".equals(buttonAlign)) {
      return BUTTON_ALIGNMENT_LEFT;
    } else {
      return BUTTON_ALIGNMENT_RIGHT;
    }
  }

  /**
   * Creats a default FormData object with the top / bottom / and left set (this is done to cut down
   * on repetative code lines.
   *
   * @param button the button to which this form data will be applied
   * @param width the width of the button
   * @param margin the margin between buttons
   * @param lastControl the last control above the buttons
   * @return the newly created FormData object
   */
  private static FormData createDefaultFormData(
      Button button, int width, int margin, Control lastControl) {
    FormData formData = new FormData();
    if (lastControl != null) {
      formData.top = new FormAttachment(lastControl, margin * 3);
    } else {
      formData.bottom = new FormAttachment(100, 0);
    }
    formData.right = new FormAttachment(button, width + margin);
    return formData;
  }

  /**
   * Aligns the buttons as left-aligned on the dialog.
   *
   * @param buttons the array of buttons to align
   * @param width the standardized width of all the buttons
   * @param margin the margin between buttons
   * @param lastControl (optional) the bottom most control used for aligning the buttons relative to
   *     the bottom of the controls on the dialog
   */
  protected static void leftAlignButtons(
      Button[] buttons, int width, int margin, Control lastControl) {
    for (int i = 0; i < buttons.length; ++i) {
      FormData formData = createDefaultFormData(buttons[i], width, margin, lastControl);

      // Set the left side of the buttons (either offset from the edge, or relative to the previous
      // button)
      if (i == 0) {
        formData.left = new FormAttachment(0, margin);
      } else {
        formData.left = new FormAttachment(buttons[i - 1], margin);
      }

      // Apply the layout data
      buttons[i].setLayoutData(formData);
    }
  }

  /**
   * Aligns the buttons as right-aligned on the dialog.
   *
   * @param buttons the array of buttons to align
   * @param width the standardized width of all the buttons
   * @param margin the margin between buttons
   * @param lastControl (optional) the bottom most control used for aligning the buttons relative to
   *     the bottom of the controls on the dialog
   */
  protected static void rightAlignButtons(
      Button[] buttons, int width, int margin, Control lastControl) {
    for (int i = buttons.length - 1; i >= 0; --i) {
      FormData formData = createDefaultFormData(buttons[i], width, margin, lastControl);

      // Set the right side of the buttons (either offset from the edge, or relative to the previous
      // button)
      if (i == buttons.length - 1) {
        formData.left = new FormAttachment(100, -(width + margin));
      } else {
        formData.left = new FormAttachment(buttons[i + 1], -(2 * (width + margin)) - margin);
      }

      // Apply the layout data
      buttons[i].setLayoutData(formData);
    }
  }

  /**
   * Aligns the buttons as centered on the dialog.
   *
   * @param buttons the array of buttons to align
   * @param width the standardized width of all the buttons
   * @param margin the margin between buttons
   * @param lastControl (optional) the bottom most control used for aligning the buttons relative to
   *     the bottom of the controls on the dialog
   */
  protected static void centerButtons(
      Button[] buttons, int width, int margin, Control lastControl) {
    // Setup the middle button
    int middleButtonIndex = buttons.length / 2;
    FormData formData =
        createDefaultFormData(buttons[middleButtonIndex], width, margin, lastControl);

    // See if we have an even or odd number of buttons...
    int leftOffset = 0;
    if (buttons.length % 2 == 0) {
      // Even number of buttons - the middle is between buttons. The "middle" button is
      // actually to the right of middle
      leftOffset = margin;
    } else {
      // Odd number of buttons - tht middle is in the middle of the button
      leftOffset = -(width + margin) / 2;
    }
    formData.left = new FormAttachment(50, leftOffset);
    buttons[middleButtonIndex].setLayoutData(formData);

    // Do the buttons to the right of the middle
    for (int i = middleButtonIndex + 1; i < buttons.length; ++i) {
      formData = createDefaultFormData(buttons[i], width, margin, lastControl);
      formData.left = new FormAttachment(buttons[i - 1], margin);
      buttons[i].setLayoutData(formData);
    }

    // Do the buttons to the left of the middle
    for (int i = middleButtonIndex - 1; i >= 0; --i) {
      formData = createDefaultFormData(buttons[i], width, margin, lastControl);
      formData.left = new FormAttachment(buttons[i + 1], -(2 * (width + margin)) - margin);
      buttons[i].setLayoutData(formData);
    }
  }

  /**
   * Gets the modify listener tooltip text.
   *
   * @param variables
   * @param textField the text field
   * @return the modify listener tooltip text
   */
  public static final ModifyListener getModifyListenerTooltipText(
      IVariables variables, final TextVar textField) {
    return e ->
        // maybe replace this with extra arguments
        textField.setToolTipText(variables.resolve(textField.getText()));
  }

  /**
   * Adds the connection line for the given parent and previous control, and returns a meta
   * selection manager control
   *
   * @param parent the parent composite object
   * @param previous the previous control
   * @param selected the selected DatabaseMeta
   * @param lsMod Changed listener
   * @return the combo box UI component
   */
  public MetaSelectionLine<DatabaseMeta> addConnectionLine(
      Composite parent, Control previous, DatabaseMeta selected, ModifyListener lsMod) {

    return addConnectionLine(
        parent,
        previous,
        selected,
        lsMod,
        BaseMessages.getString(PKG, "BaseTransformDialog.Connection.Label"),
        BaseMessages.getString(PKG, "BaseTransformDialog.Connection.Tooltip"));
  }

  public MetaSelectionLine<DatabaseMeta> addConnectionLine(
      Composite parent,
      Control previous,
      DatabaseMeta selected,
      ModifyListener lsMod,
      String connectionLabel) {
    return addConnectionLine(
        parent,
        previous,
        selected,
        lsMod,
        connectionLabel,
        BaseMessages.getString(PKG, "BaseTransformDialog.Connection.Tooltip"));
  }

  public MetaSelectionLine<DatabaseMeta> addConnectionLine(
      Composite parent,
      Control previous,
      DatabaseMeta selected,
      ModifyListener lsMod,
      String connectionLabel,
      String connectionTooltip) {
    final MetaSelectionLine<DatabaseMeta> wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            DatabaseMeta.class,
            parent,
            SWT.NONE,
            connectionLabel,
            connectionTooltip);
    wConnection.addToConnectionLine(parent, previous, selected, lsMod);
    return wConnection;
  }

  public String toString() {
    return this.getClass().getName();
  }

  /**
   * Sets the minimal shell height.
   *
   * @param shell the shell
   * @param controls the controls to measure
   * @param margin the margin between the components
   * @param extra the extra padding
   */
  public static void setMinimalShellHeight(Shell shell, Control[] controls, int margin, int extra) {
    int height = 0;

    for (Control control : controls) {
      Rectangle bounds = control.getBounds();
      height += bounds.height + margin;
    }
    height += extra;
    shell.setSize(shell.getBounds().width, height);
  }

  /**
   * Sets the size of this dialog with respect to the given shell.
   *
   * @param shell the new size
   */
  public static void setSize(Shell shell) {
    setSize(shell, -1, -1, !EnvironmentUtils.getInstance().isWeb());
  }

  public static void setSize(Shell shell, int minWidth, int minHeight) {
    setSize(shell, minWidth, minHeight, false);
  }

  /**
   * Checks if a window name represents the main Hop GUI window.
   *
   * @param windowName the window title to check
   * @return true if this is the main window, false otherwise
   */
  private static boolean isMainWindow(String windowName) {
    // The main window is identified by the "Hop" title or the localized application name
    return windowName != null && (windowName.equals("Hop") || windowName.contains("Hop"));
  }

  /**
   * Sets the size of this dialog with respect to the given parameters.
   *
   * @param shell the shell
   * @param minWidth the minimum width
   * @param minHeight the minimum height
   * @param packIt true to pack the dialog components, false otherwise
   */
  public static void setSize(Shell shell, int minWidth, int minHeight, boolean packIt) {
    PropsUi props = PropsUi.getInstance();

    // Check session-only storage first (for dialogs during current session)
    WindowProperty winprop = props.getSessionScreen(shell.getText());

    // If not in session storage, check persistent storage if user wants to persist positions
    // (or if it's the main window - main window should always restore from persistent storage)
    if (winprop == null) {
      // Only check persistent storage if reset setting is disabled, OR for main window
      if (!props.getResetDialogPositionsOnRestart() || isMainWindow(shell.getText())) {
        winprop = props.getScreen(shell.getText());
      }
    }

    if (winprop != null) {
      winprop.setShell(shell, minWidth, minHeight);
    } else {
      if (packIt) {
        shell.pack();
      } else {
        shell.layout();
      }

      // OK, sometimes this produces dialogs that are waay too big.
      // Try to limit this a bit, m'kay?
      // Use the same algorithm by cheating :-)
      //
      winprop = new WindowProperty(shell);
      winprop.setShell(shell, minWidth, minHeight);

      // Now, as this is the first time it gets opened, try to center it on the main window...
      Rectangle shellBounds = shell.getBounds();

      // Find the main window to center on - prefer parent shell, then active shell, then any shell
      Shell mainWindow = null;
      if (shell.getParent() != null && shell.getParent() instanceof Shell) {
        // Dialog has a parent shell (non-macOS case)
        mainWindow = (Shell) shell.getParent();
      } else {
        // Dialog has no parent (macOS multi-monitor case) - find the active shell
        Shell activeShell = shell.getDisplay().getActiveShell();
        if (activeShell != null && !activeShell.equals(shell)) {
          mainWindow = activeShell;
        } else {
          // If no active shell, try to find any open shell (likely the main window)
          Shell[] shells = shell.getDisplay().getShells();
          for (Shell s : shells) {
            if (s != null && !s.equals(shell) && !s.isDisposed()) {
              mainWindow = s;
              break;
            }
          }
        }
      }

      int middleX, middleY;
      if (mainWindow != null) {
        // Center on the main window
        Rectangle mainBounds = mainWindow.getBounds();
        middleX = mainBounds.x + (mainBounds.width - shellBounds.width) / 2;
        middleY = mainBounds.y + (mainBounds.height - shellBounds.height) / 2;
      } else {
        // Fallback: center on the primary monitor if we couldn't find the main window
        Monitor monitor = shell.getDisplay().getPrimaryMonitor();
        Rectangle monitorClientArea = monitor.getClientArea();
        middleX = monitorClientArea.x + (monitorClientArea.width - shellBounds.width) / 2;
        middleY = monitorClientArea.y + (monitorClientArea.height - shellBounds.height) / 2;
      }

      shell.setLocation(middleX, middleY);
    }
  }

  /**
   * Sets the traverse order for the given controls.
   *
   * @param controls the new traverse order
   */
  public static final void setTraverseOrder(final Control[] controls) {
    for (int i = 0; i < controls.length; i++) {
      final int controlNr = i;
      if (i < controls.length - 1) {
        controls[i].addTraverseListener(
            te -> {
              te.doit = false;
              // set focus on the next control.
              // What is the next control?
              int thisOne = controlNr + 1;
              while (!controls[thisOne].isEnabled()) {
                thisOne++;
                if (thisOne >= controls.length) {
                  thisOne = 0;
                }
                if (thisOne == controlNr) {
                  return; // already tried all others, time to quit.
                }
              }
              controls[thisOne].setFocus();
            });
      } else { // Link last item to first.

        controls[i].addTraverseListener(
            te -> {
              te.doit = false;
              // set focus on the next control.
              // set focus on the next control.
              // What is the next control : 0
              int thisOne = 0;
              while (!controls[thisOne].isEnabled()) {
                thisOne++;
                if (thisOne >= controls.length) {
                  return; // already tried all others, time to quit.
                }
              }
              controls[thisOne].setFocus();
            });
      }
    }
  }

  /**
   * Gets unused fields from previous transforms and inserts them as rows into a table view.
   *
   * @param variables
   * @param pipelineMeta the pipeline metadata
   * @param transformMeta the transform metadata
   * @param tableView the table view
   * @param keyColumn the key column
   * @param nameColumn the name column
   * @param dataTypeColumn the data type column
   * @param lengthColumn the length column
   * @param precisionColumn the precision column
   * @param listener a listener for tables insert events
   */
  public static final void getFieldsFromPrevious(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      TableView tableView,
      int keyColumn,
      int[] nameColumn,
      int[] dataTypeColumn,
      int lengthColumn,
      int precisionColumn,
      ITableItemInsertListener listener) {
    try {
      IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      if (row != null) {
        getFieldsFromPrevious(
            row,
            tableView,
            keyColumn,
            nameColumn,
            dataTypeColumn,
            lengthColumn,
            precisionColumn,
            listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          tableView.getShell(),
          BaseMessages.getString(PKG, "BaseTransformDialog.FailedToGetFields.Title"),
          BaseMessages.getString(
              PKG, "BaseTransformDialog.FailedToGetFields.Message", transformMeta.getName()),
          ke);
    }
  }

  /**
   * Gets unused fields from previous transforms and inserts them as rows into a table view.
   *
   * @param row the input fields
   * @param tableView the table view to modify
   * @param keyColumn the column in the table view to match with the names of the fields, checks for
   *     existance if >0
   * @param nameColumn the column numbers in which the name should end up in
   * @param dataTypeColumn the target column numbers in which the data type should end up in
   * @param lengthColumn the length column where the length should end up in (if >0)
   * @param precisionColumn the length column where the precision should end up in (if >0)
   * @param listener A listener that you can use to do custom modifications to the inserted table
   *     item, based on a value from the provided row
   */
  public static final void getFieldsFromPrevious(
      IRowMeta row,
      TableView tableView,
      int keyColumn,
      int[] nameColumn,
      int[] dataTypeColumn,
      int lengthColumn,
      int precisionColumn,
      ITableItemInsertListener listener) {
    getFieldsFromPrevious(
        row,
        tableView,
        keyColumn,
        nameColumn,
        dataTypeColumn,
        lengthColumn,
        precisionColumn,
        true,
        listener);
  }

  /**
   * Gets unused fields from previous transforms and inserts them as rows into a table view.
   *
   * @param row the input fields
   * @param tableView the table view to modify
   * @param keyColumn the column in the table view to match with the names of the fields, checks for
   *     existance if >0
   * @param nameColumn the column numbers in which the name should end up in
   * @param dataTypeColumn the target column numbers in which the data type should end up in
   * @param lengthColumn the length column where the length should end up in (if >0)
   * @param precisionColumn the length column where the precision should end up in (if >0)
   * @param optimizeWidth
   * @param listener A listener that you can use to do custom modifications to the inserted table
   *     item, based on a value from the provided row
   */
  public static final void getFieldsFromPrevious(
      IRowMeta row,
      TableView tableView,
      int keyColumn,
      int[] nameColumn,
      int[] dataTypeColumn,
      int lengthColumn,
      int precisionColumn,
      boolean optimizeWidth,
      ITableItemInsertListener listener) {
    getFieldsFromPrevious(
        row,
        tableView,
        keyColumn,
        nameColumn,
        dataTypeColumn,
        lengthColumn,
        precisionColumn,
        optimizeWidth,
        listener,
        BaseTransformDialog::getFieldsChoiceDialog);
  }

  /**
   * Gets unused fields from previous transforms and inserts them as rows into a table view.
   *
   * @param row the input fields
   * @param tableView the table view to modify
   * @param keyColumn the column in the table view to match with the names of the fields, checks for
   *     existance if >0
   * @param nameColumn the column numbers in which the name should end up in
   * @param dataTypeColumn the target column numbers in which the data type should end up in
   * @param lengthColumn the length column where the length should end up in (if >0)
   * @param precisionColumn the length column where the precision should end up in (if >0)
   * @param optimizeWidth
   * @param listener A listener that you can use to do custom modifications to the inserted table
   *     item, based on a value from the provided row
   * @param getFieldsChoiceDialogProvider the GetFieldsChoice dialog provider
   */
  public static final void getFieldsFromPrevious(
      IRowMeta row,
      TableView tableView,
      int keyColumn,
      int[] nameColumn,
      int[] dataTypeColumn,
      int lengthColumn,
      int precisionColumn,
      boolean optimizeWidth,
      ITableItemInsertListener listener,
      IFieldsChoiceDialogProvider getFieldsChoiceDialogProvider) {
    if (row == null || row.isEmpty()) {
      return; // nothing to do
    }

    Table table = tableView.table;

    // get a list of all the non-empty keys (names)
    //
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < table.getItemCount(); i++) {
      TableItem tableItem = table.getItem(i);
      String key = tableItem.getText(keyColumn);
      if (!Utils.isEmpty(key) && keys.indexOf(key) < 0) {
        keys.add(key);
      }
    }

    int choice = 0;

    if (!keys.isEmpty()) {
      // Ask what we should do with the existing data in the transform.
      //
      DialogBoxWithButtons getFieldsChoiceDialog =
          getFieldsChoiceDialogProvider.provide(tableView.getShell(), keys.size(), row.size());

      int idx = getFieldsChoiceDialog.open();
      choice = idx & 0xFF;
    }

    if (choice == 3 || choice == 255) {
      return; // Cancel clicked
    }

    if (choice == 2) {
      tableView.clearAll(false);
    }

    for (int i = 0; i < row.size(); i++) {
      IValueMeta v = row.getValueMeta(i);

      boolean add = true;

      if (choice == 0
          && keys.indexOf(v.getName()) >= 0) { // hang on, see if it's not yet in the table view
        add = false;
      }

      if (add) {
        TableItem tableItem = new TableItem(table, SWT.NONE);

        for (int k : nameColumn) {
          tableItem.setText(k, Const.NVL(v.getName(), ""));
        }
        if (dataTypeColumn != null) {
          for (int j : dataTypeColumn) {
            tableItem.setText(j, v.getTypeDesc());
          }
        }
        if (lengthColumn > 0 && v.getLength() >= 0) {
          tableItem.setText(lengthColumn, Integer.toString(v.getLength()));
        }
        if (precisionColumn > 0 && v.getPrecision() >= 0) {
          tableItem.setText(precisionColumn, Integer.toString(v.getPrecision()));
        }

        if (listener != null && !listener.tableItemInserted(tableItem, v)) {
          tableItem.dispose(); // remove it again
        }
      }
    }
    tableView.removeEmptyRows();
    tableView.setRowNums();
    if (optimizeWidth) {
      tableView.optWidth(true);
    }
  }

  static DialogBoxWithButtons getFieldsChoiceDialog(
      Shell shell, int existingFields, int newFields) {
    return new DialogBoxWithButtons(
        shell,
        BaseMessages.getString(PKG, "BaseTransformDialog.GetFieldsChoice.Title"), // "Warning!"
        BaseMessages.getString(
            PKG,
            "BaseTransformDialog.GetFieldsChoice.Message",
            "" + existingFields,
            "" + newFields),
        new String[] {
          BaseMessages.getString(PKG, "BaseTransformDialog.AddNew"),
          BaseMessages.getString(PKG, "BaseTransformDialog.Add"),
          BaseMessages.getString(PKG, "BaseTransformDialog.ClearAndAdd"),
          BaseMessages.getString(PKG, "BaseTransformDialog.Cancel"),
        });
  }

  /**
   * Gets fields from previous transforms and populate a ComboVar.
   *
   * @param variables
   * @param comboVar the Combo Box (with Variables) to populate
   * @param pipelineMeta the pipeline metadata
   * @param transformMeta the transform metadata
   */
  public static final void getFieldsFromPrevious(
      IVariables variables,
      ComboVar comboVar,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta) {
    String selectedField;
    int indexField = -1;
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformMeta);
      selectedField = comboVar.getText();
      comboVar.removeAll();

      if (r != null && !r.isEmpty()) {
        r.getFieldNames();
        comboVar.setItems(r.getFieldNames());
        indexField = r.indexOfValue(selectedField);
      }
      // Select value if possible...
      if (indexField > -1) {
        comboVar.select(indexField);
      } else {
        if (selectedField != null) {
          comboVar.setText(selectedField);
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          comboVar.getShell(),
          BaseMessages.getString(PKG, "BaseTransformDialog.FailedToGetFieldsPrevious.DialogTitle"),
          BaseMessages.getString(
              PKG, "BaseTransformDialog.FailedToGetFieldsPrevious.DialogMessage"),
          ke);
    }
  }

  /**
   * Checks if the log level is basic.
   *
   * @return true, if the log level is basic, false otherwise
   */
  public boolean isBasic() {
    return log.isBasic();
  }

  /**
   * Checks if the log level is detailed.
   *
   * @return true, if the log level is detailed, false otherwise
   */
  public boolean isDetailed() {
    return log.isDetailed();
  }

  /**
   * Checks if the log level is debug.
   *
   * @return true, if the log level is debug, false otherwise
   */
  public boolean isDebug() {
    return log.isDebug();
  }

  /**
   * Checks if the log level is row level.
   *
   * @return true, if the log level is row level, false otherwise
   */
  public boolean isRowLevel() {
    return log.isRowLevel();
  }

  /**
   * Log the message at a minimal logging level.
   *
   * @param message the message to log
   */
  public void logMinimal(String message) {
    log.logMinimal(message);
  }

  /**
   * Log the message with arguments at a minimal logging level.
   *
   * @param message the message
   * @param arguments the arguments
   */
  public void logMinimal(String message, Object... arguments) {
    log.logMinimal(message, arguments);
  }

  /**
   * Log the message at a basic logging level.
   *
   * @param message the message
   */
  public void logBasic(String message) {
    log.logBasic(message);
  }

  /**
   * Log the message with arguments at a basic logging level.
   *
   * @param message the message
   * @param arguments the arguments
   */
  public void logBasic(String message, Object... arguments) {
    log.logBasic(message, arguments);
  }

  /**
   * Log the message at a detailed logging level.
   *
   * @param message the message
   */
  public void logDetailed(String message) {
    log.logDetailed(message);
  }

  /**
   * Log the message with arguments at a detailed logging level.
   *
   * @param message the message
   * @param arguments the arguments
   */
  public void logDetailed(String message, Object... arguments) {
    log.logDetailed(message, arguments);
  }

  /**
   * Log the message at a debug logging level.
   *
   * @param message the message
   */
  public void logDebug(String message) {
    log.logDebug(message);
  }

  /**
   * Log the message with arguments at a debug logging level.
   *
   * @param message the message
   * @param arguments the arguments
   */
  public void logDebug(String message, Object... arguments) {
    log.logDebug(message, arguments);
  }

  /**
   * Log the message at a rowlevel logging level.
   *
   * @param message the message
   */
  public void logRowlevel(String message) {
    log.logRowlevel(message);
  }

  /**
   * Log the message with arguments at a rowlevel logging level.
   *
   * @param message the message
   * @param arguments the arguments
   */
  public void logRowlevel(String message, Object... arguments) {
    log.logRowlevel(message, arguments);
  }

  /**
   * Log the message at a error logging level.
   *
   * @param message the message
   */
  public void logError(String message) {
    log.logError(message);
  }

  /**
   * Log the message with the associated Throwable object at a error logging level.
   *
   * @param message the message
   * @param e the e
   */
  public void logError(String message, Throwable e) {
    log.logError(message, e);
  }

  /**
   * Log the message with arguments at a error logging level.
   *
   * @param message the message
   * @param arguments the arguments
   */
  public void logError(String message, Object... arguments) {
    log.logError(message, arguments);
  }

  private void setShellImage(Shell shell) {
    if (transformMeta != null) {
      IPlugin plugin =
          PluginRegistry.getInstance()
              .getPlugin(TransformPluginType.class, transformMeta.getTransform());
      HelpUtils.createHelpButton(shell, plugin);
      String id = plugin.getIds()[0];
      if (!OsHelper.isMac()) {
        if (id != null) {
          shell.setImage(
              GuiResource.getInstance()
                  .getSwtImageTransform(id)
                  .getAsBitmapForSize(shell.getDisplay(), ConstUi.ICON_SIZE, ConstUi.ICON_SIZE));
        }
      }
    }
  }

  /**
   * Adds the connection line for the given parent and previous control, and returns a meta
   * selection manager control
   *
   * @param parent the parent composite object
   * @param previous the previous control
   * @param connection
   * @param lsMod
   * @return the combo box UI component
   */
  public MetaSelectionLine<DatabaseMeta> addConnectionLine(
      Composite parent, Control previous, String connection, ModifyListener lsMod) {
    DatabaseMeta databaseMeta = null;
    if (!Utils.isEmpty(connection)) {
      databaseMeta = pipelineMeta.findDatabase(connection, variables);
      // If we are unable to find the database metadata, display only a warning message so that the
      // user
      // can proceed to correct the issue in the affected pipeline
      if (databaseMeta == null) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
        mb.setMessage(
            BaseMessages.getString(
                PKG,
                "BaseTransformDialog.InvalidConnection.DialogMessage",
                variables.resolve(connection)));
        mb.setText(
            BaseMessages.getString(PKG, "BaseTransformDialog.InvalidConnection.DialogTitle"));
        mb.open();
      }
    }
    return addConnectionLine(parent, previous, databaseMeta, lsMod);
  }

  public interface IFieldsChoiceDialogProvider {
    DialogBoxWithButtons provide(Shell shell, int existingFields, int newFields);
  }

  protected void replaceNameWithBaseFilename(String filename) {
    // Ask to set the name to the base filename...
    //
    MessageBox box = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText("Change name?");
    box.setMessage("Do you want to change the name of the action to match the filename?");
    int answer = box.open();
    if ((answer & SWT.YES) != 0) {
      try {
        String baseName =
            HopVfs.getFileObject(variables.resolve(filename), variables).getName().getBaseName();
        wTransformName.setText(baseName);
      } catch (Exception e) {
        new ErrorDialog(
            shell, "Error", "Error extracting name from filename '" + filename + "'", e);
      }
    }
  }

  /**
   * Builder class for creating standard dialog buttons with consistent styling and behavior. This
   * builder eliminates the repetitive boilerplate of creating OK, Cancel, Preview, Get Fields, and
   * other common buttons found in transform dialogs.
   *
   * <p>The first button added to the builder will automatically become the shell's default button,
   * which is activated when the user presses Enter. This is typically the OK button.
   *
   * <p>Example usage:
   *
   * <pre>
   * // Simple OK + Cancel (OK will be the default button)
   * buildButtonBar(lastControl)
   *   .ok(e -> ok())
   *   .cancel(e -> cancel())
   *   .build();
   *
   * // With Preview (OK is still the default button)
   * buildButtonBar(null)
   *   .ok(e -> ok())
   *   .preview(e -> preview())
   *   .cancel(e -> cancel())
   *   .build();
   *
   * // Complex with multiple buttons (OK is the default button)
   * buildButtonBar(null)
   *   .ok(e -> ok())
   *   .get(e -> getFields())
   *   .create(e -> createTable())
   *   .cancel(e -> cancel())
   *   .build();
   * </pre>
   */
  public static class ButtonBarBuilder {
    private final BaseTransformDialog dialog;
    private final List<Button> buttons = new ArrayList<>();

    ButtonBarBuilder(BaseTransformDialog dialog) {
      this.dialog = dialog;
    }

    /**
     * Adds an OK button with the specified listener.
     *
     * @param listener the listener to invoke when the button is clicked
     * @return this builder for method chaining
     */
    public ButtonBarBuilder ok(Listener listener) {
      dialog.wOk = createButton(BaseMessages.getString(PKG, "System.Button.OK"), listener);
      buttons.add(dialog.wOk);
      return this;
    }

    /**
     * Adds a Preview button with the specified listener.
     *
     * @param listener the listener to invoke when the button is clicked
     * @return this builder for method chaining
     */
    public ButtonBarBuilder preview(Listener listener) {
      dialog.wPreview =
          createButton(BaseMessages.getString(PKG, "System.Button.Preview"), listener);
      buttons.add(dialog.wPreview);
      return this;
    }

    /**
     * Adds a Get Fields button with the specified listener.
     *
     * @param listener the listener to invoke when the button is clicked
     * @return this builder for method chaining
     */
    public ButtonBarBuilder get(Listener listener) {
      dialog.wGet = createButton(BaseMessages.getString(PKG, "System.Button.GetFields"), listener);
      buttons.add(dialog.wGet);
      return this;
    }

    /**
     * Adds a Show SQL button with the specified listener.
     *
     * @param listener the listener to invoke when the button is clicked
     * @return this builder for method chaining
     */
    public ButtonBarBuilder sql(Listener listener) {
      dialog.wSql = createButton(BaseMessages.getString(PKG, "System.Button.SQL"), listener);
      buttons.add(dialog.wSql);
      return this;
    }

    /**
     * Adds a Cancel button with the specified listener.
     *
     * @param listener the listener to invoke when the button is clicked
     * @return this builder for method chaining
     */
    public ButtonBarBuilder cancel(Listener listener) {
      dialog.wCancel = createButton(BaseMessages.getString(PKG, "System.Button.Cancel"), listener);
      buttons.add(dialog.wCancel);
      return this;
    }

    /**
     * Adds a custom button with the specified name and listener.
     *
     * @param buttonName the name the button should get
     * @param listener the listener to invoke when the button is clicked
     * @return the created button
     */
    public ButtonBarBuilder custom(String buttonName, Listener listener) {
      Button button = createButton(buttonName, listener);
      buttons.add(button);
      return this;
    }

    /**
     * Creates a button with the specified name and listener.
     *
     * @param buttonName the name the button should get
     * @param listener the listener to invoke when the button is clicked
     * @return the created button
     */
    private Button createButton(String buttonName, Listener listener) {
      Button button = new Button(dialog.shell, SWT.PUSH);
      button.setText(buttonName);
      if (listener != null) {
        button.addListener(SWT.Selection, listener);
      }
      return button;
    }

    /**
     * Builds and positions the button bar at the bottom of the dialog. This method must be called
     * to finalize the button creation and positioning. The first button added to the builder will
     * automatically be set as the shell's default button (activated by pressing Enter).
     */
    public void build() {
      dialog.setButtonPositions(buttons.toArray(new Button[0]), dialog.margin, null);

      // Set the first button as the default button for the shell
      if (!buttons.isEmpty() && dialog.shell != null) {
        dialog.shell.setDefaultButton(buttons.get(0));
      }
    }
  }

  /**
   * Creates a new button bar builder for adding standard buttons to the dialog. The builder
   * provides a fluent API for creating OK, Cancel, Preview, Get Fields, SQL, and other common
   * buttons with consistent styling and behavior.
   *
   * @return a new ButtonBarBuilder instance
   */
  protected ButtonBarBuilder buildButtonBar() {
    return new ButtonBarBuilder(this);
  }
}

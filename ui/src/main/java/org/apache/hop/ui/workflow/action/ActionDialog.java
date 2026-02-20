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

package org.apache.hop.ui.workflow.action;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * The ActionDialog class is responsible for constructing and opening the settings dialog for the
 * action. Whenever the user opens the action settings in HopGui, it will instantiate the dialog
 * class passing in the IAction object and call the
 *
 * <pre>
 * open()
 * </pre>
 *
 * <p>method on the dialog. SWT is the native windowing environment of HopGui, and it is typically
 * the framework used for implementing action dialogs.
 */
public abstract class ActionDialog extends Dialog implements IActionDialog {
  private static final Class<?> PKG = ITransform.class;

  /** The loggingObject for the dialog */
  public static final ILoggingObject loggingObject =
      new SimpleLoggingObject("Action dialog", LoggingObjectType.ACTION_DIALOG, null);

  /** The Metadata provider */
  @Setter @Getter protected IHopMetadataProvider metadataProvider;

  /** The variables for the action dialogs */
  protected IVariables variables;

  /** The workflow metadata object. */
  @Getter protected WorkflowMeta workflowMeta;

  /** A reference to the properties user interface */
  protected PropsUi props;

  /** A reference to the shell */
  protected Shell shell;

  /** Action name label. Created by {@link #createShell(String)}. */
  protected Label wlName;

  /**
   * Action name text field. Created by {@link #createShell(String)}. Subclasses set its value in
   * getData() and read it in ok(). Override {@link #onActionNameModified()} to react to changes.
   */
  protected Text wName;

  /**
   * Horizontal spacer below the action name line; use for top attachment of dialog content. Created
   * by {@link #createShell(String)}.
   */
  protected Label wSpacer;

  /** Common dialog buttons. Set by {@link #buildButtonBar()}. */
  protected Button wOk;

  protected Button wCancel;

  /**
   * The margin size for form layouts. Initialized by {@link #createShell(String)} to {@code
   * PropsUi.getMargin()}.
   */
  protected int margin;

  /** True while the dialog is loading */
  protected boolean loading;

  /**
   * The middle percentage for form layouts. Initialized by {@link #createShell(String)} to {@code
   * props.getMiddlePct()}.
   */
  protected int middle;

  /**
   * Instantiates a new action dialog.
   *
   * @param parent the parent shell
   * @param workflowMeta the workflow metadata object
   */
  public ActionDialog(Shell parent, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, SWT.NONE);
    this.props = PropsUi.getInstance();
    this.variables = variables;
    this.workflowMeta = workflowMeta;
  }

  public void setActive() {
    loading = false;
    if (shell != null && !shell.isDisposed()) {
      shell.setActive();
    }
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public boolean isDisposed() {
    if (shell != null) {
      return shell.isDisposed();
    }
    return true;
  }

  /**
   * Adds the connection line for the given parent and previous control, and returns a meta
   * selection manager control
   *
   * @param parent the parent composite object
   * @param previous the previous control
   * @param selected The selected database connection
   * @param lsMod changed listener
   * @return the combo box UI component
   */
  public MetaSelectionLine<DatabaseMeta> addConnectionLine(
      Composite parent, Control previous, DatabaseMeta selected, ModifyListener lsMod) {

    final MetaSelectionLine<DatabaseMeta> wConnection =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            DatabaseMeta.class,
            parent,
            SWT.NONE,
            BaseMessages.getString(PKG, "BaseTransformDialog.Connection.Label"),
            BaseMessages.getString(PKG, "BaseTransformDialog.Connection.Tooltip"));
    wConnection.addToConnectionLine(parent, previous, selected, lsMod);
    return wConnection;
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

    DatabaseMeta databaseMeta = getWorkflowMeta().findDatabase(connection, variables);
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
      mb.setText(BaseMessages.getString(PKG, "BaseTransformDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
    return addConnectionLine(parent, previous, databaseMeta, lsMod);
  }

  /**
   * Creates and initializes the shell for an action dialog with standard settings. This method
   * handles the common boilerplate:
   *
   * <ul>
   *   <li>Creates the shell with appropriate style flags (web-safe in Hop Web)
   *   <li>Applies PropsUi look and feel
   *   <li>Applies a FormLayout with standard margins
   *   <li>Sets the shell title
   *   <li>Initializes {@link #middle} and {@link #margin} for layout calculations
   *   <li>Creates a horizontal spacer ({@link #wSpacer}) below the top for content to attach to
   * </ul>
   *
   * <p>Use {@link #createShell(String, IAction)} to also set the shell image from the action.
   *
   * <p>Example usage:
   *
   * <pre>
   * public IAction open() {
   *   createShell(BaseMessages.getString(PKG, "MyActionDialog.Title"), action);
   *   buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
   *
   *   // Add content with top = new FormAttachment(wSpacer, margin)
   *   Control lastControl = wSpacer;
   *   // ... add fields ...
   *
   *   getData();
   *   BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
   *   return action;
   * }
   * </pre>
   *
   * @param title The title for the dialog window
   * @return The spacer ({@link #wSpacer}) to use as the first control for layout chaining
   */
  protected Control createShell(String title) {
    Shell parent = getParent();
    if (OsHelper.isMac()) {
      shell = new Shell(parent.getDisplay(), BaseDialog.getDefaultDialogStyle());
    } else {
      shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    }
    PropsUi.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(title);

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // Action name line
    wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "System.ActionName.Label"));
    wlName.setToolTipText(BaseMessages.getString(PKG, "System.ActionName.Tooltip"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);

    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(
        e -> {
          if (!loading) {
            onActionNameModified();
          }
        });
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(wlName, 0, SWT.CENTER);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    wSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wName, margin);
    fdSpacer.right = new FormAttachment(100, 0);
    wSpacer.setLayoutData(fdSpacer);

    loading = true;
    return wSpacer;
  }

  /**
   * Creates and initializes the shell and sets its image from the given action. Equivalent to
   * {@code createShell(title); setShellImage(action);}.
   *
   * @param title The title for the dialog window
   * @param action The action whose plugin image is used for the shell (may be null)
   * @return The spacer ({@link #wSpacer}) to use as the first control for layout chaining
   */
  protected Control createShell(String title, IAction action) {
    createShell(title);
    setShellImage(action);
    return wSpacer;
  }

  /**
   * Sets the shell image (and optional help button) from the action's plugin. Call after {@link
   * #createShell(String)} when not using {@link #createShell(String, IAction)}.
   *
   * @param action The action whose plugin image is used (may be null)
   */
  protected void setShellImage(IAction action) {
    if (action != null && shell != null && !shell.isDisposed()) {
      WorkflowDialog.setShellImage(shell, action);
    }
  }

  /**
   * Called when the action name field is modified. Override to mark the action as changed (e.g.
   * {@code action.setChanged()}).
   */
  protected void onActionNameModified() {}

  /**
   * Positions the given buttons at the bottom of the dialog. Used by {@link
   * ButtonBarBuilder#build()}.
   *
   * @param buttons the buttons
   * @param margin the margin between buttons
   * @param lastControl the last control above the buttons (e.g. content area), or null to attach to
   *     bottom
   */
  protected void setButtonPositions(Button[] buttons, int margin, Control lastControl) {
    BaseTransformDialog.positionBottomButtons(shell, buttons, margin, lastControl);
  }

  /**
   * Focuses the action name field and selects its text. Call before {@link
   * org.apache.hop.ui.core.dialog.BaseDialog#defaultShellHandling} so the user can immediately edit
   * the name.
   */
  protected void focusActionName() {
    if (wName != null && !wName.isDisposed()) {
      wName.selectAll();
      wName.setFocus();
    }
    loading = false;
  }

  /**
   * Creates a new button bar builder for adding OK, Cancel, and other buttons with consistent
   * positioning and default margin. Call {@link ButtonBarBuilder#build()} to position the buttons.
   *
   * <p>Example:
   *
   * <pre>
   * buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();
   * </pre>
   *
   * @return a new ButtonBarBuilder instance
   */
  protected ButtonBarBuilder buildButtonBar() {
    return new ButtonBarBuilder(this);
  }

  /**
   * Builder for the dialog button bar. Use {@link ActionDialog#buildButtonBar()} to obtain an
   * instance.
   */
  public static class ButtonBarBuilder {
    private final ActionDialog dialog;
    private final List<Button> buttons = new ArrayList<>();

    ButtonBarBuilder(ActionDialog dialog) {
      this.dialog = dialog;
    }

    /** Adds an OK button. */
    public ButtonBarBuilder ok(Listener listener) {
      dialog.wOk = createButton(BaseMessages.getString(PKG, "System.Button.OK"), listener);
      buttons.add(dialog.wOk);
      return this;
    }

    /** Adds a Cancel button. */
    public ButtonBarBuilder cancel(Listener listener) {
      dialog.wCancel = createButton(BaseMessages.getString(PKG, "System.Button.Cancel"), listener);
      buttons.add(dialog.wCancel);
      return this;
    }

    /** Adds a custom button. */
    public ButtonBarBuilder custom(String buttonName, Listener listener) {
      Button button = createButton(buttonName, listener);
      buttons.add(button);
      return this;
    }

    private Button createButton(String buttonName, Listener listener) {
      Button button = new Button(dialog.shell, SWT.PUSH);
      button.setText(buttonName);
      if (listener != null) {
        button.addListener(SWT.Selection, listener);
      }
      return button;
    }

    /**
     * Positions the buttons at the bottom of the dialog using the dialog's default margin and sets
     * the first button as the shell default button.
     */
    public void build() {
      build(null);
    }

    /**
     * Positions the buttons at the bottom of the dialog, above the given control when non-null.
     *
     * @param lastControl the last control above the buttons (content area), or null to attach to
     *     shell bottom
     */
    public void build(Control lastControl) {
      dialog.setButtonPositions(buttons.toArray(new Button[0]), dialog.margin, lastControl);

      if (!buttons.isEmpty() && dialog.shell != null) {
        dialog.shell.setDefaultButton(buttons.get(0));
      }
    }
  }
}

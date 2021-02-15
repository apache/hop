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
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Allows the user to make a selection from a list of values.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class EnterSelectionDialog extends Dialog {
  private static final Class<?> PKG = EnterSelectionDialog.class; // For Translator

  private Label wlSelection;
  private List wSelection;
  private FormData fdlSelection, fdSelection;
  private TextVar wConstantValue;
  private Button wbUseConstant;

  private Button wOk, wCancel;
  private Listener lsOk, lsCancel;

  private Shell shell;

  public Shell getShell() {
    return shell;
  }

  private SelectionAdapter lsDef;

  private String[] choices;
  private String selection;
  private int selectionNr;
  private int shellHeight;
  private int shellWidth;
  private String shellText;
  private String lineText;
  private PropsUi props;
  private String constant;
  private IVariables variables;
  private String currentValue;

  private boolean viewOnly, modal;
  private int[] selectedNrs;
  private boolean multi;
  private int[] indices;
  private boolean fixed;
  private boolean quickSearch;

  private ToolItem goSearch, wfilter, addConnection;
  private ToolItem wbRegex;

  private String filterString = null;
  private Pattern pattern = null;
  private Text searchText = null;
  private boolean addNoneOption;
  private boolean noneClicked;

  /**
   * Create a new dialog allow someone to pick one value out of a list of values
   *
   * @param parent the parent shell.
   * @param choices The available list of options
   * @param shellText The shell text
   * @param message the message to display as extra information about the possible choices
   */
  public EnterSelectionDialog(Shell parent, String[] choices, String shellText, String message) {
    super(parent, SWT.NONE);

    this.choices = choices;
    this.shellText = shellText;
    this.lineText = message;

    props = PropsUi.getInstance();
    selection = null;
    viewOnly = false;
    modal = true;
    selectedNrs = new int[] {};
    multi = false;
    fixed = false;
    quickSearch = true;
  }

  public EnterSelectionDialog(
      Shell parent,
      String[] choices,
      String shellText,
      String message,
      int shellWidth,
      int shellHeight) {
    this(parent, choices, shellText, message);
    this.shellWidth = shellWidth;
    this.shellHeight = shellHeight;
  }

  public EnterSelectionDialog(
      Shell parent,
      String[] choices,
      String shellText,
      String message,
      String constant,
      IVariables variables) {
    this(parent, choices, shellText, message);
    this.constant = constant;
    this.variables = variables;
  }

  public void setViewOnly() {
    viewOnly = true;
  }

  public void setAvoidQuickSearch() {
    quickSearch = false;
  }

  public void setCurrentValue(String currentValue) {
    this.currentValue = currentValue;
  }

  public void clearModal() {
    modal = false;
  }

  public String open(int nr) {
    selectedNrs = new int[] {nr};
    return open();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM
                | (modal ? SWT.APPLICATION_MODAL | SWT.SHEET : SWT.NONE)
                | SWT.RESIZE
                | SWT.MIN
                | SWT.MAX);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout(formLayout);
    shell.setText(shellText);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    int margin = props.getMargin();

    if (quickSearch) {
      ToolBar treeTb = new ToolBar(shell, SWT.HORIZONTAL | SWT.FLAT);
      props.setLook(treeTb);

      wfilter = new ToolItem(treeTb, SWT.SEPARATOR);
      searchText = new Text(treeTb, SWT.SEARCH | SWT.CANCEL);
      props.setLook(searchText);
      searchText.setToolTipText(
          BaseMessages.getString(PKG, "EnterSelectionDialog.FilterString.ToolTip"));
      wfilter.setControl(searchText);
      wfilter.setWidth(120);

      wbRegex = new ToolItem(treeTb, SWT.CHECK);
      wbRegex.setImage(GuiResource.getInstance().getImageRegex());
      wbRegex.setToolTipText(BaseMessages.getString(PKG, "EnterSelectionDialog.useRegEx.Tooltip"));

      goSearch = new ToolItem(treeTb, SWT.PUSH);
      goSearch.setImage(GuiResource.getInstance().getImageRefresh());
      goSearch.setToolTipText(BaseMessages.getString(PKG, "EnterSelectionDialog.refresh.Label"));

      goSearch.addSelectionListener(
          new SelectionAdapter() {
            public void widgetSelected(SelectionEvent event) {
              updateFilter();
            }
          });

      FormData fd = new FormData();
      fd.right = new FormAttachment(100);
      fd.top = new FormAttachment(0, 0);
      treeTb.setLayoutData(fd);

      Label wlFilter = new Label(shell, SWT.RIGHT);
      props.setLook(wlFilter);
      wlFilter.setText(BaseMessages.getString(PKG, "EnterSelectionDialog.FilterString.Label"));
      FormData fdlFilter = new FormData();
      fdlFilter.top = new FormAttachment(0, 5);
      fdlFilter.right = new FormAttachment(treeTb, -5);
      wlFilter.setLayoutData(fdlFilter);

      searchText.addSelectionListener(
          new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
              updateFilter();
            }
          });

      // From transform line
      wlSelection = new Label(shell, SWT.NONE);
      wlSelection.setText(lineText);
      props.setLook(wlSelection);
      fdlSelection = new FormData();
      fdlSelection.left = new FormAttachment(0, 0);
      fdlSelection.top = new FormAttachment(treeTb, 10);
      wlSelection.setLayoutData(fdlSelection);
    } else {
      // From transform line
      wlSelection = new Label(shell, SWT.NONE);
      wlSelection.setText(lineText);
      props.setLook(wlSelection);
      fdlSelection = new FormData();
      fdlSelection.left = new FormAttachment(0, 0);
      wlSelection.setLayoutData(fdlSelection);
    }

    int options = SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL;
    if (multi) {
      options |= SWT.MULTI;
    } else {
      options |= SWT.SINGLE;
    }

    wSelection = new List(shell, options);
    for (int i = 0; i < choices.length; i++) {
      wSelection.add(choices[i]);
    }
    if (selectedNrs != null) {
      wSelection.select(selectedNrs);
      wSelection.showSelection();
    }
    if (fixed) {
      props.setLook(wSelection, Props.WIDGET_STYLE_FIXED);
    } else {
      props.setLook(wSelection);
    }

    ArrayList<Button> buttons = new ArrayList<>();

    // Some buttons
    wOk = new Button(shell, SWT.PUSH);
    if (viewOnly) {
      wOk.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    } else {
      wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    }
    wOk.addListener(SWT.Selection, e->ok());
    buttons.add(wOk);

    if (addNoneOption) {
      Button wNone = new Button(shell, SWT.PUSH);
      wNone.setText(BaseMessages.getString(PKG, "EnterSelectionDialog.Button.None.Label"));
      wNone.addListener(SWT.Selection, e->{
        noneClicked=true;
        cancel();
      });
      buttons.add(wNone);
    }

    if (!viewOnly) {
      wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      wCancel.addListener(SWT.Selection, e -> cancel());
      buttons.add(wCancel);
    }

    BaseTransformDialog.positionBottomButtons( shell, buttons.toArray( new Button[ 0 ] ), margin, null);

    Control nextControl = wOk;

    if (constant != null) {
      wConstantValue = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      if (!Utils.isEmpty(constant)) {
        wConstantValue.setText(constant);
      }
      props.setLook(wConstantValue);
      FormData fdConstantValue = new FormData();
      fdConstantValue.left = new FormAttachment(0, 0);
      fdConstantValue.bottom = new FormAttachment(wOk, -10);
      fdConstantValue.right = new FormAttachment(100, 0);
      wConstantValue.setLayoutData(fdConstantValue);

      wbUseConstant = new Button(shell, SWT.CHECK);
      props.setLook(wbUseConstant);
      wbUseConstant.setText(BaseMessages.getString(PKG, "EnterSelectionDialog.UseConstant.Label"));
      wbUseConstant.setSelection(!Utils.isEmpty(constant));
      nextControl = wbUseConstant;
      FormData fdUseConstant = new FormData();
      fdUseConstant.left = new FormAttachment(0, 0);
      fdUseConstant.bottom = new FormAttachment(wConstantValue, -5);
      wbUseConstant.setLayoutData(fdUseConstant);
      wbUseConstant.addSelectionListener(
          new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent selectionEvent) {
              super.widgetSelected(selectionEvent);
              setActive();
            }
          });

      setActive();
    }

    fdSelection = new FormData();
    fdSelection.left = new FormAttachment(0, 0);
    fdSelection.right = new FormAttachment(100, 0);
    fdSelection.top = new FormAttachment(wlSelection, 5);
    fdSelection.bottom = new FormAttachment(nextControl, -10);
    wSelection.setLayoutData(fdSelection);

    // Add listeners

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wSelection.addSelectionListener(lsDef);
    wSelection.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent e) {
            if (e.character == SWT.CR) {
              ok();
            }
          }
        });
    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    if (shellWidth == 0 || shellHeight == 0) {
      BaseTransformDialog.setSize(shell);
    } else {
      shell.setSize(shellWidth, shellHeight);
    }

    wOk.setFocus();

    shell.open();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return selection;
  }

  private void setActive() {
    wSelection.setEnabled(!wbUseConstant.getSelection());
    wConstantValue.setEnabled(wbUseConstant.getSelection());
  }

  public String openRepoDialog() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM
                | (modal ? SWT.APPLICATION_MODAL | SWT.SHEET : SWT.NONE)
                | SWT.MIN
                | SWT.MAX);
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(shellText);
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    wlSelection = new Label(shell, SWT.NONE);
    wlSelection.setText(lineText);
    props.setLook(wlSelection);
    fdlSelection = new FormData();
    fdlSelection.left = new FormAttachment(0, 10);
    fdlSelection.top = new FormAttachment(0, 10);
    wlSelection.setLayoutData(fdlSelection);

    int options = SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL;

    wSelection = new List(shell, options);
    for (int i = 0; i < choices.length; i++) {
      wSelection.add(choices[i]);
    }

    Label separator = new Label(shell, SWT.SEPARATOR | SWT.HORIZONTAL);
    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment(wSelection, 35);
    fdSeparator.right = new FormAttachment(100, -10);
    fdSeparator.left = new FormAttachment(0, 10);
    separator.setLayoutData(fdSeparator);

    Button btnHelp = new Button(shell, SWT.PUSH);
    btnHelp.setImage(GuiResource.getInstance().getImageHelpWeb());
    FormData fd_btnHelp = new FormData();
    fd_btnHelp.top = new FormAttachment(separator, 12);
    fd_btnHelp.left = new FormAttachment(0, 10);
    fd_btnHelp.bottom = new FormAttachment(100, -10);
    fd_btnHelp.width = (Const.isOSX() ? 85 : 75);
    btnHelp.setLayoutData(fd_btnHelp);
    btnHelp.setText(BaseMessages.getString(PKG, "System.Button.Help"));
    btnHelp.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            HelpUtils.openHelpDialog(
                shell,
                BaseMessages.getString(PKG, "EnterSelectionDialog.Help.Title"),
                Const.getDocUrl(BaseMessages.getString(PKG, "EnterSelectionDialog.Help")),
                BaseMessages.getString(PKG, "EnterSelectionDialog.Help.Header"));
          }
        });


    fdSelection = new FormData();
    fdSelection.left = new FormAttachment(0, 10);
    fdSelection.right = new FormAttachment(100, -10);
    fdSelection.top = new FormAttachment(wlSelection, 10);
    fdSelection.bottom = new FormAttachment(separator, -12);
    wSelection.setLayoutData(fdSelection);

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };
    wSelection.addSelectionListener(lsDef);
    wSelection.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent e) {
            if (e.character == SWT.CR) {
              ok();
            }
          }
        });

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    BaseTransformDialog.setSize(shell);

    wOk.setFocus();
    shell.pack();
    shell.open();

    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return selection;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {}

  private void cancel() {
    selection = null;
    dispose();
  }

  private void ok() {
    if (constant != null && wbUseConstant.getSelection()) {
      selection = wConstantValue.getText();
    } else if (wSelection.getSelectionCount() > 0) {
      selection = wSelection.getSelection()[0];
      selectionNr = wSelection.getSelectionIndices()[0];
      if (quickSearch) {
        for (int i = 0; i < choices.length; i++) {
          if (choices[i].equals(selection)) {
            selectionNr = i;
          }
        }
      }
      // We need to handle the indices properly. If a filter is applied, the wSelection will differ
      // from choices
      // So we have to get the current index from choices and store it in the indices
      String[] selections = wSelection.getSelection();
      boolean found = false;
      indices = new int[selections.length];
      for (int i = 0; i < selections.length; i++) {
        found = false;
        for (int j = 0; j < choices.length; j++) {
          if (selections[i].equals(choices[j])) {
            indices[i] = j;
            found = true;
            break;
          }
        }
      }
      if (!found) {
        indices = wSelection.getSelectionIndices();
      }
    } else {
      selection = null;
      selectionNr = -1;
      indices = new int[0];
    }
    dispose();
  }

  public int getSelectionNr(String str) {
    for (int i = 0; i < choices.length; i++) {
      if (choices[i].equalsIgnoreCase(str)) {
        return i;
      }
    }
    return -1;
  }

  public int getSelectionNr() {
    return selectionNr;
  }

  public boolean isMulti() {
    return multi;
  }

  public void setMulti(boolean multi) {
    this.multi = multi;
  }

  public int[] getSelectionIndeces() {
    return indices;
  }

  /** @return the fixed */
  public boolean isFixed() {
    return fixed;
  }

  /** @param fixed the fixed to set */
  public void setFixed(boolean fixed) {
    this.fixed = fixed;
  }

  /** @return the selectedNrs */
  public int[] getSelectedNrs() {
    return selectedNrs;
  }

  /** @param selectedNrs the selectedNrs to set */
  public void setSelectedNrs(int[] selectedNrs) {
    this.selectedNrs = selectedNrs;
  }

  /** @param selectedNrs the selectedNrs to set */
  public void setSelectedNrs(java.util.List<Integer> selectedNrs) {
    this.selectedNrs = new int[selectedNrs.size()];
    for (int i = 0; i < selectedNrs.size(); i++) {
      this.selectedNrs[i] = selectedNrs.get(i);
    }
  }

  protected void updateFilter() {
    pattern = null;
    filterString = null;
    if (searchText != null && !searchText.isDisposed() && !Utils.isEmpty(searchText.getText())) {
      if (wbRegex.getSelection()) {
        pattern = Pattern.compile(searchText.getText());
      } else {
        filterString = searchText.getText().toUpperCase();
      }
    }
    refresh();
  }

  private void refresh() {
    wSelection.removeAll();

    for (int i = 0; i < choices.length; i++) {
      if (quickSearch) {
        if (wbRegex.getSelection()) {
          // use regex
          if (pattern != null) {
            Matcher matcher = pattern.matcher(choices[i]);
            if (matcher.matches()) {
              wSelection.add(choices[i]);
            }
          } else {
            wSelection.add(choices[i]);
          }
        } else {
          if (filterString != null) {
            if (choices[i].toUpperCase().contains(filterString)) {
              wSelection.add(choices[i]);
            }
          } else {
            wSelection.add(choices[i]);
          }
        }
      } else {
        wSelection.add(choices[i]);
      }
    }
    wSelection.redraw();
    /*
     * selectedNrs = new int[] {}; if (selectedNrs!=null){ wSelection.select(selectedNrs); wSelection.showSelection(); }
     */
  }

  /**
   * Gets addNoneOption
   *
   * @return value of addNoneOption
   */
  public boolean isAddNoneOption() {
    return addNoneOption;
  }

  /**
   * @param addNoneOption The addNoneOption to set
   */
  public void setAddNoneOption( boolean addNoneOption ) {
    this.addNoneOption = addNoneOption;
  }

  /**
   * Gets noneClicked
   *
   * @return value of noneClicked
   */
  public boolean isNoneClicked() {
    return noneClicked;
  }

  /**
   * @param noneClicked The noneClicked to set
   */
  public void setNoneClicked( boolean noneClicked ) {
    this.noneClicked = noneClicked;
  }
}

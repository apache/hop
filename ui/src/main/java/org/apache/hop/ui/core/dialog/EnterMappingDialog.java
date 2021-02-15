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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.util.Utils;
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
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * Shows a user 2 lists of strings and allows the linkage of values between values in the 2 lists
 *
 * @author Matt
 * @since 23-03-2006
 */
public class EnterMappingDialog extends Dialog {
  private static final Class<?> PKG = EnterMappingDialog.class; // For Translator

  public class GuessPair {
    private int _srcIndex = -1;
    private int _targetIndex = -1;
    private boolean _found = false;

    public GuessPair(int src) {
      _srcIndex = src;
      _found = false;
    }

    public int getTargetIndex() {
      return _targetIndex;
    }

    public void setTargetIndex(int targetIndex) {
      _found = true;
      _targetIndex = targetIndex;
    }

    public int getSrcIndex() {
      return _srcIndex;
    }

    public void setSrcIndex(int srcIndex) {
      _srcIndex = srcIndex;
    }

    public boolean getFound() {
      return _found;
    }
  }

  private List wSource;

  private List wTarget;

  private Button wSourceHide;
  private Button wTargetHide;
  private Button wSourceAuto;
  private Button wTargetAuto;

  private List wResult;

  private Shell shell;

  private String[] sourceList;

  private String[] targetList;

  private PropsUi props;

  private String sourceSeparator;
  private String targetSeparator;

  private java.util.List<SourceToTargetMapping> mappings;

  /**
   * Create a new dialog allowing the user to enter a mapping
   *
   * @param parent the parent shell
   * @param source the source values
   * @param target the target values
   */
  public EnterMappingDialog(Shell parent, String[] source, String[] target) {
    this(parent, source, target, new ArrayList<>());
  }

  /**
   * Create a new dialog allowing the user to enter a mapping
   *
   * @param parent the parent shell
   * @param source the source values
   * @param target the target values
   * @param mappings the already selected mappings (ArrayList containing <code>SourceToTargetMapping
   *     </code>s)
   */
  public EnterMappingDialog(
      Shell parent,
      String[] source,
      String[] target,
      java.util.List<SourceToTargetMapping> mappings) {
    super(parent, SWT.NONE);
    props = PropsUi.getInstance();
    this.sourceList = source;
    this.targetList = target;

    this.mappings = mappings;
  }

  public java.util.List<SourceToTargetMapping> open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell =
        new Shell(
            parent,
            SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX | SWT.APPLICATION_MODAL | SWT.SHEET);
    props.setLook(shell);

    shell.setImage(GuiResource.getInstance().getImageHopUi());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "EnterMappingDialog.Title"));
    shell.setImage(GuiResource.getInstance().getImagePipeline());

    int margin = props.getMargin();
    int buttonSpace = 90;

    // Some buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wGuess = new Button(shell, SWT.PUSH);
    wGuess.setText(BaseMessages.getString(PKG, "EnterMappingDialog.Button.Guess"));
    wGuess.addListener(SWT.Selection, e -> guess());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wGuess, wCancel}, margin, null);

    // Hide used source fields?
    wSourceHide = new Button(shell, SWT.CHECK);
    wSourceHide.setSelection(true);
    wSourceHide.setText(BaseMessages.getString(PKG, "EnterMappingDialog.HideUsedSources"));
    props.setLook(wSourceHide);
    FormData fdSourceHide = new FormData();
    fdSourceHide.left = new FormAttachment(0, 0);
    fdSourceHide.right = new FormAttachment(25, 0);
    fdSourceHide.bottom = new FormAttachment(wOk, -2 * margin);
    wSourceHide.setLayoutData(fdSourceHide);
    wSourceHide.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            refreshMappings();
          }
        });

    // Hide used target fields?
    wTargetHide = new Button(shell, SWT.CHECK);
    wTargetHide.setText(BaseMessages.getString(PKG, "EnterMappingDialog.HideUsedTargets"));
    wTargetHide.setSelection(true);
    props.setLook(wTargetHide);
    FormData fdTargetHide = new FormData();
    fdTargetHide.left = new FormAttachment(25, margin * 2);
    fdTargetHide.right = new FormAttachment(50, 0);
    fdTargetHide.bottom = new FormAttachment(wOk, -2 * margin);
    wTargetHide.setLayoutData(fdTargetHide);
    wTargetHide.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            refreshMappings();
          }
        });

    // Automatic source selection
    wSourceAuto = new Button(shell, SWT.CHECK);
    wSourceAuto.setText(
        BaseMessages.getString(PKG, "EnterMappingDialog.AutoTargetSelection.Label"));
    wSourceAuto.setSelection(true);
    props.setLook(wSourceAuto);
    FormData fdSourceAuto = new FormData();
    fdSourceAuto.left = new FormAttachment(0, 0);
    fdSourceAuto.right = new FormAttachment(25, 0);
    fdSourceAuto.bottom = new FormAttachment(wSourceHide, -2 * margin);
    wSourceAuto.setLayoutData(fdSourceAuto);

    // Automatic target selection
    wTargetAuto = new Button(shell, SWT.CHECK);
    wTargetAuto.setText(
        BaseMessages.getString(PKG, "EnterMappingDialog.AutoSourceSelection.Label"));
    wTargetAuto.setSelection(false);
    props.setLook(wTargetAuto);
    FormData fdTargetAuto = new FormData();
    fdTargetAuto.left = new FormAttachment(25, margin * 2);
    fdTargetAuto.right = new FormAttachment(50, 0);
    fdTargetAuto.bottom = new FormAttachment(wTargetHide, -2 * margin);
    wTargetAuto.setLayoutData(fdTargetAuto);

    // Source table
    //
    Label wlSource = new Label(shell, SWT.NONE);
    wlSource.setText(BaseMessages.getString(PKG, "EnterMappingDialog.SourceFields.Label"));
    props.setLook(wlSource);
    FormData fdlSource = new FormData();
    fdlSource.left = new FormAttachment(0, 0);
    fdlSource.top = new FormAttachment(0, margin);
    wlSource.setLayoutData(fdlSource);
    wSource = new List(shell, SWT.SINGLE | SWT.RIGHT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    for (int i = 0; i < sourceList.length; i++) {
      wSource.add(sourceList[i]);
    }
    props.setLook(wSource);
    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment(0, 0);
    fdSource.right = new FormAttachment(25, 0);
    fdSource.top = new FormAttachment(wlSource, margin);
    fdSource.bottom = new FormAttachment(wSourceAuto, -2 * margin);
    wSource.setLayoutData(fdSource);

    // Target table
    Label wlTarget = new Label(shell, SWT.NONE);
    wlTarget.setText(BaseMessages.getString(PKG, "EnterMappingDialog.TargetFields.Label"));
    props.setLook(wlTarget);
    FormData fdlTarget = new FormData();
    fdlTarget.left = new FormAttachment(wSource, margin * 2);
    fdlTarget.top = new FormAttachment(0, margin);
    wlTarget.setLayoutData(fdlTarget);
    wTarget = new List(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    for (int i = 0; i < targetList.length; i++) {
      wTarget.add(targetList[i]);
    }
    props.setLook(wTarget);
    FormData fdTarget = new FormData();
    fdTarget.left = new FormAttachment(wSource, margin * 2);
    fdTarget.right = new FormAttachment(50, 0);
    fdTarget.top = new FormAttachment(wlTarget, margin);
    fdTarget.bottom = new FormAttachment(wTargetAuto, -2 * margin);
    wTarget.setLayoutData(fdTarget);

    // Delete mapping button
    Button wDelete = new Button(shell, SWT.PUSH);
    FormData fdDelete = new FormData();
    wDelete.setText(BaseMessages.getString(PKG, "EnterMappingDialog.Button.Delete"));
    fdDelete.left = new FormAttachment(wTarget, margin * 2);
    fdDelete.top = new FormAttachment(wTarget, 0, SWT.CENTER);
    wDelete.setLayoutData(fdDelete);
    wDelete.addListener(SWT.Selection, e -> delete());

    // Add mapping button:
    Button wAdd = new Button(shell, SWT.PUSH);
    FormData fdAdd = new FormData();
    wAdd.setText(BaseMessages.getString(PKG, "EnterMappingDialog.Button.Add"));
    fdAdd.left = new FormAttachment(wTarget, margin * 2);
    fdAdd.right = new FormAttachment(wDelete, 0, SWT.RIGHT);
    fdAdd.bottom = new FormAttachment(wDelete, -2 * margin);
    wAdd.setLayoutData(fdAdd);
    wAdd.addListener(SWT.Selection, e -> add());

    // Result table
    Label wlResult = new Label(shell, SWT.NONE);
    wlResult.setText(BaseMessages.getString(PKG, "EnterMappingDialog.ResultMappings.Label"));
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(wDelete, margin * 2);
    fdlResult.top = new FormAttachment(0, margin);
    wlResult.setLayoutData(fdlResult);
    wResult = new List(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    for (int i = 0; i < targetList.length; i++) {
      wResult.add(targetList[i]);
    }
    props.setLook(wResult);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(wDelete, margin * 2);
    fdResult.right = new FormAttachment(100, 0);
    fdResult.top = new FormAttachment(wlResult, margin);
    fdResult.bottom = new FormAttachment(wOk, -50);
    wResult.setLayoutData(fdResult);

    wSource.addListener(
        SWT.Selection,
        event -> {
          if (wSourceAuto.getSelection()) {
            findTarget();
          }
        });
    wSource.addListener(SWT.DefaultSelection, event -> add());

    wTarget.addListener(
        SWT.Selection,
        event -> {
          if (wTargetAuto.getSelection()) {
            findSource();
          }
        });
    wTarget.addListener(SWT.DefaultSelection, event -> add());

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          @Override
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return mappings;
  }

  private void guess() {
    // Guess the target for all the sources...
    String[] sortedSourceList = Arrays.copyOf(sourceList, sourceList.length);

    // Sort Longest to Shortest string - makes matching better
    Arrays.sort(sortedSourceList, (s1, s2) -> s2.length() - s1.length());
    // Look for matches using longest field name to shortest
    ArrayList<GuessPair> pList = new ArrayList<>();
    for (int i = 0; i < sourceList.length; i++) {
      int idx = Const.indexOfString(sortedSourceList[i], wSource.getItems());
      if (idx >= 0) {
        pList.add(findTargetPair(idx));
      }
    }
    // Now add them in order or source field list
    Collections.sort(pList, (s1, s2) -> s1.getSrcIndex() - s2.getSrcIndex());
    for (GuessPair p : pList) {
      if (p.getFound()) {
        SourceToTargetMapping mapping =
            new SourceToTargetMapping(p.getSrcIndex(), p.getTargetIndex());
        mappings.add(mapping);
      }
    }
    refreshMappings();
  }

  private void findTarget() {
    int sourceIndex = wSource.getSelectionIndex();
    GuessPair p = findTargetPair(sourceIndex);
    if (p.getFound()) {
      wTarget.setSelection(p.getTargetIndex());
    }
  }

  private GuessPair findTargetPair(int sourceIndex) {
    // Guess, user selects an entry in the list on the left.
    // Find a comparable entry in the target list...
    GuessPair result = new GuessPair(sourceIndex);

    if (sourceIndex < 0) {
      return result; // Not Found
    }

    // Skip everything after the bracket...
    String sourceString = wSource.getItem(sourceIndex).toUpperCase();
    String sourceValue = sourceString.toLowerCase();
    if (StringUtils.isNotEmpty(sourceSeparator)) {
      int index = sourceValue.indexOf(sourceSeparator);
      if (index >= 0) {
        sourceValue = sourceValue.substring(index + sourceSeparator.length());
      }
    }

    int minDistance = Integer.MAX_VALUE;
    int minTarget = -1;

    for (int i = 0; i < wTarget.getItemCount(); i++) {
      String targetString = wTarget.getItem(i);
      String targetValue = targetString.toLowerCase();
      // Only consider the part after the first target separator...
      //
      if (StringUtils.isNotEmpty(targetSeparator)) {
        int index = targetValue.indexOf(targetSeparator);
        if (index >= 0) {
          targetValue = targetValue.substring(index + targetSeparator.length());
        }
      }

      // Compare source and target values...
      //
      if (sourceValue.equals( targetValue )) {
        minDistance=0;
        minTarget=i;
        break; // we found an exact match
      }

      // Compare sourceValue and targetValue using a distance
      //
      int distance =
          Utils.getDamerauLevenshteinDistance(sourceValue.toLowerCase(), targetValue.toLowerCase());
      if (distance < minDistance) {
        minDistance = distance;
        minTarget = i;
      }
    }

    if (minTarget >= 0) {
      result.setTargetIndex( minTarget );
      result._found = true; // always make a guess
    }

    return result;
  }

  private boolean findSource() {
    // Guess, user selects an entry in the list on the right.
    // Find a comparable entry in the source list...
    boolean found = false;

    int targetIndex = wTarget.getSelectionIndex();
    // Skip everything after the bracket...
    String targetString = wTarget.getItem(targetIndex).toUpperCase();

    int length = targetString.length();
    boolean first = true;

    while (!found && (length >= 2 || first)) {
      first = false;

      for (int i = 0; i < wSource.getItemCount() && !found; i++) {
        if (wSource.getItem(i).toUpperCase().indexOf(targetString.substring(0, length)) >= 0) {
          wSource.setSelection(i);
          found = true;
        }
      }
      length--;
    }
    return found;
  }

  private void add() {
    if (wSource.getSelectionCount() == 1 && wTarget.getSelectionCount() == 1) {
      String sourceString = wSource.getSelection()[0];
      String targetString = wTarget.getSelection()[0];

      int srcIndex = Const.indexOfString(sourceString, sourceList);
      int tgtIndex = Const.indexOfString(targetString, targetList);

      if (srcIndex >= 0 && tgtIndex >= 0) {
        // New mapping: add it to the list...
        SourceToTargetMapping mapping = new SourceToTargetMapping(srcIndex, tgtIndex);
        mappings.add(mapping);

        refreshMappings();
      }
    }
  }

  private void refreshMappings() {
    // Refresh the results...
    wResult.removeAll();

    // Sort the mappings by result string
    //
    Collections.sort(mappings, Comparator.comparing(this::getMappingResultString));

    for (int i = 0; i < mappings.size(); i++) {
      SourceToTargetMapping mapping = mappings.get(i);
      String mappingString = getMappingResultString(mapping);
      wResult.add(mappingString);
    }

    wSource.removeAll();
    // Refresh the sources
    for (int a = 0; a < sourceList.length; a++) {
      boolean found = false;
      if (wSourceHide.getSelection()) {
        for (int b = 0; b < mappings.size() && !found; b++) {
          SourceToTargetMapping mapping = mappings.get(b);
          if (mapping.getSourcePosition() == Const.indexOfString(sourceList[a], sourceList)) {
            found = true;
          }
        }
      }

      if (!found) {
        wSource.add(sourceList[a]);
      }
    }

    wTarget.removeAll();
    // Refresh the targets
    for (int a = 0; a < targetList.length; a++) {
      boolean found = false;
      if (wTargetHide.getSelection()) {
        for (int b = 0; b < mappings.size() && !found; b++) {
          SourceToTargetMapping mapping = mappings.get(b);
          if (mapping.getTargetPosition() == Const.indexOfString(targetList[a], targetList)) {
            found = true;
          }
        }
      }

      if (!found) {
        wTarget.add(targetList[a]);
      }
    }
  }

  private String getMappingResultString(SourceToTargetMapping mapping) {
    return sourceList[mapping.getSourcePosition()]
        + " --> "
        + targetList[mapping.getTargetPosition()];
  }

  private void delete() {
    String[] result = wResult.getSelection();
    for (int i = result.length - 1; i >= 0; i--) {
      int idx = wResult.indexOf(result[i]);
      if (idx >= 0 && idx < mappings.size()) {
        mappings.remove(idx);
      }
    }
    refreshMappings();
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void getData() {
    refreshMappings();
  }

  private void cancel() {
    mappings = null;
    dispose();
  }

  private void ok() {
    dispose();
  }

  /**
   * Gets sourceSeparator
   *
   * @return value of sourceSeparator
   */
  public String getSourceSeparator() {
    return sourceSeparator;
  }

  /** @param sourceSeparator The sourceSeparator to set */
  public void setSourceSeparator(String sourceSeparator) {
    this.sourceSeparator = sourceSeparator;
  }

  /**
   * Gets targetSeparator
   *
   * @return value of targetSeparator
   */
  public String getTargetSeparator() {
    return targetSeparator;
  }

  /** @param targetSeparator The targetSeparator to set */
  public void setTargetSeparator(String targetSeparator) {
    this.targetSeparator = targetSeparator;
  }
}

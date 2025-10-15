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

package org.apache.hop.ui.workflow.actions.start;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ActionStartDialog extends ActionDialog {
  private static final Class<?> PKG = ActionStart.class;

  private static final String NO_SCHEDULING =
      BaseMessages.getString(PKG, "ActionStart.Type.NoScheduling");

  private static final String INTERVAL = BaseMessages.getString(PKG, "ActionStart.Type.Interval");

  private static final String DAILY = BaseMessages.getString(PKG, "ActionStart.Type.Daily");

  private static final String WEEKLY = BaseMessages.getString(PKG, "ActionStart.Type.Weekly");

  private static final String MONTHLY = BaseMessages.getString(PKG, "ActionStart.Type.Monthly");

  private ActionStart action;

  private Group gRepeat;
  private Text wName;
  private Button wRepeat;
  private TextVar wIntervalSeconds;
  private TextVar wIntervalMinutes;
  private ComboVar wType;
  private TextVar wHour;
  private TextVar wMinutes;
  private ComboVar wDayOfWeek;
  private TextVar wDayOfMonth;
  private Button wDoNotWaitOnFirstExecution;

  public ActionStartDialog(
      Shell parent, ActionStart action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionStart.Name"));

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    // Some buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionStart.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    Label wlRepeat = new Label(shell, SWT.RIGHT);
    wlRepeat.setText(BaseMessages.getString(PKG, "ActionStart.Repeat.Label"));
    PropsUi.setLook(wlRepeat);
    FormData fdlRepeat = new FormData();
    fdlRepeat.left = new FormAttachment(0, 0);
    fdlRepeat.right = new FormAttachment(middle, -margin);
    fdlRepeat.top = new FormAttachment(wName, margin);
    wlRepeat.setLayoutData(fdlRepeat);
    wRepeat = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wRepeat);
    FormData fdRepeat = new FormData();
    fdRepeat.left = new FormAttachment(middle, 0);
    fdRepeat.right = new FormAttachment(100, 0);
    fdRepeat.top = new FormAttachment(wlRepeat, 0, SWT.CENTER);
    wRepeat.setLayoutData(fdRepeat);
    wRepeat.addListener(SWT.Selection, e -> enableDisableControls());

    gRepeat = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(gRepeat);
    gRepeat.setText(BaseMessages.getString(PKG, "ActionStart.Repeat.Label"));
    FormData fdgRepeat = new FormData();
    fdgRepeat.left = new FormAttachment(0, 0);
    fdgRepeat.right = new FormAttachment(100, 0);
    fdgRepeat.top = new FormAttachment(wRepeat, 0);
    fdgRepeat.bottom = new FormAttachment(wOk, -2 * margin);
    gRepeat.setLayoutData(fdgRepeat);

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    gRepeat.setLayout(groupLayout);

    Label wlType = new Label(gRepeat, SWT.RIGHT);
    PropsUi.setLook(wlType);
    wlType.setText(BaseMessages.getString(PKG, "ActionStart.Type.Label"));
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    fdlType.top = new FormAttachment(0, margin);
    wlType.setLayoutData(fdlType);

    wType = new ComboVar(variables, gRepeat, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wType);
    wType.addListener(SWT.Selection, arg0 -> enableDisableControls());
    wType.setItems(new String[] {NO_SCHEDULING, INTERVAL, DAILY, WEEKLY, MONTHLY});
    wType.setEditable(false);
    wType.getCComboWidget().setVisibleItemCount(wType.getItemCount());
    FormData fdType = new FormData();
    fdType.left = new FormAttachment(middle, 0);
    fdType.right = new FormAttachment(100, 0);
    fdType.top = new FormAttachment(wlType, 0, SWT.CENTER);
    wType.setLayoutData(fdType);

    wDoNotWaitOnFirstExecution = new Button(gRepeat, SWT.CHECK);
    placeControl(
        gRepeat,
        BaseMessages.getString(PKG, "ActionStart.DoNotWaitAtFirstExecution.Label"),
        wDoNotWaitOnFirstExecution,
        wType);

    wIntervalSeconds = new TextVar(variables, gRepeat, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    placeControl(
        gRepeat,
        BaseMessages.getString(PKG, "ActionStart.IntervalSeconds.Label"),
        wIntervalSeconds,
        wDoNotWaitOnFirstExecution);

    wIntervalMinutes = new TextVar(variables, gRepeat, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    placeControl(
        gRepeat,
        BaseMessages.getString(PKG, "ActionStart.IntervalMinutes.Label"),
        wIntervalMinutes,
        wIntervalSeconds);

    Composite time = new Composite(gRepeat, SWT.NONE);
    time.setLayout(new FillLayout());
    wHour = new TextVar(variables, time, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMinutes = new TextVar(variables, time, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    placeControl(
        gRepeat,
        BaseMessages.getString(PKG, "ActionStart.TimeOfDay.Label"),
        time,
        wIntervalMinutes);

    wDayOfWeek = new ComboVar(variables, gRepeat, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Sunday"));
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Monday"));
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Tuesday"));
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Wednesday"));
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Thursday"));
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Friday"));
    wDayOfWeek.add(BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Saturday"));
    wDayOfWeek.setEditable(false);
    wDayOfWeek.getCComboWidget().setVisibleItemCount(wDayOfWeek.getItemCount());
    placeControl(
        gRepeat, BaseMessages.getString(PKG, "ActionStart.DayOfWeek.Label"), wDayOfWeek, time);

    wDayOfMonth = new TextVar(variables, gRepeat, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    placeControl(
        gRepeat,
        BaseMessages.getString(PKG, "ActionStart.DayOfMonth.Label"),
        wDayOfMonth,
        wDayOfWeek);

    getData();
    enableDisableControls();

    BaseDialog.defaultShellHandling(shell, n -> ok(), n -> cancel());

    return action;
  }

  public void getData() {
    wName.setText(action.getName());
    wRepeat.setSelection(action.isRepeat());
    wType.select(action.getSchedulerType());
    wIntervalSeconds.setText(Const.NVL(action.getIntervalSeconds(), ""));
    wIntervalMinutes.setText(Const.NVL(action.getIntervalMinutes(), ""));
    wHour.setText(Const.NVL(action.getHour(), ""));
    wMinutes.setText(Const.NVL(action.getMinutes(), ""));
    wDayOfWeek.setText(Const.NVL(action.getWeekDay(), ""));
    wDayOfMonth.setText(Const.NVL(action.getDayOfMonth(), ""));
    wDoNotWaitOnFirstExecution.setSelection(action.isDoNotWaitOnFirstExecution());
    wName.setFocus();
  }

  private void cancel() {
    action = null;
    dispose();
  }

  private void ok() {
    action.setName(wName.getText());
    action.setRepeat(wRepeat.getSelection());
    action.setSchedulerType(wType.getSelectionIndex());
    action.setIntervalSeconds(wIntervalSeconds.getText());
    action.setIntervalMinutes(wIntervalMinutes.getText());
    action.setHour(wHour.getText());
    action.setMinutes(wMinutes.getText());
    action.setWeekDay(wDayOfWeek.getText());
    action.setDayOfMonth(wDayOfMonth.getText());
    action.setDoNotWaitOnFirstExecution(wDoNotWaitOnFirstExecution.getSelection());

    action.setChanged();
    dispose();
  }

  private void placeControl(Composite composite, String text, Control control, Control under) {
    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();
    int extraVerticalMargin = (under instanceof Button) ? 2 * margin : 0;

    Label label = new Label(composite, SWT.RIGHT);
    label.setText(text);
    PropsUi.setLook(label);
    FormData formDataLabel = new FormData();
    formDataLabel.left = new FormAttachment(0, 0);
    if (under != null) {
      formDataLabel.top = new FormAttachment(under, margin + extraVerticalMargin);
    } else {
      formDataLabel.top = new FormAttachment(0, 0);
    }
    formDataLabel.right = new FormAttachment(middle, -margin);
    label.setLayoutData(formDataLabel);

    PropsUi.setLook(control);
    FormData formDataControl = new FormData();
    formDataControl.left = new FormAttachment(middle, 0);
    if (under != null) {
      formDataControl.top = new FormAttachment(under, margin + extraVerticalMargin);
    } else {
      formDataControl.top = new FormAttachment(0, 0);
    }
    formDataControl.right = new FormAttachment(100, 0);
    control.setLayoutData(formDataControl);
  }

  private void enableDisableControls() {
    boolean repeatEnabled = wRepeat.getSelection();
    boolean intervalSecondsEnabled = false;
    boolean intervalMinutesEnabled = false;
    boolean hourEnabled = false;
    boolean minutesEnabled = false;
    boolean dayOfWeekEnabled = false;
    boolean dayOfMonthEnabled = false;
    boolean doNotWaitOnFirstExecutionEnabled = false;

    if (repeatEnabled) {
      if (INTERVAL.equals(wType.getText())) {
        intervalSecondsEnabled = true;
        intervalMinutesEnabled = true;
      } else if (DAILY.equals(wType.getText())) {
        hourEnabled = true;
        minutesEnabled = true;
      } else if (WEEKLY.equals(wType.getText())) {
        dayOfWeekEnabled = true;
        hourEnabled = true;
        minutesEnabled = true;
      } else if (MONTHLY.equals(wType.getText())) {
        dayOfMonthEnabled = true;
        hourEnabled = true;
        minutesEnabled = true;
      }
    }

    gRepeat.setEnabled(repeatEnabled);
    wType.setEnabled(repeatEnabled);
    wIntervalMinutes.setEnabled(intervalMinutesEnabled);
    wIntervalSeconds.setEnabled(intervalSecondsEnabled);
    wDayOfWeek.setEnabled(dayOfWeekEnabled);
    wDayOfMonth.setEnabled(dayOfMonthEnabled);
    wHour.setEnabled(hourEnabled);
    wMinutes.setEnabled(minutesEnabled);
    wDoNotWaitOnFirstExecution.setEnabled(repeatEnabled);
  }
}

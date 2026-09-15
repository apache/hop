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

package org.apache.hop.workflow.actions.dbt;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Const;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;

/**
 * SWT editor for the {@link DbtProject} metadata type, shown in the Hop GUI metadata perspective.
 */
public class DbtProjectEditor extends MetadataEditor<DbtProject> {

  private static final Class<?> PKG = DbtProjectEditor.class;

  /** How often the dbt debug process is checked for cancellation. */
  private static final long POLL_INTERVAL_MS = 250L;

  private IVariables variables;

  private Text wName;
  private TextVar wProjectDirectory;
  private TextVar wProfilesDirectory;
  private TextVar wDefaultTarget;
  private TextVar wDbtExecutable;
  private TextVar wDbtOlExecutable;

  public DbtProjectEditor(HopGui hopGui, MetadataManager<DbtProject> manager, DbtProject metadata) {
    super(hopGui, manager, metadata);
  }

  @Override
  public void createControl(Composite composite) {
    PropsUi props = PropsUi.getInstance();
    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    variables = hopGui.getVariables();

    Label wlName = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(wlName);
    wlName.setText(BaseMessages.getString(PKG, "DbtProjectEditor.Label.Name"));
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wName);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.right = new FormAttachment(100, 0);
    fdName.top = new FormAttachment(0, margin);
    wName.setLayoutData(fdName);
    Control lastControl = wName;

    wProjectDirectory =
        addTextLine(
            composite, "DbtProjectEditor.Label.ProjectDirectory", middle, margin, lastControl);
    lastControl = wProjectDirectory;

    wProfilesDirectory =
        addTextLine(
            composite, "DbtProjectEditor.Label.ProfilesDirectory", middle, margin, lastControl);
    lastControl = wProfilesDirectory;

    wDefaultTarget =
        addTextLine(composite, "DbtProjectEditor.Label.DefaultTarget", middle, margin, lastControl);
    lastControl = wDefaultTarget;

    wDbtExecutable =
        addTextLine(composite, "DbtProjectEditor.Label.DbtExecutable", middle, margin, lastControl);
    lastControl = wDbtExecutable;

    wDbtOlExecutable =
        addTextLine(
            composite, "DbtProjectEditor.Label.DbtOlExecutable", middle, margin, lastControl);

    setWidgetsContent();
    resetChanged();

    Control[] controls = {
      wName, wProjectDirectory, wProfilesDirectory, wDefaultTarget, wDbtExecutable, wDbtOlExecutable
    };
    for (Control control : controls) {
      control.addListener(
          SWT.Modify,
          e -> {
            setChanged();
            MetadataPerspective.getInstance().updateEditor(this);
          });
    }
  }

  private TextVar addTextLine(
      Composite composite, String labelKey, int middle, int margin, Control above) {
    Label label = new Label(composite, SWT.RIGHT);
    PropsUi.setLook(label);
    label.setText(BaseMessages.getString(PKG, labelKey));
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.right = new FormAttachment(middle, -margin);
    fdl.top = new FormAttachment(above, margin);
    label.setLayoutData(fdl);

    TextVar text = new TextVar(variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(above, margin);
    text.setLayoutData(fd);
    return text;
  }

  @Override
  public void setWidgetsContent() {
    wName.setText(Const.NVL(metadata.getName(), ""));
    wProjectDirectory.setText(Const.NVL(metadata.getProjectDirectory(), ""));
    wProfilesDirectory.setText(Const.NVL(metadata.getProfilesDirectory(), ""));
    wDefaultTarget.setText(Const.NVL(metadata.getDefaultTarget(), ""));
    wDbtExecutable.setText(Const.NVL(metadata.getDbtExecutable(), ""));
    wDbtOlExecutable.setText(Const.NVL(metadata.getDbtOlExecutable(), ""));
  }

  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wTest = new Button(composite, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> testProject());
    return new Button[] {wTest};
  }

  /**
   * Runs {@code dbt debug} against the configured project to validate the runtime and profile. It
   * runs off the SWT thread behind a progress dialog: {@code dbt debug} contacts the warehouse, so
   * on an unreachable host it would otherwise freeze the whole Hop GUI until it gave up.
   */
  private void testProject() {
    String executable = variables.resolve(wDbtExecutable.getText());
    if (Utils.isEmpty(executable)) {
      executable = "dbt";
    }
    String projectDir = variables.resolve(wProjectDirectory.getText());

    List<String> command = new ArrayList<>();
    command.add(executable);
    command.add("debug");
    if (!Utils.isEmpty(projectDir)) {
      command.add("--project-dir");
      command.add(projectDir);
    }
    String profilesDir = variables.resolve(wProfilesDirectory.getText());
    if (!Utils.isEmpty(profilesDir)) {
      command.add("--profiles-dir");
      command.add(profilesDir);
    }
    String target = variables.resolve(wDefaultTarget.getText());
    if (!Utils.isEmpty(target)) {
      command.add("--target");
      command.add(target);
    }

    DebugOutcome outcome = new DebugOutcome();
    try {
      new ProgressMonitorDialog(hopGui.getShell())
          .run(
              true,
              monitor -> {
                // dbt reports no progress of its own, so this is a single indeterminate step.
                monitor.beginTask(BaseMessages.getString(PKG, "DbtProjectEditor.Test.Running"), 1);
                try {
                  runDebug(command, projectDir, monitor, outcome);
                } catch (IOException e) {
                  throw new InvocationTargetException(e);
                } finally {
                  monitor.done();
                }
              });
    } catch (InvocationTargetException e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "DbtProjectEditor.Test.Error.Title"),
          "Unable to run '" + String.join(" ", command) + "'",
          e.getCause() == null ? e : e.getCause());
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    if (outcome.cancelled) {
      return;
    }
    int style = outcome.exitCode == 0 ? SWT.OK | SWT.ICON_INFORMATION : SWT.OK | SWT.ICON_WARNING;
    MessageBox box = new MessageBox(hopGui.getShell(), style);
    box.setText("dbt debug (exit=" + outcome.exitCode + ")");
    box.setMessage(outcome.output.length() == 0 ? "(no output)" : outcome.output.toString());
    box.open();
  }

  /** Runs dbt debug on a worker thread, killing it when the progress dialog is cancelled. */
  private void runDebug(
      List<String> command, String projectDir, IProgressMonitor monitor, DebugOutcome outcome)
      throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true);
    if (!Utils.isEmpty(projectDir)) {
      pb.directory(new File(projectDir));
    }
    Process process = pb.start();
    Thread pump =
        new Thread(
            () -> {
              try (BufferedReader reader =
                  new BufferedReader(
                      new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                  synchronized (outcome.output) {
                    outcome.output.append(line).append(Const.CR);
                  }
                }
              } catch (IOException e) {
                synchronized (outcome.output) {
                  outcome.output.append(e.getMessage()).append(Const.CR);
                }
              }
            },
            "dbt-debug-output");
    pump.setDaemon(true);
    pump.start();

    try {
      while (!process.waitFor(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS)) {
        if (monitor.isCanceled()) {
          outcome.cancelled = true;
          process.descendants().forEach(ProcessHandle::destroy);
          process.destroy();
          break;
        }
      }
    } finally {
      pump.join(TimeUnit.SECONDS.toMillis(2));
    }
    outcome.exitCode = process.waitFor();
  }

  /** Collects what the worker thread produced so the UI thread can report it. */
  private static final class DebugOutcome {
    private final StringBuilder output = new StringBuilder();
    private int exitCode;
    private boolean cancelled;
  }

  @Override
  public void getWidgetsContent(DbtProject metadata) {
    metadata.setName(wName.getText());
    metadata.setProjectDirectory(wProjectDirectory.getText());
    metadata.setProfilesDirectory(wProfilesDirectory.getText());
    metadata.setDefaultTarget(wDefaultTarget.getText());
    metadata.setDbtExecutable(wDbtExecutable.getText());
    metadata.setDbtOlExecutable(wDbtOlExecutable.getText());
  }
}

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

package org.apache.hop.ai.ui;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

/** Review and selectively apply AI-proposed pipeline or workflow graph changes. */
public class AiAdvisorProposalReviewDialog {

  private static final Class<?> PKG = AiAdvisorPerspective.class;
  private static final String BLOCKED_ITEM_KEY = "blocked";

  private final Shell parent;
  private final List<AiProposal> proposals;
  private final List<AiProposalValidation> validations;
  private final Function<AiProposal, String> previewFn;
  private Shell shell;
  private Table wProposals;
  private Text wPreview;
  private boolean applied;
  private List<AiProposal> selectedProposals = List.of();

  public AiAdvisorProposalReviewDialog(
      Shell parent,
      List<AiProposal> proposals,
      List<AiProposalValidation> validations,
      Function<AiProposal, String> previewFn) {
    this.parent = parent;
    this.proposals = proposals != null ? proposals : List.of();
    this.validations = validations != null ? validations : List.of();
    this.previewFn = previewFn != null ? previewFn : proposal -> "";
  }

  public boolean open() {
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Title"));
    shell.setLayout(PropsUi.getInstance().createFormLayout());

    int margin = PropsUi.getMargin();

    Button wApply = new Button(shell, SWT.PUSH);
    wApply.setText(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Apply.Label"));
    wApply.addListener(SWT.Selection, e -> applySelected());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Cancel.Label"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wApply, wCancel}, margin, null);

    Label wlList = new Label(shell, SWT.LEFT);
    wlList.setText(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Proposals.Label"));
    PropsUi.setLook(wlList);
    wlList.setLayoutData(new FormDataBuilder().left(0, margin).top(0, margin).result());

    Label wlPreview = new Label(shell, SWT.LEFT);
    wlPreview.setText(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Preview.Label"));
    PropsUi.setLook(wlPreview);
    wlPreview.setLayoutData(new FormDataBuilder().left(0, margin).top(34, 0).result());

    wProposals =
        new Table(
            shell,
            SWT.CHECK | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL | SWT.FULL_SELECTION | SWT.MULTI);
    PropsUi.setLook(wProposals);
    wProposals.setHeaderVisible(false);
    wProposals.setLinesVisible(true);
    TableColumn column = new TableColumn(wProposals, SWT.NONE);
    column.setResizable(true);
    column.setWidth(580);
    wProposals.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wlList, margin)
            .right(100, -margin)
            .bottom(wlPreview, -margin)
            .result());

    for (int i = 0; i < proposals.size(); i++) {
      AiProposal proposal = proposals.get(i);
      AiProposalValidation validation = i < validations.size() ? validations.get(i) : null;
      boolean blocked = validation != null && validation.isBlocked();
      TableItem item = new TableItem(wProposals, SWT.NONE);
      item.setText(proposalLabel(proposal, validation));
      item.setChecked(!blocked);
      if (blocked) {
        item.setData(BLOCKED_ITEM_KEY, Boolean.TRUE);
      }
    }
    column.pack();

    wProposals.addListener(
        SWT.Selection,
        e -> {
          if (e.detail == SWT.CHECK) {
            TableItem item = (TableItem) e.item;
            if (item != null && Boolean.TRUE.equals(item.getData(BLOCKED_ITEM_KEY))) {
              item.setChecked(false);
            }
          }
          updatePreview();
        });

    wPreview =
        new Text(shell, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.READ_ONLY);
    PropsUi.setLook(wPreview);
    wPreview.setLayoutData(
        new FormDataBuilder()
            .left(0, margin)
            .top(wlPreview, margin)
            .bottom(wApply, -margin)
            .right(100, -margin)
            .result());

    updatePreview();
    BaseTransformDialog.setSize(shell);
    shell.open();
    while (!shell.isDisposed()) {
      if (!parent.getDisplay().readAndDispatch()) {
        parent.getDisplay().sleep();
      }
    }
    return applied;
  }

  public List<AiProposal> getSelectedProposals() {
    return selectedProposals;
  }

  private List<AiProposal> readSelectedFromWidgets() {
    List<AiProposal> selected = new ArrayList<>();
    if (wProposals == null || wProposals.isDisposed()) {
      return selected;
    }
    for (int i = 0; i < wProposals.getItemCount() && i < proposals.size(); i++) {
      TableItem item = wProposals.getItem(i);
      if (item.getChecked()) {
        selected.add(proposals.get(i));
      }
    }
    return selected;
  }

  private void updatePreview() {
    StringBuilder preview = new StringBuilder();
    List<AiProposal> selected = readSelectedFromWidgets();
    for (int i = 0; i < selected.size(); i++) {
      if (i > 0) {
        preview.append("\n\n---\n\n");
      }
      preview.append(previewFn.apply(selected.get(i)));
    }
    wPreview.setText(preview.toString());
  }

  private void applySelected() {
    selectedProposals = readSelectedFromWidgets();
    applied = !selectedProposals.isEmpty();
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void cancel() {
    applied = false;
    selectedProposals = List.of();
    shell.dispose();
  }

  private static String proposalLabel(AiProposal proposal, AiProposalValidation validation) {
    String description =
        !Utils.isEmpty(proposal.getDescription()) ? proposal.getDescription() : proposal.getType();
    StringBuilder label = new StringBuilder();
    label.append('[').append(proposal.getRiskLevel()).append("] ").append(description);
    if (validation != null && validation.isBlocked() && !Utils.isEmpty(validation.getReason())) {
      label.append(" — ").append(validation.getReason());
    } else if (validation != null && !Utils.isEmpty(validation.getWarning())) {
      label.append(" (").append(validation.getWarning()).append(')');
    }
    return label.toString();
  }
}

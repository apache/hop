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

package org.apache.hop.pipeline.transforms.languagemodelchat;

import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters.Builder.buildCompositeParameters;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.i18nUtil.i18n;
import static org.apache.hop.ui.core.PropsUi.getMargin;
import static org.apache.hop.ui.core.PropsUi.setLook;
import static org.apache.hop.ui.core.dialog.BaseDialog.defaultShellHandling;
import static org.eclipse.swt.SWT.BORDER;
import static org.eclipse.swt.SWT.DIALOG_TRIM;
import static org.eclipse.swt.SWT.MAX;
import static org.eclipse.swt.SWT.MIN;
import static org.eclipse.swt.SWT.RESIZE;

import java.util.Collection;
import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CancelButton;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters.Builder;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.GeneralSettingsComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.IDialogComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.OkButton;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.PopulateInputsAdapter;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.TransformNameComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models.AnthropicComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models.HuggingFaceComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models.IModelComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models.MistralComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models.OllamaComposite;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models.OpenAiComposite;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

public class LanguageModelChatDialog extends BaseTransformDialog
    implements ITransformDialog, PopulateInputsAdapter {

  private final LanguageModelChatMeta input;
  private Collection<IDialogComposite> composites;

  public LanguageModelChatDialog(
      Shell parent,
      IVariables variables,
      LanguageModelChatMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, DIALOG_TRIM | RESIZE | MIN | MAX);
    setLook(shell);
    setShellImage(shell, input);

    changed = input.hasChanged();

    int margin = getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = margin;
    formLayout.marginHeight = margin;

    shell.setLayout(formLayout);
    shell.setText(i18n("LanguageModelChatDialog.Shell.Title"));

    // Model Specific Composite
    Composite modelComposite = new Composite(shell, BORDER);

    PopulateInputsAdapter modelCompositeInputsAdapter =
        new PopulateInputsAdapter() {
          @Override
          public void populateInputs() {
            LanguageModelChatDialog.this.populateInputs(modelComposite);
          }
        };

    Builder params =
        buildCompositeParameters()
            .dialog(this)
            .populateInputsAdapter(modelCompositeInputsAdapter)
            .middlePct(props.getMiddlePct())
            .margin(margin)
            .shell(shell)
            .parent(shell)
            .meta(input)
            .variables(variables)
            .transformName(transformName)
            .pipelineMeta(pipelineMeta);

    // Transform Name Row
    IDialogComposite tnc = new TransformNameComposite(params.build());

    IDialogComposite gsc = new GeneralSettingsComposite(params.control(shell).build());

    // Model Specific Composite
    modelComposite.setLayout(new FormLayout());
    FormData fdModelSpecificComp = new FormData();
    fdModelSpecificComp.left = new FormAttachment(0, 0);
    fdModelSpecificComp.top = new FormAttachment(gsc.control(), margin);
    fdModelSpecificComp.right = new FormAttachment(100, 0);
    fdModelSpecificComp.bottom = new FormAttachment(100, -50); // Leave space for OK/Cancel
    modelComposite.setLayoutData(fdModelSpecificComp);
    setLook(modelComposite);
    params.parent(modelComposite);

    IDialogComposite anthropic = new AnthropicComposite(params.control(modelComposite).build());
    IDialogComposite huggingface = new HuggingFaceComposite(params.control(modelComposite).build());
    IDialogComposite mistral = new MistralComposite(params.control(modelComposite).build());
    IDialogComposite ollama = new OllamaComposite(params.control(modelComposite).build());
    IDialogComposite openai = new OpenAiComposite(params.control(modelComposite).build());

    composites = List.of(tnc, gsc, anthropic, huggingface, mistral, ollama, openai);

    // OK and Cancel buttons
    setButtonPositions(
        new Button[] {
          new OkButton(shell, e -> ok()).delegate(),
          new CancelButton(shell, e -> cancel()).delegate()
        },
        margin,
        modelComposite);

    populateInputs(modelComposite);

    // Open dialog and return the transformed name
    defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  public void setTransformName(String transformName) {
    this.transformName = transformName;
  }

  @Override
  public void populateInputs(Composite composite) {
    // Hide all model-specific composites initially
    for (IDialogComposite c : composites) {
      if (c instanceof IModelComposite) {
        c.composite().setVisible(false);
        c.updateLayouts();
      }
    }

    // Show the relevant composite based on the modelType selection
    composites.forEach(IDialogComposite::populateInputs);

    // Show the selected model composite
    for (IDialogComposite c : composites) {
      if (c instanceof IModelComposite m && m.isSelectedModelType()) {
        c.composite().setVisible(true);
        c.updateLayouts();
      }

      c.updateLayouts();
    }

    composite.layout(true, true);
    shell.layout(true, true); // Ensure the dialog is correctly laid out after setting the data

    input.setChanged(changed);
  }

  private void ok() {
    int validated = (int) composites.stream().filter(IDialogComposite::validateInputs).count();
    if (validated != composites.size()) {
      // TODO implement
      return;
    }

    if (composites.stream().anyMatch(c -> !c.ok())) {
      return;
    }

    // Mark the transform as changed
    input.setChanged();

    // Close the dialog
    dispose();
  }

  private void cancel() {

    composites.forEach(IDialogComposite::cancel);

    // Set the transformName to null indicates that no changes should be applied
    transformName = null;
    // Mark the transform as unchanged (since we are cancelling)
    input.setChanged(false);
    // Close the dialog
    dispose();
  }
}

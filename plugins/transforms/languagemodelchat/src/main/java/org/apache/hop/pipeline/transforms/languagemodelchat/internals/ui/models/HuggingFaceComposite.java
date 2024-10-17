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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.models;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trim;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.HUGGING_FACE;

import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;

public class HuggingFaceComposite extends AbstractModelComposite {

  private final Label accessTokenLabel;
  private final TextVar accessTokenInput;
  private final Label modelIdLabel;
  private final TextVar modelIdInput;
  private final Label timeoutLabel;
  private final TextVar timeoutInput;
  private final Label temperatureLabel;
  private final TextVar temperatureInput;
  private final Label maxNewTokensLabel;
  private final TextVar maxNewTokensInput;
  private final Button returnFullTextButton;
  private final Button waitForModelButton;

  public HuggingFaceComposite(CompositeParameters parameters) {
    super(HUGGING_FACE, parameters);

    // Access Token
    accessTokenLabel = createLabel();
    accessTokenInput = createTextVar();
    prepare("AccessToken", accessTokenLabel, accessTokenInput);

    // Temperature
    temperatureLabel = createLabel();
    temperatureInput = createTextVar();
    prepare("Temperature", temperatureLabel, temperatureInput);

    // Model ID
    modelIdLabel = createLabel();
    modelIdInput = createTextVar();
    prepare("ModelId", modelIdLabel, modelIdInput);

    // Max new tokens
    maxNewTokensLabel = createLabel();
    maxNewTokensInput = createTextVar();
    prepare("MaxNewTokens", maxNewTokensLabel, maxNewTokensInput);

    // Timeout
    timeoutLabel = createLabel();
    timeoutInput = createTextVar();
    prepare("Timeout", timeoutLabel, timeoutInput);

    // Return Full Text
    returnFullTextButton = createButton();
    prepare("ReturnFullText", null, returnFullTextButton);

    // Wait for model
    waitForModelButton = createButton();
    prepare("WaitForModel", null, waitForModelButton);
  }

  @Override
  public void loadData() {
    if (!isSelectedModelType()) {
      return; // Don't load data
    }

    if (meta.getHuggingFaceAccessToken() != null) {
      accessTokenInput.setText(meta.getHuggingFaceAccessToken());
    }
    if (meta.getHuggingFaceTemperature() != null) {
      temperatureInput.setText("" + meta.getHuggingFaceTemperature());
    }
    if (meta.getHuggingFaceModelId() != null) {
      modelIdInput.setText(meta.getHuggingFaceModelId());
    }
    if (meta.getHuggingFaceMaxNewTokens() != null) {
      maxNewTokensInput.setText("" + meta.getHuggingFaceMaxNewTokens());
    }
    if (meta.getHuggingFaceTimeout() != null) {
      timeoutInput.setText("" + meta.getHuggingFaceTimeout());
    }

    returnFullTextButton.setSelection(meta.isHuggingFaceReturnFullText());
    waitForModelButton.setSelection(meta.isHuggingFaceWaitForModel());
  }

  @Override
  public boolean validateInputs() {
    // If not selected modelType, always validate
    if (!isSelectedModelType()) {
      return true;
    }

    String temperature = trim(temperatureInput.getText());
    if (!(isBlank(temperature) || isCreatable(temperature))) {
      return false;
    }

    // TODO Implement

    return true;
  }

  @Override
  public void updateTransformMeta() {
    if (!validateInputs()) {
      return;
    }
    meta.setHuggingFaceAccessToken(trimStringToNull(accessTokenInput.getText()));
    meta.setHuggingFaceTemperature(trimDoubleToNull(temperatureInput.getText()));
    meta.setHuggingFaceModelId(trimStringToNull(modelIdInput.getText()));
    meta.setHuggingFaceMaxNewTokens(trimIntegerToNull(maxNewTokensInput.getText()));
    meta.setHuggingFaceTimeout(trimIntegerToNull(timeoutInput.getText()));
    meta.setHuggingFaceReturnFullText(returnFullTextButton.getSelection());
    meta.setHuggingFaceWaitForModel(waitForModelButton.getSelection());
  }

  public Label getAccessTokenLabel() {
    return accessTokenLabel;
  }

  public TextVar getAccessTokenInput() {
    return accessTokenInput;
  }

  public Label getModelIdLabel() {
    return modelIdLabel;
  }

  public TextVar getModelIdInput() {
    return modelIdInput;
  }

  public Label getTimeoutLabel() {
    return timeoutLabel;
  }

  public TextVar getTimeoutInput() {
    return timeoutInput;
  }

  public Label getTemperatureLabel() {
    return temperatureLabel;
  }

  public TextVar getTemperatureInput() {
    return temperatureInput;
  }

  public Label getMaxNewTokensLabel() {
    return maxNewTokensLabel;
  }

  public TextVar getMaxNewTokensInput() {
    return maxNewTokensInput;
  }

  public Button getReturnFullTextButton() {
    return returnFullTextButton;
  }

  public Button getWaitForModelButton() {
    return waitForModelButton;
  }
}

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
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.MISTRAL;

import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;

public class MistralComposite extends AbstractModelComposite {

  private final Label baseUrlLabel;
  private final TextVar baseUrlInput;
  private final Label apiKeyLabel;
  private final TextVar apiKeyInput;
  private final Label modelNameLabel;
  private final TextVar modelNameInput;
  private final Label temperatureLabel;
  private final TextVar temperatureInput;
  private final Label topPLabel;
  private final TextVar topPInput;
  private final Label maxTokensLabel;
  private final TextVar maxTokensInput;
  private final Button safePromptButton;
  private final Label seedLabel;
  private final TextVar seedInput;
  private final Label responseFormatLabel;
  private final TextVar responseFormatInput;
  private final Label timeoutLabel;
  private final TextVar timeoutInput;
  private final Button logRequestsButton;
  private final Button logResponsesButton;
  private final Label maxRetriesLabel;
  private final TextVar maxRetriesInput;

  public MistralComposite(CompositeParameters parameters) {
    super(MISTRAL, parameters);
    // Base URL
    baseUrlLabel = createLabel();
    baseUrlInput = createTextVar();
    prepare("BaseUrl", baseUrlLabel, baseUrlInput);

    // API Key
    apiKeyLabel = createLabel();
    apiKeyInput = createTextVar();
    prepare("ApiKey", apiKeyLabel, apiKeyInput);

    // Model Name
    modelNameLabel = createLabel();
    modelNameInput = createTextVar();
    prepare("ModelName", modelNameLabel, modelNameInput);

    // Temperature
    temperatureLabel = createLabel();
    temperatureInput = createTextVar();
    prepare("Temperature", temperatureLabel, temperatureInput);

    // Top P
    topPLabel = createLabel();
    topPInput = createTextVar();
    prepare("TopP", topPLabel, topPInput);

    // Max tokens
    maxTokensLabel = createLabel();
    maxTokensInput = createTextVar();
    prepare("MaxTokens", maxTokensLabel, maxTokensInput);

    // Safe Prompt
    safePromptButton = createButton();
    prepare("SafePrompt", null, safePromptButton);

    // Seed
    seedLabel = createLabel();
    seedInput = createTextVar();
    prepare("Seed", seedLabel, seedInput);

    // Response Format
    responseFormatLabel = createLabel();
    responseFormatInput = createTextVar();
    prepare("ResponseFormat", responseFormatLabel, responseFormatInput);

    // Timeout
    timeoutLabel = createLabel();
    timeoutInput = createTextVar();
    prepare("Timeout", timeoutLabel, timeoutInput);

    // Log Requests
    logRequestsButton = createButton();
    prepare("LogRequests", null, logRequestsButton);

    // Log Responses
    logResponsesButton = createButton();
    prepare("LogResponses", null, logResponsesButton);

    // Max Retries
    maxRetriesLabel = createLabel();
    maxRetriesInput = createTextVar();
    prepare("MaxRetries", maxRetriesLabel, maxRetriesInput);
  }

  @Override
  public void loadData() {
    if (!isSelectedModelType()) {
      return; // Don't load data
    }
    if (meta.getMistralBaseUrl() != null) {
      baseUrlInput.setText(meta.getMistralBaseUrl());
    }
    if (meta.getMistralApiKey() != null) {
      apiKeyInput.setText(meta.getMistralApiKey());
    }
    if (meta.getMistralModelName() != null) {
      modelNameInput.setText(meta.getMistralModelName());
    }
    if (meta.getMistralTemperature() != null) {
      temperatureInput.setText("" + meta.getMistralTemperature());
    }
    if (meta.getMistralTopP() != null) {
      topPInput.setText("" + meta.getMistralTopP());
    }
    if (meta.getMistralMaxTokens() != null) {
      maxTokensInput.setText("" + meta.getMistralMaxTokens());
    }
    safePromptButton.setSelection(meta.isMistralSafePrompt());
    if (meta.getMistralRandomSeed() != null) {
      seedInput.setText("" + meta.getMistralRandomSeed());
    }
    if (meta.getMistralResponseFormat() != null) {
      responseFormatInput.setText(meta.getMistralResponseFormat());
    }
    if (meta.getMistralTimeout() != null) {
      timeoutInput.setText("" + meta.getMistralTimeout());
    }
    logRequestsButton.setSelection(meta.isMistralLogRequests());

    logResponsesButton.setSelection(meta.isMistralLogResponses());

    if (meta.getMistralMaxRetries() != null) {
      maxRetriesInput.setText("" + meta.getMistralMaxRetries());
    }
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
    meta.setMistralBaseUrl(trimStringToNull(baseUrlInput.getText()));
    meta.setMistralApiKey(trimStringToNull(apiKeyInput.getText()));
    meta.setMistralModelName(trimStringToNull(modelNameInput.getText()));
    meta.setMistralTemperature(trimDoubleToNull(temperatureInput.getText()));
    meta.setMistralTopP(trimDoubleToNull(topPInput.getText()));
    meta.setMistralMaxTokens(trimIntegerToNull(maxTokensInput.getText()));
    meta.setMistralSafePrompt(safePromptButton.getSelection());
    meta.setMistralRandomSeed(trimIntegerToNull(seedInput.getText()));
    meta.setMistralResponseFormat(trimStringToNull(responseFormatInput.getText()));
    meta.setMistralTimeout(trimIntegerToNull(timeoutInput.getText()));
    meta.setMistralLogRequests(logRequestsButton.getSelection());
    meta.setMistralLogResponses(logResponsesButton.getSelection());
    meta.setMistralMaxRetries(trimIntegerToNull(maxRetriesInput.getText()));
  }

  public Label getApiKeyLabel() {
    return apiKeyLabel;
  }

  public TextVar getApiKeyInput() {
    return apiKeyInput;
  }

  public Label getModelNameLabel() {
    return modelNameLabel;
  }

  public TextVar getModelNameInput() {
    return modelNameInput;
  }

  public Label getTemperatureLabel() {
    return temperatureLabel;
  }

  public TextVar getTemperatureInput() {
    return temperatureInput;
  }

  public Label getBaseUrlLabel() {
    return baseUrlLabel;
  }

  public TextVar getBaseUrlInput() {
    return baseUrlInput;
  }

  public Button getSafePromptButton() {
    return safePromptButton;
  }

  public Label getTopPLabel() {
    return topPLabel;
  }

  public TextVar getTopPInput() {
    return topPInput;
  }

  public Label getMaxTokensLabel() {
    return maxTokensLabel;
  }

  public TextVar getMaxTokensInput() {
    return maxTokensInput;
  }

  public Label getResponseFormatLabel() {
    return responseFormatLabel;
  }

  public TextVar getResponseFormatInput() {
    return responseFormatInput;
  }

  public Label getSeedLabel() {
    return seedLabel;
  }

  public TextVar getSeedInput() {
    return seedInput;
  }

  public Label getTimeoutLabel() {
    return timeoutLabel;
  }

  public TextVar getTimeoutInput() {
    return timeoutInput;
  }

  public Label getMaxRetriesLabel() {
    return maxRetriesLabel;
  }

  public TextVar getMaxRetriesInput() {
    return maxRetriesInput;
  }

  public Button getLogRequestsButton() {
    return logRequestsButton;
  }

  public Button getLogResponsesButton() {
    return logResponsesButton;
  }
}

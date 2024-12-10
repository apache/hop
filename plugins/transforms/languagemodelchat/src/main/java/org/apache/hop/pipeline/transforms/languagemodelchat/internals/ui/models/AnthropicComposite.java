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
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.ANTHROPIC;

import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;

public class AnthropicComposite extends AbstractModelComposite {

  private final Label baseUrlLabel;
  private final TextVar baseUrlInput;
  private final Label apiKeyLabel;
  private final TextVar apiKeyInput;
  private final Label versionLabel;
  private final TextVar versionInput;
  private final Label modelNameLabel;
  private final TextVar modelNameInput;
  private final Label temperatureLabel;
  private final TextVar temperatureInput;
  private final Label topPLabel;
  private final TextVar topPInput;
  private final Label topKLabel;
  private final TextVar topKInput;
  private final Label maxTokensLabel;
  private final TextVar maxTokensInput;
  // TODO private final Label stopSequencesLabel;
  // TODO private final TextVar stopSequencesInput;
  // TODO private final Label seedLabel;
  // TODO private final TextVar seedInput;
  private final Label timeoutLabel;
  private final TextVar timeoutInput;
  private final Label maxRetriesLabel;
  private final TextVar maxRetriesInput;
  private final Button logRequestsButton;
  private final Button logResponsesButton;

  public AnthropicComposite(CompositeParameters parameters) {
    super(ANTHROPIC, parameters);
    // Base URL
    baseUrlLabel = createLabel();
    baseUrlInput = createTextVar();
    prepare("BaseUrl", baseUrlLabel, baseUrlInput);

    // API Key
    apiKeyLabel = createLabel();
    apiKeyInput = createTextVar();
    prepare("ApiKey", apiKeyLabel, apiKeyInput);

    // Version
    versionLabel = createLabel();
    versionInput = createTextVar();
    prepare("Version", versionLabel, versionInput);

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

    // TopK
    topKLabel = createLabel();
    topKInput = createTextVar();
    prepare("TopK", topKLabel, topKInput);

    // Max tokens
    maxTokensLabel = createLabel();
    maxTokensInput = createTextVar();
    prepare("MaxTokens", maxTokensLabel, maxTokensInput);

    // Stop Sequences
    /*
    TODO Implement anthropic Stop sequences
    stopSequencesLabel = createLabel();
    stopSequencesInput = createTextVar();
    prepare("StopSequences", stopSequencesLabel, stopSequencesInput);
     */

    // Seed
    /*
    TODO Implement anthropic seed
    seedLabel = createLabel();
    seedInput = createTextVar();
    prepare("Seed", seedLabel, seedInput);
     */

    // Timeout
    timeoutLabel = createLabel();
    timeoutInput = createTextVar();
    prepare("Timeout", timeoutLabel, timeoutInput);

    // Max Retries
    maxRetriesLabel = createLabel();
    maxRetriesInput = createTextVar();
    prepare("MaxRetries", maxRetriesLabel, maxRetriesInput);

    // Log Requests
    logRequestsButton = createButton();
    prepare("LogRequests", null, logRequestsButton);

    // Log Responses
    logResponsesButton = createButton();
    prepare("LogResponses", null, logResponsesButton);
  }

  @Override
  public void loadData() {
    if (!isSelectedModelType()) {
      return; // Don't load data
    }
    if (meta.getAnthropicBaseUrl() != null) {
      baseUrlInput.setText(meta.getAnthropicBaseUrl());
    }
    if (meta.getAnthropicApiKey() != null) {
      apiKeyInput.setText(meta.getAnthropicApiKey());
    }
    if (meta.getAnthropicVersion() != null) {
      versionInput.setText(meta.getAnthropicVersion());
    }
    if (meta.getAnthropicModelName() != null) {
      modelNameInput.setText(meta.getAnthropicModelName());
    }
    if (meta.getAnthropicTemperature() != null) {
      temperatureInput.setText("" + meta.getAnthropicTemperature());
    }
    if (meta.getAnthropicTopP() != null) {
      topPInput.setText("" + meta.getAnthropicTopP());
    }
    if (meta.getAnthropicTopK() != null) {
      topKInput.setText("" + meta.getAnthropicTopK());
    }
    if (meta.getAnthropicMaxTokens() != null) {
      maxTokensInput.setText("" + meta.getAnthropicMaxTokens());
    }
    /*
    TODO
     if (meta.getAnthropicStopSequences() != null) {
       stopSequencesInput.setText(meta.getAnthropicStopSequences());
     }
     if (meta.getAnthropicSeed() != null) {
        seedInput.setText("" + meta.getAnthropicSeed());
     }
     */
    if (meta.getAnthropicTimeout() != null) {
      timeoutInput.setText("" + meta.getAnthropicTimeout());
    }
    if (meta.getAnthropicMaxRetries() != null) {
      maxRetriesInput.setText("" + meta.getAnthropicMaxRetries());
    }
    logRequestsButton.setSelection(meta.isAnthropicLogRequests());
    logResponsesButton.setSelection(meta.isAnthropicLogResponses());
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
    meta.setAnthropicBaseUrl(trimStringToNull(baseUrlInput.getText()));
    meta.setAnthropicApiKey(trimStringToNull(apiKeyInput.getText()));
    meta.setAnthropicVersion(trimStringToNull(versionInput.getText()));
    meta.setAnthropicModelName(trimStringToNull(modelNameInput.getText()));
    meta.setAnthropicTemperature(trimDoubleToNull(temperatureInput.getText()));
    meta.setAnthropicTopP(trimDoubleToNull(topPInput.getText()));
    meta.setAnthropicTopK(trimIntegerToNull(topKInput.getText()));
    meta.setAnthropicMaxTokens(trimIntegerToNull(maxTokensInput.getText()));
    // TODO meta.setAnthropicStopSequences(trimStringToNull(stopSequencesInput.getText()));
    // TODO meta.setAnthropicSeed(trimIntegerToNull(seedInput.getText()));
    meta.setAnthropicTimeout(trimIntegerToNull(timeoutInput.getText()));
    meta.setAnthropicMaxRetries(trimIntegerToNull(maxRetriesInput.getText()));
    meta.setAnthropicLogRequests(logRequestsButton.getSelection());
    meta.setAnthropicLogResponses(logResponsesButton.getSelection());
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

  public Label getVersionLabel() {
    return versionLabel;
  }

  public TextVar getVersionInput() {
    return versionInput;
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

  public Label getTopKLabel() {
    return topKLabel;
  }

  public TextVar getTopKInput() {
    return topKInput;
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

  /*
  TODO Implement Anthropic stop sequences and seed
  public Label getStopSequencesLabel() {
    return stopSequencesLabel;
  }

  public TextVar getStopSequencesInput() {
    return stopSequencesInput;
  }

  public Label getSeedLabel() {
    return seedLabel;
  }

  public TextVar getSeedInput() {
    return seedInput;
  }
   */

  public Button getLogRequestsButton() {
    return logRequestsButton;
  }

  public Button getLogResponsesButton() {
    return logResponsesButton;
  }
}

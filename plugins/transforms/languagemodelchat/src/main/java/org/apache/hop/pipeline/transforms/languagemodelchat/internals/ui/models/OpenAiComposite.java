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
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.OPEN_AI;

import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;

public class OpenAiComposite extends AbstractModelComposite {

  private final Label baseUrlLabel;
  private final TextVar baseUrlInput;
  private final Label apiKeyLabel;
  private final TextVar apiKeyInput;
  private final Label organizationIdLabel;
  private final TextVar organizationIdInput;
  private final Label modelNameLabel;
  private final TextVar modelNameInput;
  private final Label temperatureLabel;
  private final TextVar temperatureInput;
  private final Label topPLabel;
  private final TextVar topPInput;
  private final Label maxTokensLabel;
  private final TextVar maxTokensInput;
  private final Label presencePenaltyLabel;
  private final TextVar presencePenaltyInput;
  private final Label frequencyPenaltyLabel;
  private final TextVar frequencyPenaltyInput;
  private final Label responseFormatLabel;
  private final TextVar responseFormatInput;
  private final Label seedLabel;
  private final TextVar seedInput;
  private final Label userLabel;
  private final TextVar userInput;
  private final Label timeoutLabel;
  private final TextVar timeoutInput;
  private final Label maxRetriesLabel;
  private final TextVar maxRetriesInput;
  private final Button useProxyButton;
  private final Label proxyHostLabel;
  private final TextVar proxyHostInput;
  private final Label proxyPortLabel;
  private final TextVar proxyPortInput;
  private final Button logRequestsButton;
  private final Button logResponsesButton;

  public OpenAiComposite(CompositeParameters parameters) {
    super(OPEN_AI, parameters);
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

    // Organisation
    organizationIdLabel = createLabel();
    organizationIdInput = createTextVar();
    prepare("Organisation", organizationIdLabel, organizationIdInput);

    // Top P
    topPLabel = createLabel();
    topPInput = createTextVar();
    prepare("TopP", topPLabel, topPInput);

    // Max tokens
    maxTokensLabel = createLabel();
    maxTokensInput = createTextVar();
    prepare("MaxTokens", maxTokensLabel, maxTokensInput);

    // Presence Penalty
    presencePenaltyLabel = createLabel();
    presencePenaltyInput = createTextVar();
    prepare("PresencePenalty", presencePenaltyLabel, presencePenaltyInput);

    // Frequency Penalty
    frequencyPenaltyLabel = createLabel();
    frequencyPenaltyInput = createTextVar();
    prepare("FrequencyPenalty", frequencyPenaltyLabel, frequencyPenaltyInput);

    // Response Format
    // TODO Convert to combo box. Only text and json_object are allowed
    responseFormatLabel = createLabel();
    responseFormatInput = createTextVar();
    prepare("ResponseFormat", responseFormatLabel, responseFormatInput);

    // Seed
    seedLabel = createLabel();
    seedInput = createTextVar();
    prepare("Seed", seedLabel, seedInput);

    // User
    userLabel = createLabel();
    userInput = createTextVar();
    prepare("User", userLabel, userInput);

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

    // Use Proxy
    useProxyButton = createButton();
    useProxyButton.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            proxyHostLabel.setVisible(useProxyButton.getSelection());
            proxyHostInput.setVisible(useProxyButton.getSelection());
            proxyPortLabel.setVisible(useProxyButton.getSelection());
            proxyPortInput.setVisible(useProxyButton.getSelection());
            composite.layout(true, true);
          }
        });
    prepare("UseProxy", null, useProxyButton);

    // Proxy Host
    proxyHostLabel = createLabel();
    proxyHostInput = createTextVar();
    proxyHostLabel.setVisible(false);
    proxyHostInput.setVisible(false);
    prepare("ProxyHost", proxyHostLabel, proxyHostInput);

    // Proxy Port
    proxyPortLabel = createLabel();
    proxyPortInput = createTextVar();
    proxyPortLabel.setVisible(false);
    proxyPortInput.setVisible(false);
    prepare("ProxyPort", proxyPortLabel, proxyPortInput);
  }

  @Override
  public void loadData() {
    if (!isSelectedModelType()) {
      return; // Don't load data
    }
    if (meta.getOpenAiBaseUrl() != null) {
      baseUrlInput.setText(meta.getOpenAiBaseUrl());
    }
    if (meta.getOpenAiApiKey() != null) {
      apiKeyInput.setText(meta.getOpenAiApiKey());
    }
    if (meta.getOpenAiOrganizationId() != null) {
      organizationIdInput.setText(meta.getOpenAiOrganizationId());
    }
    if (meta.getOpenAiModelName() != null) {
      modelNameInput.setText(meta.getOpenAiModelName());
    }
    if (meta.getOpenAiTemperature() != null) {
      temperatureInput.setText("" + meta.getOpenAiTemperature());
    }
    if (meta.getOpenAiTopP() != null) {
      topPInput.setText("" + meta.getOpenAiTopP());
    }
    if (meta.getOpenAiMaxTokens() != null) {
      maxTokensInput.setText("" + meta.getOpenAiMaxTokens());
    }
    if (meta.getOpenAiPresencePenalty() != null) {
      presencePenaltyInput.setText("" + meta.getOpenAiPresencePenalty());
    }
    if (meta.getOpenAiFrequencyPenalty() != null) {
      frequencyPenaltyInput.setText("" + meta.getOpenAiFrequencyPenalty());
    }
    if (meta.getOpenAiResponseFormat() != null) {
      responseFormatInput.setText(meta.getOpenAiResponseFormat());
    }
    if (meta.getOpenAiSeed() != null) {
      seedInput.setText("" + meta.getOpenAiSeed());
    }
    if (meta.getOpenAiUser() != null) {
      userInput.setText(meta.getOpenAiUser());
    }
    if (meta.getOpenAiTimeout() != null) {
      timeoutInput.setText("" + meta.getOpenAiTimeout());
    }
    if (meta.getOpenAiMaxRetries() != null) {
      maxRetriesInput.setText("" + meta.getOpenAiMaxRetries());
    }

    useProxyButton.setSelection(meta.isOpenAiUseProxy());

    if (meta.getOpenAiProxyHost() != null) {
      proxyHostInput.setText(meta.getOpenAiProxyHost());
    }
    if (meta.getOpenAiProxyPort() != null) {
      proxyPortInput.setText("" + meta.getOpenAiProxyPort());
    }
    logRequestsButton.setSelection(meta.isOpenAiLogRequests());
    logResponsesButton.setSelection(meta.isOpenAiLogResponses());
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
    meta.setOpenAiBaseUrl(trimStringToNull(baseUrlInput.getText()));
    meta.setOpenAiApiKey(trimStringToNull(apiKeyInput.getText()));
    meta.setOpenAiOrganizationId(trimStringToNull(organizationIdInput.getText()));
    meta.setOpenAiModelName(trimStringToNull(modelNameInput.getText()));
    meta.setOpenAiTemperature(trimDoubleToNull(temperatureInput.getText()));
    meta.setOpenAiTopP(trimDoubleToNull(topPInput.getText()));
    meta.setOpenAiMaxTokens(trimIntegerToNull(maxTokensInput.getText()));
    meta.setOpenAiPresencePenalty(trimDoubleToNull(presencePenaltyInput.getText()));
    meta.setOpenAiFrequencyPenalty(trimDoubleToNull(frequencyPenaltyInput.getText()));
    meta.setOpenAiResponseFormat(trimStringToNull(responseFormatInput.getText()));
    meta.setOpenAiSeed(trimIntegerToNull(seedInput.getText()));
    meta.setOpenAiUser(trimStringToNull(userInput.getText()));
    meta.setOpenAiTimeout(trimIntegerToNull(timeoutInput.getText()));
    meta.setOpenAiMaxRetries(trimIntegerToNull(maxRetriesInput.getText()));
    meta.setOpenAiUseProxy(useProxyButton.getSelection());
    meta.setOpenAiProxyHost(trimStringToNull(proxyHostInput.getText()));
    meta.setOpenAiProxyPort(trimIntegerToNull(proxyPortInput.getText()));
    meta.setOpenAiLogRequests(logRequestsButton.getSelection());
    meta.setOpenAiLogResponses(logResponsesButton.getSelection());
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

  public Label getOrganizationIdLabel() {
    return organizationIdLabel;
  }

  public TextVar getOrganizationIdInput() {
    return organizationIdInput;
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

  public Label getPresencePenaltyLabel() {
    return presencePenaltyLabel;
  }

  public TextVar getPresencePenaltyInput() {
    return presencePenaltyInput;
  }

  public Label getFrequencyPenaltyLabel() {
    return frequencyPenaltyLabel;
  }

  public TextVar getFrequencyPenaltyInput() {
    return frequencyPenaltyInput;
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

  public Label getUserLabel() {
    return userLabel;
  }

  public TextVar getUserInput() {
    return userInput;
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

  public Button getUseProxyButton() {
    return useProxyButton;
  }

  public Label getProxyHostLabel() {
    return proxyHostLabel;
  }

  public TextVar getProxyHostInput() {
    return proxyHostInput;
  }

  public Label getProxyPortLabel() {
    return proxyPortLabel;
  }

  public TextVar getProxyPortInput() {
    return proxyPortInput;
  }

  public Button getLogRequestsButton() {
    return logRequestsButton;
  }

  public Button getLogResponsesButton() {
    return logResponsesButton;
  }
}

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
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.OLLAMA;

import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ui.CompositeParameters;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.widgets.Label;

public class OllamaComposite extends AbstractModelComposite {

  private final Label imageEndpointLabel;
  private final TextVar imageEndpointInput;
  private final Label modelNameLabel;
  private final TextVar modelNameInput;
  private final Label temperatureLabel;
  private final TextVar temperatureInput;
  private final Label topKLabel;
  private final TextVar topKInput;
  private final Label topPLabel;
  private final TextVar topPInput;
  private final Label repeatPenaltyLabel;
  private final TextVar repeatPenaltyInput;
  private final Label seedLabel;
  private final TextVar seedInput;
  private final Label numPredictLabel;
  private final TextVar numPredictInput;
  private final Label numCtxLabel;
  private final TextVar numCtxInput;
  private final Label formatLabel;
  private final TextVar formatInput;
  private final Label timeoutLabel;
  private final TextVar timeoutInput;
  private final Label maxRetriesLabel;
  private final TextVar maxRetriesInput;

  public OllamaComposite(CompositeParameters parameters) {
    super(OLLAMA, parameters);
    // Base URL
    imageEndpointLabel = createLabel();
    imageEndpointInput = createTextVar();
    prepare("ImageEndpoint", imageEndpointLabel, imageEndpointInput);

    // Model Name
    modelNameLabel = createLabel();
    modelNameInput = createTextVar();
    prepare("ModelName", modelNameLabel, modelNameInput);

    // Temperature
    temperatureLabel = createLabel();
    temperatureInput = createTextVar();
    prepare("Temperature", temperatureLabel, temperatureInput);

    // Top K
    topKLabel = createLabel();
    topKInput = createTextVar();
    prepare("TopK", topKLabel, topKInput);

    // Top P
    topPLabel = createLabel();
    topPInput = createTextVar();
    prepare("TopP", topPLabel, topPInput);

    // Repeat Penalty
    repeatPenaltyLabel = createLabel();
    repeatPenaltyInput = createTextVar();
    repeatPenaltyLabel.setVisible(false);
    repeatPenaltyInput.setVisible(false);
    prepare("RepeatPenalty", repeatPenaltyLabel, repeatPenaltyInput);

    // Seed
    seedLabel = createLabel();
    seedInput = createTextVar();
    prepare("Seed", seedLabel, seedInput);

    // Num Predict
    numPredictLabel = createLabel();
    numPredictInput = createTextVar();
    prepare("NumPredict", numPredictLabel, numPredictInput);

    // Num Ctx
    numCtxLabel = createLabel();
    numCtxInput = createTextVar();
    prepare("NumCtx", numCtxLabel, numCtxInput);

    // Format
    formatLabel = createLabel();
    formatInput = createTextVar();
    prepare("Format", formatLabel, formatInput);

    // Timeout
    timeoutLabel = createLabel();
    timeoutInput = createTextVar();
    prepare("Timeout", timeoutLabel, timeoutInput);

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
    if (meta.getOllamaImageEndpoint() != null) {
      imageEndpointInput.setText(meta.getOllamaImageEndpoint());
    }
    if (meta.getOllamaModelName() != null) {
      modelNameInput.setText(meta.getOllamaModelName());
    }
    if (meta.getOllamaTemperature() != null) {
      temperatureInput.setText("" + meta.getOllamaTemperature());
    }
    if (meta.getOllamaTopK() != null) {
      topKInput.setText("" + meta.getOllamaTopK());
    }
    if (meta.getOllamaTopP() != null) {
      topPInput.setText("" + meta.getOllamaTopP());
    }
    if (meta.getOllamaRepeatPenalty() != null) {
      repeatPenaltyInput.setText("" + meta.getOllamaRepeatPenalty());
    }
    if (meta.getOllamaSeed() != null) {
      seedInput.setText("" + meta.getOllamaSeed());
    }
    if (meta.getOllamaNumPredict() != null) {
      numPredictInput.setText("" + meta.getOllamaNumPredict());
    }
    if (meta.getOllamaNumCtx() != null) {
      numCtxInput.setText("" + meta.getOllamaNumCtx());
    }
    if (meta.getOllamaFormat() != null) {
      formatInput.setText(meta.getOllamaFormat());
    }
    if (meta.getOllamaTimeout() != null) {
      timeoutInput.setText("" + meta.getOllamaTimeout());
    }
    if (meta.getOllamaMaxRetries() != null) {
      maxRetriesInput.setText("" + meta.getOllamaMaxRetries());
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
    meta.setOllamaImageEndpoint(trimStringToNull(imageEndpointInput.getText()));
    meta.setOllamaModelName(trimStringToNull(modelNameInput.getText()));
    meta.setOllamaTemperature(trimDoubleToNull(temperatureInput.getText()));
    meta.setOllamaTopK(trimIntegerToNull(topKInput.getText()));
    meta.setOllamaTopP(trimDoubleToNull(topPInput.getText()));
    meta.setOllamaRepeatPenalty(trimDoubleToNull(repeatPenaltyInput.getText()));
    meta.setOllamaSeed(trimIntegerToNull(seedInput.getText()));
    meta.setOllamaNumPredict(trimIntegerToNull(numPredictInput.getText()));
    meta.setOllamaNumCtx(trimIntegerToNull(numCtxInput.getText()));
    // TODO  meta.setOllamaStop();
    meta.setOllamaFormat(trimStringToNull(formatInput.getText())); // TODO Review
    meta.setOllamaTimeout(trimIntegerToNull(timeoutInput.getText()));
    meta.setOllamaMaxRetries(trimIntegerToNull(maxRetriesInput.getText()));
  }

  public Label getNumPredictLabel() {
    return numPredictLabel;
  }

  public TextVar getNumPredictInput() {
    return numPredictInput;
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

  public Label getImageEndpointLabel() {
    return imageEndpointLabel;
  }

  public TextVar getImageEndpointInput() {
    return imageEndpointInput;
  }

  public Label getNumCtxLabel() {
    return numCtxLabel;
  }

  public TextVar getNumCtxInput() {
    return numCtxInput;
  }

  public Label getTopPLabel() {
    return topPLabel;
  }

  public TextVar getTopPInput() {
    return topPInput;
  }

  public Label getTopKLabel() {
    return topKLabel;
  }

  public TextVar getTopKInput() {
    return topKInput;
  }

  public Label getFormatLabel() {
    return formatLabel;
  }

  public TextVar getFormatInput() {
    return formatInput;
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

  public Label getRepeatPenaltyLabel() {
    return repeatPenaltyLabel;
  }

  public TextVar getRepeatPenaltyInput() {
    return repeatPenaltyInput;
  }
}

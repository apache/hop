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

import static org.apache.commons.lang3.StringUtils.containsAnyIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;
import static org.apache.commons.lang3.math.NumberUtils.toDouble;
import static org.apache.commons.lang3.math.NumberUtils.toInt;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.util.Utils.isEmpty;
import static org.apache.hop.core.xml.XmlHandler.addTagValue;
import static org.apache.hop.core.xml.XmlHandler.getTagValue;
import static org.apache.hop.i18n.BaseMessages.getString;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.ANTHROPIC_CLAUDE_3_HAIKU_20240307;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.ANTHROPIC_CLAUDE_3_OPUS_20240229;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.HUGGING_FACE_LLAMA3_70B_INSTRUCT;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.MISTRAL_LARGE_LATEST;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.OLLAMA_PHI3_3_8B;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.OPENAI_GPT_4O;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.LanguageModelChatModelName.OPENAI_GPT_4O_MINI;
import static org.apache.hop.pipeline.transforms.languagemodelchat.internals.ModelType.OPEN_AI;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "LanguageModelChat",
    image = "languagemodelchat.svg",
    name = "i18n::BaseTransform.TypeLongDesc.LanguageModelChat",
    description = "i18n::BaseTransform.TypeTooltipDesc.LanguageModelChat",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/languagemodelchat.html")
public class LanguageModelChatMeta
    extends BaseTransformMeta<LanguageModelChat, LanguageModelChatData> {
  private static Class<?> PKG = LanguageModelChatMeta.class; // For Translator

  private String inputField = "input";
  private boolean inputChatJson = false;
  private boolean outputChatJson = false;
  private boolean mock = false;
  private String outputFieldNamePrefix = "llm_";
  private String mockOutputValue = "";
  private String identifierValue = "";
  private String modelType = OPEN_AI.code();
  private int parallelism = 1;

  // OpenAI
  private String openAiBaseUrl = "https://api.openai.com/v1";
  private String openAiApiKey = "OPENAI_API_KEY}";
  private String openAiOrganizationId;
  private String openAiModelName = OPENAI_GPT_4O.toString();
  private Double openAiTemperature = 0.7;
  private Double openAiTopP;
  // private List<String> openAiStop;
  private Integer openAiMaxTokens;
  private Double openAiPresencePenalty;
  private Double openAiFrequencyPenalty;
  // private Map<String, Integer> openAiLogitBias;
  private String openAiResponseFormat = "text"; // json_object
  private Integer openAiSeed;
  private String openAiUser;
  private Integer openAiTimeout = 60;
  private Integer openAiMaxRetries = 3;
  private boolean openAiUseProxy = false;
  private String openAiProxyHost = "127.0.0.1";
  private Integer openAiProxyPort = 30000;
  private boolean openAiLogRequests = false;
  private boolean openAiLogResponses = false;
  // private Tokenizer openAiTokenizer = new OpenAiTokenizer();

  // HuggingFace
  private String huggingFaceAccessToken = "HF_ACCESS_TOKEN";
  private String huggingFaceModelId = HUGGING_FACE_LLAMA3_70B_INSTRUCT.toString();
  private Integer huggingFaceTimeout = 15;
  private Double huggingFaceTemperature;
  private Integer huggingFaceMaxNewTokens;
  private boolean huggingFaceReturnFullText = false;
  private boolean huggingFaceWaitForModel = true;

  // Mistral
  private String mistralBaseUrl = "https://api.mistral.ai/v1";
  private String mistralApiKey = "MISTRAL_API_KEY";
  private String mistralModelName = MISTRAL_LARGE_LATEST.toString();
  private Double mistralTemperature;
  private Double mistralTopP;
  private Integer mistralMaxTokens;
  private boolean mistralSafePrompt = true;
  private Integer mistralRandomSeed;
  private String mistralResponseFormat;
  private Integer mistralTimeout = 60;
  private boolean mistralLogRequests = false;
  private boolean mistralLogResponses = false;
  private Integer mistralMaxRetries = 3;

  // Ollama
  private String ollamaImageEndpoint;
  private String ollamaModelName = OLLAMA_PHI3_3_8B.toString();
  private Double ollamaTemperature;
  private Integer ollamaTopK;
  private Double ollamaTopP;
  private Double ollamaRepeatPenalty;
  private Integer ollamaSeed;
  private Integer ollamaNumPredict;
  private Integer ollamaNumCtx;
  // private List<String> ollamaStop;
  private String ollamaFormat;
  private Integer ollamaTimeout = 60;
  private Integer ollamaMaxRetries = 3;

  // Anthropic
  private String anthropicBaseUrl = "https://api.anthropic.com/v1/";
  private String anthropicApiKey = "ANTHROPIC_API_KEY";
  private String anthropicVersion = "2023-06-01";
  private String anthropicModelName = ANTHROPIC_CLAUDE_3_OPUS_20240229.toString();
  private Double anthropicTemperature;
  private Double anthropicTopP;
  private Integer anthropicTopK;
  private Integer anthropicMaxTokens = 1024;
  // private List<String> anthropicStopSequences;
  private Integer anthropicTimeout = 15;
  private Integer anthropicMaxRetries = 3;
  private boolean anthropicLogRequests = false;
  private boolean anthropicLogResponses = false;

  public LanguageModelChatMeta() {
    super();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      inputField = tagToString(transformNode, "inputField");
      inputChatJson = tagToBoolean(transformNode, "inputChatJson");
      outputChatJson = tagToBoolean(transformNode, "outputChatJson");
      mock = tagToBoolean(transformNode, "mock");
      outputFieldNamePrefix = tagToString(transformNode, "outputFieldNamePrefix");
      mockOutputValue = tagToString(transformNode, "mockOutputValue");
      identifierValue = tagToString(transformNode, "identifierValue");
      modelType = tagToString(transformNode, "modelType");
      parallelism = tagToInteger(transformNode, "parallelism");

      openAiBaseUrl = tagToString(transformNode, "openAiBaseUrl");
      openAiApiKey = tagToString(transformNode, "openAiApiKey");
      openAiOrganizationId = tagToString(transformNode, "openAiOrganizationId");
      openAiModelName = tagToString(transformNode, "openAiModelName");
      openAiTemperature = tagToDouble(transformNode, "openAiTemperature");
      openAiTopP = tagToDouble(transformNode, "openAiTopP");
      openAiMaxTokens = tagToInteger(transformNode, "openAiMaxTokens");
      openAiPresencePenalty = tagToDouble(transformNode, "openAiPresencePenalty");
      openAiFrequencyPenalty = tagToDouble(transformNode, "openAiFrequencyPenalty");
      openAiResponseFormat = tagToString(transformNode, "openAiResponseFormat");
      openAiSeed = tagToInteger(transformNode, "openAiSeed");
      openAiUser = tagToString(transformNode, "openAiUser");
      openAiTimeout = tagToInteger(transformNode, "openAiTimeout");
      openAiMaxRetries = tagToInteger(transformNode, "openAiMaxRetries");
      openAiUseProxy = tagToBoolean(transformNode, "openAiUseProxy");
      openAiProxyHost = tagToString(transformNode, "openAiProxyHost");
      openAiProxyPort = tagToInteger(transformNode, "openAiProxyPort");
      openAiLogRequests = tagToBoolean(transformNode, "openAiLogRequests");
      openAiLogResponses = tagToBoolean(transformNode, "openAiLogResponses");

      huggingFaceAccessToken = tagToString(transformNode, "huggingFaceAccessToken");
      huggingFaceModelId = tagToString(transformNode, "huggingFaceModelId");
      huggingFaceTimeout = tagToInteger(transformNode, "huggingFaceTimeout");
      huggingFaceTemperature = tagToDouble(transformNode, "huggingFaceTemperature");
      huggingFaceMaxNewTokens = tagToInteger(transformNode, "huggingFaceMaxNewTokens");
      huggingFaceReturnFullText = tagToBoolean(transformNode, "huggingFaceReturnFullText");
      huggingFaceWaitForModel = tagToBoolean(transformNode, "huggingFaceWaitForModel");

      mistralBaseUrl = tagToString(transformNode, "mistralBaseUrl");
      mistralApiKey = tagToString(transformNode, "mistralApiKey");
      mistralModelName = tagToString(transformNode, "mistralModelName");
      mistralTemperature = tagToDouble(transformNode, "mistralTemperature");
      mistralTopP = tagToDouble(transformNode, "mistralTopP");
      mistralMaxTokens = tagToInteger(transformNode, "mistralMaxTokens");
      mistralSafePrompt = tagToBoolean(transformNode, "mistralSafePrompt");
      mistralRandomSeed = tagToInteger(transformNode, "mistralRandomSeed");
      mistralResponseFormat = tagToString(transformNode, "mistralResponseFormat");
      mistralTimeout = tagToInteger(transformNode, "mistralTimeout");
      mistralLogRequests = tagToBoolean(transformNode, "mistralLogRequests");
      mistralLogResponses = tagToBoolean(transformNode, "mistralLogResponses");
      mistralMaxRetries = tagToInteger(transformNode, "mistralMaxRetries");

      ollamaImageEndpoint = tagToString(transformNode, "ollamaImageEndpoint");
      ollamaModelName = tagToString(transformNode, "ollamaModelName");
      ollamaTemperature = tagToDouble(transformNode, "ollamaTemperature");
      ollamaTopK = tagToInteger(transformNode, "ollamaTopK");
      ollamaTopP = tagToDouble(transformNode, "ollamaTopP");
      ollamaRepeatPenalty = tagToDouble(transformNode, "ollamaRepeatPenalty");
      ollamaSeed = tagToInteger(transformNode, "ollamaSeed");
      ollamaNumPredict = tagToInteger(transformNode, "ollamaNumPredict");
      ollamaNumCtx = tagToInteger(transformNode, "ollamaNumCtx");
      ollamaFormat = tagToString(transformNode, "ollamaFormat");
      ollamaTimeout = tagToInteger(transformNode, "ollamaTimeout");
      ollamaMaxRetries = tagToInteger(transformNode, "ollamaMaxRetries");

      anthropicBaseUrl = tagToString(transformNode, "anthropicBaseUrl");
      anthropicApiKey = tagToString(transformNode, "anthropicApiKey");
      anthropicVersion = tagToString(transformNode, "anthropicVersion");
      anthropicModelName = tagToString(transformNode, "anthropicModelName");
      anthropicTemperature = tagToDouble(transformNode, "anthropicTemperature");
      anthropicTopP = tagToDouble(transformNode, "anthropicTopP");
      anthropicTopK = tagToInteger(transformNode, "anthropicTopK");
      anthropicMaxTokens = tagToInteger(transformNode, "anthropicMaxTokens");
      anthropicTimeout = tagToInteger(transformNode, "anthropicTimeout");
      anthropicMaxRetries = tagToInteger(transformNode, "anthropicMaxRetries");
      anthropicLogRequests = tagToBoolean(transformNode, "anthropicLogRequests");
      anthropicLogResponses = tagToBoolean(transformNode, "anthropicLogResponses");

    } catch (Exception e) {
      throw new HopXmlException(
          getString(PKG, "LanguageModelChatMeta.Exception.UnableToReadTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    inputField = "input";
    inputChatJson = false;
    outputChatJson = false;
    mock = false;
    outputFieldNamePrefix = "llm_";
    mockOutputValue = "";
    identifierValue = "";
    modelType = OPEN_AI.code();
    parallelism = 1;

    openAiBaseUrl = "https://api.openai.com/v1";
    openAiApiKey = "OPENAI_API_KEY";
    openAiModelName = OPENAI_GPT_4O_MINI.toString();
    openAiResponseFormat = "text";
    openAiTemperature = 0.7;
    openAiTimeout = 60;
    openAiMaxRetries = 3;
    openAiUseProxy = false;
    openAiProxyHost = "127.0.0.1";
    openAiProxyPort = 30000;
    openAiLogRequests = false;
    openAiLogResponses = false;

    huggingFaceAccessToken = "HF_ACCESS_TOKEN";
    huggingFaceModelId = HUGGING_FACE_LLAMA3_70B_INSTRUCT.toString();
    huggingFaceTimeout = 15;
    huggingFaceReturnFullText = false;
    huggingFaceWaitForModel = true;

    mistralBaseUrl = "https://api.mistral.ai/v1";
    mistralApiKey = "MISTRAL_API_KEY";
    mistralModelName = MISTRAL_LARGE_LATEST.toString();
    mistralSafePrompt = true;
    mistralTimeout = 60;
    mistralLogRequests = false;
    mistralLogResponses = false;
    mistralMaxRetries = 3;

    ollamaImageEndpoint = "";
    ollamaModelName = OLLAMA_PHI3_3_8B.toString();
    ollamaTimeout = 60;
    ollamaMaxRetries = 3;

    anthropicBaseUrl = "https://api.anthropic.com/v1/";
    anthropicApiKey = "ANTHROPIC_API_KEY";
    anthropicVersion = "2023-06-01";
    anthropicModelName = ANTHROPIC_CLAUDE_3_HAIKU_20240307.toString();
    anthropicMaxTokens = 1024;
    anthropicTimeout = 15;
    anthropicMaxRetries = 3;
    anthropicLogRequests = false;
    anthropicLogResponses = false;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    valueMetaString(r, name, variables.resolve(outputFieldNamePrefix + "identifier"));
    valueMetaString(r, name, variables.resolve(outputFieldNamePrefix + "model_type"));
    valueMetaString(r, name, variables.resolve(outputFieldNamePrefix + "model_name"));
    valueMetaString(r, name, variables.resolve(outputFieldNamePrefix + "finish_reason"));
    valueMetaInteger(r, name, variables.resolve(outputFieldNamePrefix + "input_token_count"));
    valueMetaInteger(r, name, variables.resolve(outputFieldNamePrefix + "output_token_count"));
    valueMetaInteger(r, name, variables.resolve(outputFieldNamePrefix + "total_token_count"));
    valueMetaInteger(r, name, variables.resolve(outputFieldNamePrefix + "inference_time"));
    valueMetaString(r, name, variables.resolve(outputFieldNamePrefix + "output"));
  }

  private void valueMetaString(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaString(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  private void valueMetaBoolean(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaBoolean(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  private void valueMetaInteger(IRowMeta r, String name, String metaName) {
    IValueMeta sText = new ValueMetaInteger(metaName);
    sText.setOrigin(name);
    r.addValueMeta(sText);
  }

  @Override
  public String getXml() {
    return toTag("inputField", inputField)
        + toTag("inputChatJson", inputChatJson)
        + toTag("outputChatJson", outputChatJson)
        + toTag("mock", mock)
        + toTag("mockOutputValue", mockOutputValue)
        + toTag("identifierValue", identifierValue)
        + toTag("outputFieldNamePrefix", outputFieldNamePrefix)
        + toTag("modelType", modelType)
        + toTag("parallelism", parallelism)
        + toTag("openAiBaseUrl", openAiBaseUrl)
        + toTag("openAiApiKey", openAiApiKey)
        + toTag("openAiOrganizationId", openAiOrganizationId)
        + toTag("openAiModelName", openAiModelName)
        + toTag("openAiTemperature", openAiTemperature)
        + toTag("openAiTopP", openAiTopP)
        + toTag("openAiMaxTokens", openAiMaxTokens)
        + toTag("openAiPresencePenalty", openAiPresencePenalty)
        + toTag("openAiFrequencyPenalty", openAiFrequencyPenalty)
        + toTag("openAiResponseFormat", openAiResponseFormat)
        + toTag("openAiSeed", openAiSeed)
        + toTag("openAiUser", openAiUser)
        + toTag("openAiTimeout", openAiTimeout)
        + toTag("openAiMaxRetries", openAiMaxRetries)
        + toTag("openAiUseProxy", openAiUseProxy)
        + toTag("openAiProxyHost", openAiProxyHost)
        + toTag("openAiProxyPort", openAiProxyPort)
        + toTag("openAiLogRequests", openAiLogRequests)
        + toTag("openAiLogResponses", openAiLogResponses)
        + toTag("huggingFaceAccessToken", huggingFaceAccessToken)
        + toTag("huggingFaceModelId", huggingFaceModelId)
        + toTag("huggingFaceTimeout", huggingFaceTimeout)
        + toTag("huggingFaceTemperature", huggingFaceTemperature)
        + toTag("huggingFaceMaxNewTokens", huggingFaceMaxNewTokens)
        + toTag("huggingFaceReturnFullText", huggingFaceReturnFullText)
        + toTag("huggingFaceWaitForModel", huggingFaceWaitForModel)
        + toTag("mistralBaseUrl", mistralBaseUrl)
        + toTag("mistralApiKey", mistralApiKey)
        + toTag("mistralModelName", mistralModelName)
        + toTag("mistralTemperature", mistralTemperature)
        + toTag("mistralTopP", mistralTopP)
        + toTag("mistralMaxTokens", mistralMaxTokens)
        + toTag("mistralSafePrompt", mistralSafePrompt)
        + toTag("mistralRandomSeed", mistralRandomSeed)
        + toTag("mistralResponseFormat", mistralResponseFormat)
        + toTag("mistralTimeout", mistralTimeout)
        + toTag("mistralLogRequests", mistralLogRequests)
        + toTag("mistralLogResponses", mistralLogResponses)
        + toTag("mistralMaxRetries", mistralMaxRetries)
        + toTag("ollamaImageEndpoint", ollamaImageEndpoint)
        + toTag("ollamaModelName", ollamaModelName)
        + toTag("ollamaTemperature", ollamaTemperature)
        + toTag("ollamaTopK", ollamaTopK)
        + toTag("ollamaTopP", ollamaTopP)
        + toTag("ollamaRepeatPenalty", ollamaRepeatPenalty)
        + toTag("ollamaSeed", ollamaSeed)
        + toTag("ollamaNumPredict", ollamaNumPredict)
        + toTag("ollamaNumCtx", ollamaNumCtx)
        + toTag("ollamaFormat", ollamaFormat)
        + toTag("ollamaTimeout", ollamaTimeout)
        + toTag("ollamaMaxRetries", ollamaMaxRetries)
        + toTag("anthropicBaseUrl", anthropicBaseUrl)
        + toTag("anthropicApiKey", anthropicApiKey)
        + toTag("anthropicVersion", anthropicVersion)
        + toTag("anthropicModelName", anthropicModelName)
        + toTag("anthropicTemperature", anthropicTemperature)
        + toTag("anthropicTopP", anthropicTopP)
        + toTag("anthropicTopK", anthropicTopK)
        + toTag("anthropicMaxTokens", anthropicMaxTokens)
        + toTag("anthropicTimeout", anthropicTimeout)
        + toTag("anthropicMaxRetries", anthropicMaxRetries)
        + toTag("anthropicLogRequests", anthropicLogRequests)
        + toTag("anthropicLogResponses", anthropicLogResponses);
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    if (isEmpty(inputField)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "LanguageModelChatMeta.CheckResult.InputFieldMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "LanguageModelChatMeta.CheckResult.InputFieldOK"),
              transformMeta);
    }
    remarks.add(cr);

    if (isEmpty(outputFieldNamePrefix)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "LanguageModelChatMeta.CheckResult.OutputFieldNamePrefixMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "LanguageModelChatMeta.CheckResult.OutputFieldNamePrefixOK"),
              transformMeta);
    }
    remarks.add(cr);

    if (isEmpty(modelType)) {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "LanguageModelChatMeta.CheckResult.ModelTypeMissing"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "LanguageModelChatMeta.CheckResult.ModelTypeOK"),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              TYPE_RESULT_OK,
              getString(PKG, "LanguageModelChatMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              TYPE_RESULT_ERROR,
              getString(PKG, "LanguageModelChatMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public String getInputField() {
    return inputField;
  }

  public void setInputField(String inputField) {
    this.inputField = inputField;
  }

  public boolean isInputChatJson() {
    return inputChatJson;
  }

  public void setInputChatJson(boolean inputChatJson) {
    this.inputChatJson = inputChatJson;
  }

  public boolean isOutputChatJson() {
    return outputChatJson;
  }

  public void setOutputChatJson(boolean outputChatJson) {
    this.outputChatJson = outputChatJson;
  }

  public String getOutputFieldNamePrefix() {
    return outputFieldNamePrefix;
  }

  public void setOutputFieldNamePrefix(String outputFieldNamePrefix) {
    this.outputFieldNamePrefix = outputFieldNamePrefix;
  }

  public String getModelType() {
    return modelType;
  }

  public void setModelType(String modelType) {
    this.modelType = modelType;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public String getOpenAiBaseUrl() {
    return openAiBaseUrl;
  }

  public void setOpenAiBaseUrl(String openAiBaseUrl) {
    this.openAiBaseUrl = openAiBaseUrl;
  }

  public String getOpenAiApiKey() {
    return openAiApiKey;
  }

  public void setOpenAiApiKey(String openAiApiKey) {
    this.openAiApiKey = openAiApiKey;
  }

  public String getOpenAiOrganizationId() {
    return openAiOrganizationId;
  }

  public void setOpenAiOrganizationId(String openAiOrganizationId) {
    this.openAiOrganizationId = openAiOrganizationId;
  }

  public String getOpenAiModelName() {
    return openAiModelName;
  }

  public void setOpenAiModelName(String openAiModelName) {
    this.openAiModelName = openAiModelName;
  }

  public Double getOpenAiTemperature() {
    return openAiTemperature;
  }

  public void setOpenAiTemperature(Double openAiTemperature) {
    this.openAiTemperature = openAiTemperature;
  }

  public Double getOpenAiTopP() {
    return openAiTopP;
  }

  public void setOpenAiTopP(Double openAiTopP) {
    this.openAiTopP = openAiTopP;
  }

  public Integer getOpenAiMaxTokens() {
    return openAiMaxTokens;
  }

  public void setOpenAiMaxTokens(Integer openAiMaxTokens) {
    this.openAiMaxTokens = openAiMaxTokens;
  }

  public Double getOpenAiPresencePenalty() {
    return openAiPresencePenalty;
  }

  public void setOpenAiPresencePenalty(Double openAiPresencePenalty) {
    this.openAiPresencePenalty = openAiPresencePenalty;
  }

  public Double getOpenAiFrequencyPenalty() {
    return openAiFrequencyPenalty;
  }

  public void setOpenAiFrequencyPenalty(Double openAiFrequencyPenalty) {
    this.openAiFrequencyPenalty = openAiFrequencyPenalty;
  }

  public String getOpenAiResponseFormat() {
    return openAiResponseFormat;
  }

  public void setOpenAiResponseFormat(String openAiResponseFormat) {
    this.openAiResponseFormat = openAiResponseFormat;
  }

  public Integer getOpenAiSeed() {
    return openAiSeed;
  }

  public void setOpenAiSeed(Integer openAiSeed) {
    this.openAiSeed = openAiSeed;
  }

  public String getOpenAiUser() {
    return openAiUser;
  }

  public void setOpenAiUser(String openAiUser) {
    this.openAiUser = openAiUser;
  }

  public Integer getOpenAiTimeout() {
    return openAiTimeout;
  }

  public void setOpenAiTimeout(Integer openAiTimeout) {
    this.openAiTimeout = openAiTimeout;
  }

  public Integer getOpenAiMaxRetries() {
    return openAiMaxRetries;
  }

  public void setOpenAiMaxRetries(Integer openAiMaxRetries) {
    this.openAiMaxRetries = openAiMaxRetries;
  }

  public boolean isOpenAiUseProxy() {
    return openAiUseProxy;
  }

  public void setOpenAiUseProxy(boolean openAiUseProxy) {
    this.openAiUseProxy = openAiUseProxy;
  }

  public String getOpenAiProxyHost() {
    return openAiProxyHost;
  }

  public void setOpenAiProxyHost(String openAiProxyHost) {
    this.openAiProxyHost = openAiProxyHost;
  }

  public Integer getOpenAiProxyPort() {
    return openAiProxyPort;
  }

  public void setOpenAiProxyPort(Integer openAiProxyPort) {
    this.openAiProxyPort = openAiProxyPort;
  }

  public boolean isOpenAiLogRequests() {
    return openAiLogRequests;
  }

  public void setOpenAiLogRequests(boolean openAiLogRequests) {
    this.openAiLogRequests = openAiLogRequests;
  }

  public boolean isOpenAiLogResponses() {
    return openAiLogResponses;
  }

  public void setOpenAiLogResponses(boolean openAiLogResponses) {
    this.openAiLogResponses = openAiLogResponses;
  }

  public String getHuggingFaceAccessToken() {
    return huggingFaceAccessToken;
  }

  public void setHuggingFaceAccessToken(String huggingFaceAccessToken) {
    this.huggingFaceAccessToken = huggingFaceAccessToken;
  }

  public String getHuggingFaceModelId() {
    return huggingFaceModelId;
  }

  public void setHuggingFaceModelId(String huggingFaceModelId) {
    this.huggingFaceModelId = huggingFaceModelId;
  }

  public Integer getHuggingFaceTimeout() {
    return huggingFaceTimeout;
  }

  public void setHuggingFaceTimeout(Integer huggingFaceTimeout) {
    this.huggingFaceTimeout = huggingFaceTimeout;
  }

  public Double getHuggingFaceTemperature() {
    return huggingFaceTemperature;
  }

  public void setHuggingFaceTemperature(Double huggingFaceTemperature) {
    this.huggingFaceTemperature = huggingFaceTemperature;
  }

  public Integer getHuggingFaceMaxNewTokens() {
    return huggingFaceMaxNewTokens;
  }

  public void setHuggingFaceMaxNewTokens(Integer huggingFaceMaxNewTokens) {
    this.huggingFaceMaxNewTokens = huggingFaceMaxNewTokens;
  }

  public boolean isHuggingFaceReturnFullText() {
    return huggingFaceReturnFullText;
  }

  public void setHuggingFaceReturnFullText(boolean huggingFaceReturnFullText) {
    this.huggingFaceReturnFullText = huggingFaceReturnFullText;
  }

  public boolean isHuggingFaceWaitForModel() {
    return huggingFaceWaitForModel;
  }

  public void setHuggingFaceWaitForModel(boolean huggingFaceWaitForModel) {
    this.huggingFaceWaitForModel = huggingFaceWaitForModel;
  }

  public String getMistralBaseUrl() {
    return mistralBaseUrl;
  }

  public void setMistralBaseUrl(String mistralBaseUrl) {
    this.mistralBaseUrl = mistralBaseUrl;
  }

  public String getMistralApiKey() {
    return mistralApiKey;
  }

  public void setMistralApiKey(String mistralApiKey) {
    this.mistralApiKey = mistralApiKey;
  }

  public String getMistralModelName() {
    return mistralModelName;
  }

  public void setMistralModelName(String mistralModelName) {
    this.mistralModelName = mistralModelName;
  }

  public Double getMistralTemperature() {
    return mistralTemperature;
  }

  public void setMistralTemperature(Double mistralTemperature) {
    this.mistralTemperature = mistralTemperature;
  }

  public Double getMistralTopP() {
    return mistralTopP;
  }

  public void setMistralTopP(Double mistralTopP) {
    this.mistralTopP = mistralTopP;
  }

  public Integer getMistralMaxTokens() {
    return mistralMaxTokens;
  }

  public void setMistralMaxTokens(Integer mistralMaxTokens) {
    this.mistralMaxTokens = mistralMaxTokens;
  }

  public boolean isMistralSafePrompt() {
    return mistralSafePrompt;
  }

  public void setMistralSafePrompt(boolean mistralSafePrompt) {
    this.mistralSafePrompt = mistralSafePrompt;
  }

  public Integer getMistralRandomSeed() {
    return mistralRandomSeed;
  }

  public void setMistralRandomSeed(Integer mistralRandomSeed) {
    this.mistralRandomSeed = mistralRandomSeed;
  }

  public String getMistralResponseFormat() {
    return mistralResponseFormat;
  }

  public void setMistralResponseFormat(String mistralResponseFormat) {
    this.mistralResponseFormat = mistralResponseFormat;
  }

  public Integer getMistralTimeout() {
    return mistralTimeout;
  }

  public void setMistralTimeout(Integer mistralTimeout) {
    this.mistralTimeout = mistralTimeout;
  }

  public boolean isMistralLogRequests() {
    return mistralLogRequests;
  }

  public void setMistralLogRequests(boolean mistralLogRequests) {
    this.mistralLogRequests = mistralLogRequests;
  }

  public boolean isMistralLogResponses() {
    return mistralLogResponses;
  }

  public void setMistralLogResponses(boolean mistralLogResponses) {
    this.mistralLogResponses = mistralLogResponses;
  }

  public Integer getMistralMaxRetries() {
    return mistralMaxRetries;
  }

  public void setMistralMaxRetries(Integer mistralMaxRetries) {
    this.mistralMaxRetries = mistralMaxRetries;
  }

  public String getOllamaImageEndpoint() {
    return ollamaImageEndpoint;
  }

  public void setOllamaImageEndpoint(String ollamaImageEndpoint) {
    this.ollamaImageEndpoint = ollamaImageEndpoint;
  }

  public String getOllamaModelName() {
    return ollamaModelName;
  }

  public void setOllamaModelName(String ollamaModelName) {
    this.ollamaModelName = ollamaModelName;
  }

  public Double getOllamaTemperature() {
    return ollamaTemperature;
  }

  public void setOllamaTemperature(Double ollamaTemperature) {
    this.ollamaTemperature = ollamaTemperature;
  }

  public Integer getOllamaTopK() {
    return ollamaTopK;
  }

  public void setOllamaTopK(Integer ollamaTopK) {
    this.ollamaTopK = ollamaTopK;
  }

  public Double getOllamaTopP() {
    return ollamaTopP;
  }

  public void setOllamaTopP(Double ollamaTopP) {
    this.ollamaTopP = ollamaTopP;
  }

  public Double getOllamaRepeatPenalty() {
    return ollamaRepeatPenalty;
  }

  public void setOllamaRepeatPenalty(Double ollamaRepeatPenalty) {
    this.ollamaRepeatPenalty = ollamaRepeatPenalty;
  }

  public Integer getOllamaSeed() {
    return ollamaSeed;
  }

  public void setOllamaSeed(Integer ollamaSeed) {
    this.ollamaSeed = ollamaSeed;
  }

  public Integer getOllamaNumPredict() {
    return ollamaNumPredict;
  }

  public void setOllamaNumPredict(Integer ollamaNumPredict) {
    this.ollamaNumPredict = ollamaNumPredict;
  }

  public Integer getOllamaNumCtx() {
    return ollamaNumCtx;
  }

  public void setOllamaNumCtx(Integer ollamaNumCtx) {
    this.ollamaNumCtx = ollamaNumCtx;
  }

  public String getOllamaFormat() {
    return ollamaFormat;
  }

  public void setOllamaFormat(String ollamaFormat) {
    this.ollamaFormat = ollamaFormat;
  }

  public Integer getOllamaTimeout() {
    return ollamaTimeout;
  }

  public void setOllamaTimeout(Integer ollamaTimeout) {
    this.ollamaTimeout = ollamaTimeout;
  }

  public Integer getOllamaMaxRetries() {
    return ollamaMaxRetries;
  }

  public void setOllamaMaxRetries(Integer ollamaMaxRetries) {
    this.ollamaMaxRetries = ollamaMaxRetries;
  }

  public String getAnthropicBaseUrl() {
    return anthropicBaseUrl;
  }

  public void setAnthropicBaseUrl(String anthropicBaseUrl) {
    this.anthropicBaseUrl = anthropicBaseUrl;
  }

  public String getAnthropicApiKey() {
    return anthropicApiKey;
  }

  public void setAnthropicApiKey(String anthropicApiKey) {
    this.anthropicApiKey = anthropicApiKey;
  }

  public String getAnthropicVersion() {
    return anthropicVersion;
  }

  public void setAnthropicVersion(String anthropicVersion) {
    this.anthropicVersion = anthropicVersion;
  }

  public String getAnthropicModelName() {
    return anthropicModelName;
  }

  public void setAnthropicModelName(String anthropicModelName) {
    this.anthropicModelName = anthropicModelName;
  }

  public Double getAnthropicTemperature() {
    return anthropicTemperature;
  }

  public void setAnthropicTemperature(Double anthropicTemperature) {
    this.anthropicTemperature = anthropicTemperature;
  }

  public Double getAnthropicTopP() {
    return anthropicTopP;
  }

  public void setAnthropicTopP(Double anthropicTopP) {
    this.anthropicTopP = anthropicTopP;
  }

  public Integer getAnthropicTopK() {
    return anthropicTopK;
  }

  public void setAnthropicTopK(Integer anthropicTopK) {
    this.anthropicTopK = anthropicTopK;
  }

  public Integer getAnthropicMaxTokens() {
    return anthropicMaxTokens;
  }

  public void setAnthropicMaxTokens(Integer anthropicMaxTokens) {
    this.anthropicMaxTokens = anthropicMaxTokens;
  }

  public Integer getAnthropicTimeout() {
    return anthropicTimeout;
  }

  public void setAnthropicTimeout(Integer anthropicTimeout) {
    this.anthropicTimeout = anthropicTimeout;
  }

  public Integer getAnthropicMaxRetries() {
    return anthropicMaxRetries;
  }

  public void setAnthropicMaxRetries(Integer anthropicMaxRetries) {
    this.anthropicMaxRetries = anthropicMaxRetries;
  }

  public boolean isAnthropicLogRequests() {
    return anthropicLogRequests;
  }

  public void setAnthropicLogRequests(boolean anthropicLogRequests) {
    this.anthropicLogRequests = anthropicLogRequests;
  }

  public boolean isAnthropicLogResponses() {
    return anthropicLogResponses;
  }

  public void setAnthropicLogResponses(boolean anthropicLogResponses) {
    this.anthropicLogResponses = anthropicLogResponses;
  }

  private String toTag(String key, Object value) {
    String v = value == null ? "" : value.toString();
    return isBlank(v) ? "" : "    " + addTagValue(key, v);
  }

  private String tagToString(Node node, String tag) {
    return getTagValue(node, tag);
  }

  private boolean tagToBoolean(Node node, String tag) {
    return containsAnyIgnoreCase(tagToString(node, tag), "y", "yes", "1", "true");
  }

  private Integer tagToInteger(Node node, String tag) {
    String v = tagToString(node, tag);
    return isBlank(v) || !isCreatable(v) ? null : toInt(v);
  }

  private Double tagToDouble(Node node, String tag) {
    String v = tagToString(node, tag);
    return isBlank(v) || !isCreatable(v) ? null : toDouble(v);
  }

  public boolean isMock() {
    return mock;
  }

  public void setMock(boolean mock) {
    this.mock = mock;
  }

  public String getMockOutputValue() {
    return mockOutputValue;
  }

  public void setMockOutputValue(String mockOutputValue) {
    this.mockOutputValue = mockOutputValue;
  }

  public String getIdentifierValue() {
    return identifierValue;
  }

  public void setIdentifierValue(String identifierValue) {
    this.identifierValue = identifierValue;
  }
}

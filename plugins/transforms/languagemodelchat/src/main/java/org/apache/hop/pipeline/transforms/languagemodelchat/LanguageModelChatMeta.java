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

import static org.apache.hop.core.ICheckResult.TYPE_RESULT_ERROR;
import static org.apache.hop.core.ICheckResult.TYPE_RESULT_OK;
import static org.apache.hop.core.util.Utils.isEmpty;
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
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "LanguageModelChat",
    image = "languagemodelchat.svg",
    name = "i18n::BaseTransform.TypeLongDesc.LanguageModelChat",
    description = "i18n::BaseTransform.TypeTooltipDesc.LanguageModelChat",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/languagemodelchat.html")
@Getter
@Setter
public class LanguageModelChatMeta
    extends BaseTransformMeta<LanguageModelChat, LanguageModelChatData> {
  private static final Class<?> PKG = LanguageModelChatMeta.class; // For Translator

  @HopMetadataProperty private String inputField = "input";
  @HopMetadataProperty private boolean inputChatJson = false;
  @HopMetadataProperty private boolean outputChatJson = false;
  @HopMetadataProperty private boolean mock = false;
  @HopMetadataProperty private String outputFieldNamePrefix = "llm_";
  @HopMetadataProperty private String mockOutputValue = "";
  @HopMetadataProperty private String identifierValue = "";
  @HopMetadataProperty private String modelType = OPEN_AI.code();
  @HopMetadataProperty private int parallelism = 1;

  // OpenAI
  @HopMetadataProperty private String openAiBaseUrl = "https://api.openai.com/v1";
  @HopMetadataProperty private String openAiApiKey = "OPENAI_API_KEY}";
  @HopMetadataProperty private String openAiOrganizationId;
  @HopMetadataProperty private String openAiModelName = OPENAI_GPT_4O.toString();
  @HopMetadataProperty private Double openAiTemperature = 0.7;
  @HopMetadataProperty private Double openAiTopP;
  // TODO private List<String> openAiStop;
  @HopMetadataProperty private Integer openAiMaxTokens;
  @HopMetadataProperty private Double openAiPresencePenalty;
  @HopMetadataProperty private Double openAiFrequencyPenalty;
  // TODO private Map<String, Integer> openAiLogitBias;
  @HopMetadataProperty private String openAiResponseFormat = "text"; // json_object
  @HopMetadataProperty private Integer openAiSeed;
  @HopMetadataProperty private String openAiUser;
  @HopMetadataProperty private Integer openAiTimeout = 60;
  @HopMetadataProperty private Integer openAiMaxRetries = 3;
  @HopMetadataProperty private boolean openAiUseProxy = false;
  @HopMetadataProperty private String openAiProxyHost = "127.0.0.1";
  @HopMetadataProperty private Integer openAiProxyPort = 30000;
  @HopMetadataProperty private boolean openAiLogRequests = false;
  @HopMetadataProperty private boolean openAiLogResponses = false;
  // TODO private Tokenizer openAiTokenizer = new OpenAiTokenizer();

  // HuggingFace
  @HopMetadataProperty private String huggingFaceAccessToken = "HF_ACCESS_TOKEN";

  @HopMetadataProperty
  private String huggingFaceModelId = HUGGING_FACE_LLAMA3_70B_INSTRUCT.toString();

  @HopMetadataProperty private Integer huggingFaceTimeout = 15;
  @HopMetadataProperty private Double huggingFaceTemperature;
  @HopMetadataProperty private Integer huggingFaceMaxNewTokens;
  @HopMetadataProperty private boolean huggingFaceReturnFullText = false;
  @HopMetadataProperty private boolean huggingFaceWaitForModel = true;

  // Mistral
  @HopMetadataProperty private String mistralBaseUrl = "https://api.mistral.ai/v1";
  @HopMetadataProperty private String mistralApiKey = "MISTRAL_API_KEY";
  @HopMetadataProperty private String mistralModelName = MISTRAL_LARGE_LATEST.toString();
  @HopMetadataProperty private Double mistralTemperature;
  @HopMetadataProperty private Double mistralTopP;
  @HopMetadataProperty private Integer mistralMaxTokens;
  @HopMetadataProperty private boolean mistralSafePrompt = true;
  @HopMetadataProperty private Integer mistralRandomSeed;
  @HopMetadataProperty private String mistralResponseFormat;
  @HopMetadataProperty private Integer mistralTimeout = 60;
  @HopMetadataProperty private boolean mistralLogRequests = false;
  @HopMetadataProperty private boolean mistralLogResponses = false;
  @HopMetadataProperty private Integer mistralMaxRetries = 3;

  // Ollama
  @HopMetadataProperty private String ollamaImageEndpoint;
  @HopMetadataProperty private String ollamaModelName = OLLAMA_PHI3_3_8B.toString();
  @HopMetadataProperty private Double ollamaTemperature;
  @HopMetadataProperty private Integer ollamaTopK;
  @HopMetadataProperty private Double ollamaTopP;
  @HopMetadataProperty private Double ollamaRepeatPenalty;
  @HopMetadataProperty private Integer ollamaSeed;
  @HopMetadataProperty private Integer ollamaNumPredict;
  @HopMetadataProperty private Integer ollamaNumCtx;
  // TODO private List<String> ollamaStop;
  @HopMetadataProperty private String ollamaFormat;
  @HopMetadataProperty private Integer ollamaTimeout = 60;
  @HopMetadataProperty private Integer ollamaMaxRetries = 3;

  // Anthropic
  @HopMetadataProperty private String anthropicBaseUrl = "https://api.anthropic.com/v1/";
  @HopMetadataProperty private String anthropicApiKey = "ANTHROPIC_API_KEY";
  @HopMetadataProperty private String anthropicVersion = "2023-06-01";

  @HopMetadataProperty
  private String anthropicModelName = ANTHROPIC_CLAUDE_3_OPUS_20240229.toString();

  @HopMetadataProperty private Double anthropicTemperature;
  @HopMetadataProperty private Double anthropicTopP;
  @HopMetadataProperty private Integer anthropicTopK;
  @HopMetadataProperty private Integer anthropicMaxTokens = 1024;
  // TODO private List<String> anthropicStopSequences;
  @HopMetadataProperty private Integer anthropicTimeout = 15;
  @HopMetadataProperty private Integer anthropicMaxRetries = 3;
  @HopMetadataProperty private boolean anthropicLogRequests = false;
  @HopMetadataProperty private boolean anthropicLogResponses = false;

  public LanguageModelChatMeta() {
    super();
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
}

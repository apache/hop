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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.time.Duration;
import java.util.Objects;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.ChatMessages;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface.HuggingFaceClient.Options;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface.HuggingFaceClient.Parameters;
import org.apache.hop.pipeline.transforms.languagemodelchat.internals.huggingface.HuggingFaceClient.TextGenerationRequest;

/**
 * A chat model on top of the Hugging Face text generation API. The langchain4j Hugging Face module
 * is a beta artifact that upstream deprecated for removal, so the API is called here directly.
 */
public class HuggingFaceChatModel implements ChatModel {

  private final HuggingFaceClient client;
  private final Double temperature;
  private final Integer maxNewTokens;
  private final Boolean returnFullText;
  private final Boolean waitForModel;

  public HuggingFaceChatModel(Builder builder) {
    if (isBlank(builder.accessToken)) {
      throw new IllegalArgumentException(
          "HuggingFace access token must be defined. It can be generated here: https://huggingface.co/settings/tokens");
    }
    if (isBlank(builder.modelResource)) {
      throw new IllegalArgumentException(
          "HuggingFace model id or dedicated endpoint URL must be defined.");
    }
    this.client =
        new HuggingFaceClient(builder.modelResource, builder.accessToken, builder.timeout);
    this.temperature = builder.temperature;
    this.maxNewTokens = builder.maxNewTokens;
    this.returnFullText = builder.returnFullText;
    this.waitForModel = builder.waitForModel;
  }

  @Override
  public ChatResponse doChat(ChatRequest chatRequest) {
    String inputs =
        chatRequest.messages().stream()
            .map(ChatMessages::text)
            .filter(Objects::nonNull)
            .collect(joining("\n"));

    TextGenerationRequest request =
        new TextGenerationRequest(
            inputs,
            new Parameters(temperature, maxNewTokens, returnFullText),
            new Options(waitForModel));

    return ChatResponse.builder().aiMessage(AiMessage.from(client.generate(request))).build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String accessToken;
    private String modelResource;
    private Duration timeout = Duration.ofSeconds(15);
    private Double temperature;
    private Integer maxNewTokens;
    private Boolean returnFullText = false;
    private Boolean waitForModel = true;

    public Builder accessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    /** A Hugging Face model id, or the URL of a dedicated inference endpoint. */
    public Builder modelResource(String modelResource) {
      this.modelResource = modelResource;
      return this;
    }

    public Builder timeout(Duration timeout) {
      if (timeout != null) {
        this.timeout = timeout;
      }
      return this;
    }

    public Builder temperature(Double temperature) {
      this.temperature = temperature;
      return this;
    }

    public Builder maxNewTokens(Integer maxNewTokens) {
      this.maxNewTokens = maxNewTokens;
      return this;
    }

    public Builder returnFullText(Boolean returnFullText) {
      if (returnFullText != null) {
        this.returnFullText = returnFullText;
      }
      return this;
    }

    public Builder waitForModel(Boolean waitForModel) {
      if (waitForModel != null) {
        this.waitForModel = waitForModel;
      }
      return this;
    }

    public HuggingFaceChatModel build() {
      return new HuggingFaceChatModel(this);
    }
  }
}

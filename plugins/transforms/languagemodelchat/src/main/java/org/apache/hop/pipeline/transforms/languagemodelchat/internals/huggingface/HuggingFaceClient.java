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

import static com.squareup.moshi.Types.newParameterizedType;
import static dev.langchain4j.http.client.HttpMethod.POST;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;

import com.squareup.moshi.Json;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import dev.langchain4j.exception.HttpException;
import dev.langchain4j.http.client.HttpClient;
import dev.langchain4j.http.client.HttpRequest;
import dev.langchain4j.http.client.SuccessfulHttpResponse;
import dev.langchain4j.http.client.jdk.JdkHttpClientBuilder;
import java.time.Duration;
import java.util.List;
import org.apache.hop.core.exception.HopRuntimeException;

/**
 * Calls the Hugging Face text generation API. A model id is sent to the Hugging Face inference
 * router, an http(s) model resource is taken to be a dedicated inference endpoint and is called as
 * it stands.
 */
public class HuggingFaceClient {

  private static final String ROUTER_URL = "https://router.huggingface.co/hf-inference/models/";

  private final HttpClient httpClient;
  private final JsonAdapter<TextGenerationRequest> requestAdapter;
  private final JsonAdapter<List<TextGenerationResponse>> responseAdapter;
  private final String url;
  private final String accessToken;

  public HuggingFaceClient(String modelResource, String accessToken, Duration timeout) {
    this.httpClient =
        new JdkHttpClientBuilder().connectTimeout(timeout).readTimeout(timeout).build();
    this.url = isDedicatedEndpoint(modelResource) ? modelResource : ROUTER_URL + modelResource;
    this.accessToken = accessToken;

    Moshi moshi = new Moshi.Builder().build();
    this.requestAdapter = moshi.adapter(TextGenerationRequest.class);
    this.responseAdapter =
        moshi.adapter(newParameterizedType(List.class, TextGenerationResponse.class));
  }

  public static boolean isDedicatedEndpoint(String modelResource) {
    return startsWithIgnoreCase(modelResource, "http://")
        || startsWithIgnoreCase(modelResource, "https://");
  }

  public String generate(TextGenerationRequest request) {
    HttpRequest httpRequest =
        HttpRequest.builder()
            .method(POST)
            .url(url)
            .addHeader("Content-Type", "application/json")
            .addHeader("Authorization", "Bearer " + accessToken)
            .body(requestAdapter.toJson(request))
            .build();

    SuccessfulHttpResponse httpResponse;
    try {
      httpResponse = httpClient.execute(httpRequest);
    } catch (HttpException e) {
      throw new HopRuntimeException(
          "status code: " + e.statusCode() + "; body: " + e.getMessage(), e);
    }

    List<TextGenerationResponse> responses;
    try {
      responses = responseAdapter.fromJson(httpResponse.body());
    } catch (Exception e) {
      throw new HopRuntimeException("Could not read the Hugging Face response", e);
    }

    if (responses == null || responses.size() != 1) {
      throw new HopRuntimeException(
          "Expected only one generated_text, but was: "
              + (responses == null ? 0 : responses.size()));
    }

    return responses.get(0).generatedText;
  }

  public static class TextGenerationRequest {
    private String inputs;
    private Parameters parameters;
    private Options options;

    public TextGenerationRequest(String inputs, Parameters parameters, Options options) {
      this.inputs = inputs;
      this.parameters = parameters;
      this.options = options;
    }
  }

  public static class Parameters {
    private Double temperature;

    @Json(name = "max_new_tokens")
    private Integer maxNewTokens;

    @Json(name = "return_full_text")
    private Boolean returnFullText;

    public Parameters(Double temperature, Integer maxNewTokens, Boolean returnFullText) {
      this.temperature = temperature;
      this.maxNewTokens = maxNewTokens;
      this.returnFullText = returnFullText;
    }
  }

  public static class Options {
    @Json(name = "wait_for_model")
    private Boolean waitForModel;

    public Options(Boolean waitForModel) {
      this.waitForModel = waitForModel;
    }
  }

  public static class TextGenerationResponse {
    @Json(name = "generated_text")
    private String generatedText;
  }
}

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

package dev.langchain4j.model.huggingface;

import static com.google.gson.FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES;
import static dev.langchain4j.internal.ValidationUtils.ensureNotBlank;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dev.langchain4j.model.huggingface.client.EmbeddingRequest;
import dev.langchain4j.model.huggingface.client.HuggingFaceClient;
import dev.langchain4j.model.huggingface.client.TextGenerationRequest;
import dev.langchain4j.model.huggingface.client.TextGenerationResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import okhttp3.OkHttpClient;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

class DedicatedEndpointHuggingFaceClient implements HuggingFaceClient {

  private final DedicatedEndpointHuggingFaceApi huggingFaceApi;
  private final String endpointUrl;

  DedicatedEndpointHuggingFaceClient(String apiKey, String endpointUrl, Duration timeout) {

    this.endpointUrl = ensureNotBlank(endpointUrl, "endpointUrl");
    OkHttpClient okHttpClient =
        new OkHttpClient.Builder()
            .addInterceptor(new ApiKeyInsertingInterceptor(apiKey))
            .callTimeout(timeout)
            .connectTimeout(timeout)
            .readTimeout(timeout)
            .writeTimeout(timeout)
            .build();

    Gson gson = new GsonBuilder().setFieldNamingPolicy(LOWER_CASE_WITH_UNDERSCORES).create();

    Retrofit retrofit =
        new Retrofit.Builder()
            .baseUrl(endpointUrl)
            .client(okHttpClient)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .build();

    this.huggingFaceApi = retrofit.create(DedicatedEndpointHuggingFaceApi.class);
  }

  @Override
  public TextGenerationResponse chat(TextGenerationRequest request) {
    return generate(request);
  }

  @Override
  public TextGenerationResponse generate(TextGenerationRequest request) {
    try {
      retrofit2.Response<List<TextGenerationResponse>> retrofitResponse =
          huggingFaceApi.generate(request).execute();

      if (retrofitResponse.isSuccessful()) {
        return toOneResponse(retrofitResponse);
      } else {
        throw toException(retrofitResponse);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static TextGenerationResponse toOneResponse(
      Response<List<TextGenerationResponse>> retrofitResponse) {
    List<TextGenerationResponse> responses = retrofitResponse.body();
    if (responses != null && responses.size() == 1) {
      return responses.get(0);
    } else {
      throw new RuntimeException(
          "Expected only one generated_text, but was: "
              + (responses == null ? 0 : responses.size()));
    }
  }

  @Override
  public List<float[]> embed(EmbeddingRequest request) {
    try {
      retrofit2.Response<List<float[]>> retrofitResponse = huggingFaceApi.embed(request).execute();
      if (retrofitResponse.isSuccessful()) {
        return retrofitResponse.body();
      } else {
        throw toException(retrofitResponse);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RuntimeException toException(retrofit2.Response<?> response) throws IOException {

    int code = response.code();
    String body = response.errorBody().string();

    String errorMessage = String.format("status code: %s; body: %s", code, body);
    return new RuntimeException(errorMessage);
  }
}

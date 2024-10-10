package dev.langchain4j.model.huggingface;

import dev.langchain4j.model.huggingface.client.EmbeddingRequest;
import dev.langchain4j.model.huggingface.client.TextGenerationRequest;
import dev.langchain4j.model.huggingface.client.TextGenerationResponse;
import java.util.List;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Headers;
import retrofit2.http.POST;

interface DedicatedEndpointHuggingFaceApi {

  @POST("/")
  @Headers({"Content-Type: application/json"})
  Call<List<TextGenerationResponse>> generate(@Body TextGenerationRequest request);

  @POST("/")
  @Headers({"Content-Type: application/json"})
  Call<List<float[]>> embed(@Body EmbeddingRequest request);
}

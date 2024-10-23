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

import static dev.langchain4j.spi.ServiceHelper.loadFactories;

import dev.langchain4j.model.huggingface.client.HuggingFaceClient;
import dev.langchain4j.model.huggingface.spi.HuggingFaceClientFactory;

class DedicatedEndpointFactoryCreator {

  private DedicatedEndpointFactoryCreator() {}

  static final HuggingFaceClientFactory FACTORY = factory();

  private static HuggingFaceClientFactory factory() {
    for (HuggingFaceClientFactory factory : loadFactories(HuggingFaceClientFactory.class)) {
      return factory;
    }
    return new DefaultHuggingFaceClientFactory();
  }

  static class DefaultHuggingFaceClientFactory implements HuggingFaceClientFactory {

    @Override
    public HuggingFaceClient create(Input input) {
      return new DedicatedEndpointHuggingFaceClient(
          input.apiKey(), input.modelId(), input.timeout());
    }
  }
}

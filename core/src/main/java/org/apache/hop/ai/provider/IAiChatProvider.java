/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.provider;

import java.time.Duration;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

/**
 * Runtime chat capability for providers that cannot be represented by a LangChain4j model.
 *
 * <p>Implementations are provider plugins and must keep model-library-specific types behind this
 * interface.
 */
public interface IAiChatProvider {

  /** Executes a chat request using the provider's native SDK or protocol. */
  AiChatResponse chat(AiChatRequest request, IVariables variables) throws HopException;

  /** Lists the chat model names available from this provider. */
  List<String> listModelNames(IVariables variables, Duration timeout) throws HopException;
}

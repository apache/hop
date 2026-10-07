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

package org.apache.hop.ai.engine;

import org.apache.hop.core.exception.HopException;

/**
 * A problem the user can fix from its message alone: no provider selected, a provider that cannot
 * be found or has no type, a missing API key, a question that does not fit the context window. The
 * AI Assistant shows the message where the question is asked, without an error dialog and stack
 * trace.
 */
public class AiUserException extends HopException {

  private static final long serialVersionUID = 1L;

  public AiUserException(String message) {
    super(message);
  }

  public AiUserException(String message, Throwable cause) {
    super(message, cause);
  }
}

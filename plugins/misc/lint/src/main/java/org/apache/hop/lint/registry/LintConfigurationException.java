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
package org.apache.hop.lint.registry;

/**
 * Thrown when the project's {@code hop-lint.yml} cannot be parsed.
 *
 * <p>A broken rule pack shipped by a third party is skipped with a logged error, because one bad
 * vendor jar should not stop the linter. The project's own configuration is different: the user
 * wrote it and needs to be told it is broken, rather than silently getting default rules.
 */
public class LintConfigurationException extends RuntimeException {

  public LintConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}

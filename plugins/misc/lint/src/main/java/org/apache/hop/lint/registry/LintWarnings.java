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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.logging.LogChannel;

/**
 * Logs a configuration warning prominently once, and quietly after that.
 *
 * <p>Rule packs and the project's hop-lint.yml are read again on every lint run, and the editor
 * lints on a timer, so logging each time repeated the same line for as long as the file kept the
 * mistake.
 */
final class LintWarnings {

  private static final Set<String> logged = ConcurrentHashMap.newKeySet();

  private LintWarnings() {}

  /**
   * @param key what makes this warning the same as one already logged
   * @param warning the line to log
   */
  static void logOnce(String key, String warning) {
    if (logged.add(key)) {
      LogChannel.GENERAL.logMinimal(warning);
    } else {
      LogChannel.GENERAL.logDetailed(warning);
    }
  }
}

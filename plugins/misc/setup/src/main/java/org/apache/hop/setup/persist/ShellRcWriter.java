/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.setup.persist;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.setup.HopSetupException;

/** Idempotent marked-block upsert for bash/zsh rc files. */
public final class ShellRcWriter {

  public static final String BEGIN = "# >>> hop setup >>>";
  public static final String END = "# <<< hop setup <<<";

  private ShellRcWriter() {}

  public static String renderBlock(Map<String, String> variables) throws HopSetupException {
    StringBuilder block = new StringBuilder();
    block.append(BEGIN).append('\n');
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      if (StringUtils.isEmpty(entry.getValue())) {
        continue;
      }
      block
          .append("export ")
          .append(entry.getKey())
          .append('=')
          .append(EnvValueEscaper.shellSingleQuoted(entry.getKey(), entry.getValue()))
          .append('\n');
    }
    block.append(END).append('\n');
    return block.toString();
  }

  public static String upsert(String existing, Map<String, String> variables)
      throws HopSetupException {
    String block = renderBlock(variables);
    String content = existing == null ? "" : existing;
    int start = content.indexOf(BEGIN);
    int end = content.indexOf(END);
    if (start >= 0 && end > start) {
      int endLine = content.indexOf('\n', end);
      int replaceTo = endLine >= 0 ? endLine + 1 : content.length();
      return content.substring(0, start) + block + content.substring(replaceTo);
    }
    if (content.isEmpty()) {
      return block;
    }
    StringBuilder next = new StringBuilder(content);
    if (!content.endsWith("\n")) {
      next.append('\n');
    }
    next.append('\n').append(block);
    return next.toString();
  }
}

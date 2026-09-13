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

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;

public final class AiCheckResultsSerializer {

  private AiCheckResultsSerializer() {}

  public static String serialize(List<ICheckResult> results) {
    StringBuilder json = new StringBuilder();
    json.append("{\"results\":[");
    List<ICheckResult> safe = results != null ? results : new ArrayList<>();
    for (int i = 0; i < safe.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      ICheckResult result = safe.get(i);
      String type =
          result.getType() == ICheckResult.TYPE_RESULT_ERROR
              ? "ERROR"
              : result.getType() == ICheckResult.TYPE_RESULT_OK
                  ? "OK"
                  : result.getType() == ICheckResult.TYPE_RESULT_WARNING ? "WARNING" : "INFO";
      json.append("{\"type\":").append(AiTextUtil.jsonString(type));
      json.append(",\"text\":")
          .append(AiTextUtil.jsonString(AiTextUtil.redactSecrets(result.getText())));
      json.append(",\"source\":").append(AiTextUtil.jsonString(formatCheckSource(result)));
      json.append('}');
    }
    json.append("]}");
    return json.toString();
  }

  private static String formatCheckSource(ICheckResult result) {
    if (result == null || result.getSourceInfo() == null) {
      return "";
    }
    Object source = result.getSourceInfo();
    if (source instanceof ICheckResultSource checkSource) {
      return checkSource.getName();
    }
    return String.valueOf(source);
  }
}

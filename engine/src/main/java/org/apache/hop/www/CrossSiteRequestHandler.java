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

package org.apache.hop.www;

import java.util.List;
import lombok.Getter;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.security.CrossSitePolicy;
import org.apache.hop.i18n.BaseMessages;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

/**
 * Rejects browser requests to the Hop server that a {@link CrossSitePolicy} does not allow, before
 * they reach authentication or any servlet.
 *
 * <p>Installed once, around the handler that carries every servlet context, the static resources
 * and the JSON API, so no endpoint can be added later that forgets to check. See {@link
 * CrossSitePolicy} for why {@code Sec-Fetch-Site} is the header this keys on.
 *
 * <p>A cross-site top-level navigation is rejected along with everything else: the usual carve-out
 * for one assumes a {@code GET} navigation is safe, which is exactly what does not hold here. An
 * operator following a link to the server from another site therefore lands on a 403; typing the
 * address or using a bookmark sends {@code Sec-Fetch-Site: none} and still works.
 */
public class CrossSiteRequestHandler extends Handler.Wrapper {

  private static final Class<?> PKG = CrossSiteRequestHandler.class;

  /** The request header the policy is keyed on. */
  public static final String SEC_FETCH_SITE = "Sec-Fetch-Site";

  /** Longest header value echoed into the log, to keep a hostile client from flooding it. */
  private static final int MAX_LOGGED_VALUE_LENGTH = 50;

  @Getter private final CrossSitePolicy policy;

  private final ILogChannel log;

  public CrossSiteRequestHandler(Handler handler, CrossSitePolicy policy, ILogChannel log) {
    super(handler);
    this.policy = policy;
    this.log = log;
  }

  @Override
  public boolean handle(Request request, Response response, Callback callback) throws Exception {
    List<String> values =
        request.getHeaders().getFields(SEC_FETCH_SITE).stream().map(HttpField::getValue).toList();

    if (policy.allows(values)) {
      return super.handle(request, response, callback);
    }

    if (log != null && log.isDetailed()) {
      log.logDetailed(
          BaseMessages.getString(
              PKG,
              "CrossSiteRequestHandler.Log.Rejected",
              request.getMethod(),
              request.getHttpURI().getPath(),
              forLog(values),
              policy.getCode()));
    }

    Response.writeError(
        request,
        response,
        callback,
        HttpStatus.FORBIDDEN_403,
        BaseMessages.getString(PKG, "CrossSiteRequestHandler.Error.Rejected"));
    return true;
  }

  /**
   * Render the rejected header values for a log line. Shared with {@link CrossSiteRequestFilter}.
   * The values come from an untrusted client, so they are truncated and stripped of anything that
   * could forge a log entry.
   */
  static String forLog(List<String> values) {
    if (values.isEmpty()) {
      return "<absent>";
    }
    StringBuilder text = new StringBuilder();
    for (String value : values) {
      if (!text.isEmpty()) {
        text.append(", ");
      }
      String trimmed =
          value.length() > MAX_LOGGED_VALUE_LENGTH
              ? value.substring(0, MAX_LOGGED_VALUE_LENGTH) + "..."
              : value;
      text.append(trimmed.replaceAll("[^A-Za-z0-9_.:/ -]", "?"));
    }
    return text.toString();
  }
}

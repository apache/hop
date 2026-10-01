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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.security.CrossSitePolicy;
import org.apache.hop.core.security.HopSecurityConfig;

/**
 * Rejects browser requests to the Hop Server API that a {@link CrossSitePolicy} does not allow.
 *
 * <p>This is the servlet-container counterpart of {@link CrossSiteRequestHandler}, which does the
 * same job inside the standalone hop-server. It exists because {@link HopServerServlet} is also
 * deployed on {@code /hop/*} inside Hop Web, where there is no {@link WebServer} to wrap — the same
 * state-changing {@code GET} endpoints, reached through a plain {@code web.xml} filter chain.
 *
 * <p>Map it ahead of the authentication filters so a cross-site request is refused without the
 * session or credentials being consulted at all. In Hop Web that matters more than in hop-server:
 * the authenticated modes are session-backed, and a browser attaches a session cookie to a
 * cross-site request by itself.
 *
 * <p>The policy comes from the {@code crossSitePolicy} init parameter when present, otherwise from
 * {@link HopSecurityConfig#resolveCrossSitePolicy()} — which in turn honours the {@code
 * HOP_SERVER_CROSS_SITE_POLICY} system property or environment variable.
 */
public class CrossSiteRequestFilter implements Filter {

  private static final Logger LOG = Logger.getLogger(CrossSiteRequestFilter.class.getName());

  /** Init parameter that pins the policy for this deployment, overriding the security config. */
  public static final String INIT_PARAM_POLICY = "crossSitePolicy";

  private static final String MESSAGE = "This Hop server rejects cross-site requests.";

  /** Set only when the deployment pinned a policy; otherwise the security config decides. */
  private CrossSitePolicy configuredPolicy;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String value = filterConfig == null ? null : filterConfig.getInitParameter(INIT_PARAM_POLICY);
    if (value != null && !value.isBlank()) {
      try {
        configuredPolicy = CrossSitePolicy.parse(value);
      } catch (HopException e) {
        // Refuse to start rather than run with a security control the operator believes is set.
        throw new ServletException(e.getMessage(), e);
      }
    }
    LOG.log(
        Level.INFO,
        "CrossSiteRequestFilter initialized (cross-site policy: {0})",
        configuredPolicy == null ? "from security config" : configuredPolicy.getCode());
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if (!(request instanceof HttpServletRequest httpRequest)
        || !(response instanceof HttpServletResponse httpResponse)) {
      chain.doFilter(request, response);
      return;
    }

    CrossSitePolicy policy = policy();
    List<String> values = secFetchSiteValues(httpRequest);

    if (policy.allows(values)) {
      chain.doFilter(request, response);
      return;
    }

    LOG.log(
        Level.FINE,
        "Rejected cross-site request {0} {1} (Sec-Fetch-Site: {2}), cross-site policy is [{3}]",
        new Object[] {
          httpRequest.getMethod(),
          httpRequest.getRequestURI(),
          CrossSiteRequestHandler.forLog(values),
          policy.getCode()
        });

    if (httpResponse.isCommitted()) {
      return;
    }
    httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
    httpResponse.setCharacterEncoding(StandardCharsets.UTF_8.name());
    httpResponse.setContentType("text/plain; charset=UTF-8");
    httpResponse.setHeader("Cache-Control", "no-store");
    httpResponse.getWriter().write(MESSAGE);
  }

  private CrossSitePolicy policy() {
    if (configuredPolicy != null) {
      return configuredPolicy;
    }
    return HopSecurityConfig.load().resolveCrossSitePolicy();
  }

  /** Every Sec-Fetch-Site value on the request, in order, so duplicates can be spotted. */
  private static List<String> secFetchSiteValues(HttpServletRequest request) {
    var headers = request.getHeaders(CrossSiteRequestHandler.SEC_FETCH_SITE);
    if (headers == null) {
      return List.of();
    }
    return Collections.list(headers);
  }
}

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

package org.apache.hop.www;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.apache.hop.core.security.CrossSitePolicy;
import org.junit.jupiter.api.Test;

/**
 * The servlet-container half of the cross-site check, used where {@link HopServerServlet} is
 * deployed on {@code /hop/*} outside the standalone hop-server - notably inside Hop Web.
 */
class CrossSiteRequestFilterTest {

  private static CrossSiteRequestFilter filterWithPolicy(String policy) throws ServletException {
    FilterConfig config = mock(FilterConfig.class);
    when(config.getInitParameter(CrossSiteRequestFilter.INIT_PARAM_POLICY)).thenReturn(policy);
    CrossSiteRequestFilter filter = new CrossSiteRequestFilter();
    filter.init(config);
    return filter;
  }

  private static HttpServletRequest requestWith(String... secFetchSiteValues) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/hop/execPipeline");
    Enumeration<String> values = Collections.enumeration(List.of(secFetchSiteValues));
    when(request.getHeaders(CrossSiteRequestHandler.SEC_FETCH_SITE)).thenReturn(values);
    return request;
  }

  @Test
  void crossSiteRequestIsRejectedAndNeverReachesTheChain() throws Exception {
    CrossSiteRequestFilter filter = filterWithPolicy(CrossSitePolicy.SAME_SITE.getCode());
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body, true));
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(requestWith("cross-site"), response, chain);

    // Never reaching the chain is the point: the auth filters and the servlet are both downstream.
    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    assertTrue(body.toString().contains("cross-site"), body.toString());
  }

  @Test
  void allowedRequestsReachTheChain() throws Exception {
    CrossSiteRequestFilter filter = filterWithPolicy(CrossSitePolicy.SAME_SITE.getCode());
    FilterChain chain = mock(FilterChain.class);

    // The server's own pages, a bookmark, a sibling host, and a non-browser client.
    filter.doFilter(requestWith("same-origin"), mock(HttpServletResponse.class), chain);
    filter.doFilter(requestWith("none"), mock(HttpServletResponse.class), chain);
    filter.doFilter(requestWith("same-site"), mock(HttpServletResponse.class), chain);
    filter.doFilter(requestWith(), mock(HttpServletResponse.class), chain);

    verify(chain, times(4)).doFilter(any(), any());
  }

  @Test
  void sameOriginPolicyAlsoRejectsSameSite() throws Exception {
    CrossSiteRequestFilter filter = filterWithPolicy(CrossSitePolicy.SAME_ORIGIN.getCode());
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter(), true));
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(requestWith("same-site"), response, chain);

    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void offPolicyLetsEverythingThrough() throws Exception {
    CrossSiteRequestFilter filter = filterWithPolicy(CrossSitePolicy.OFF.getCode());
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(requestWith("cross-site"), mock(HttpServletResponse.class), chain);

    verify(chain).doFilter(any(), any());
  }

  /** A missing header enumeration is what some containers hand back; it must not blow up. */
  @Test
  void absentHeaderEnumerationIsTreatedAsNoHeader() throws Exception {
    CrossSiteRequestFilter filter = filterWithPolicy(CrossSitePolicy.SAME_ORIGIN.getCode());
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeaders(CrossSiteRequestHandler.SEC_FETCH_SITE)).thenReturn(null);
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(request, mock(HttpServletResponse.class), chain);

    verify(chain).doFilter(any(), any());
  }

  /** Starting with a policy the operator mistyped would silently leave the check wide open. */
  @Test
  void unusableInitParameterFailsDeployment() {
    ServletException e =
        assertThrows(ServletException.class, () -> filterWithPolicy("strict-please"));
    assertTrue(e.getMessage().contains("strict-please"), e.getMessage());
  }

  /** No init parameter means the Hop Web security config decides, which defaults to same-site. */
  @Test
  void withoutAnInitParameterTheSecurityConfigDecides() throws Exception {
    CrossSiteRequestFilter filter = filterWithPolicy(null);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter(), true));
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(requestWith("cross-site"), response, chain);

    verify(chain, never()).doFilter(any(), any());
    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
  }

  @Test
  void policyCodesAreTheOnesTheGuiOffers() {
    assertEquals("same-site", CrossSitePolicy.SAME_SITE.getCode());
    assertEquals("same-origin", CrossSitePolicy.SAME_ORIGIN.getCode());
    assertEquals("off", CrossSitePolicy.OFF.getCode());
  }
}

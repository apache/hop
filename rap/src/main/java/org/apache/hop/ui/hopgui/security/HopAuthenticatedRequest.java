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

package org.apache.hop.ui.hopgui.security;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.security.Principal;

/** Request wrapper that exposes a Hop-managed principal and roles. */
public class HopAuthenticatedRequest extends HttpServletRequestWrapper {

  private final HopAuthenticatedPrincipal principal;

  public HopAuthenticatedRequest(HttpServletRequest request, HopAuthenticatedPrincipal principal) {
    super(request);
    this.principal = principal;
  }

  @Override
  public Principal getUserPrincipal() {
    return principal;
  }

  @Override
  public String getRemoteUser() {
    return principal != null ? principal.getName() : null;
  }

  @Override
  public boolean isUserInRole(String role) {
    return principal != null && principal.isInRole(role);
  }

  @Override
  public String getAuthType() {
    return HttpServletRequest.BASIC_AUTH;
  }
}

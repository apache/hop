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

import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Servlet principal for a Hop-managed BASIC-authenticated user. */
public final class HopAuthenticatedPrincipal implements Principal {

  private final String name;
  private final Set<String> roles;

  public HopAuthenticatedPrincipal(String name, Set<String> roles) {
    this.name = name;
    this.roles = roles == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(roles));
  }

  @Override
  public String getName() {
    return name;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public boolean isInRole(String role) {
    if (role == null || role.isBlank()) {
      return false;
    }
    if (roles.contains(role)) {
      return true;
    }
    for (String r : roles) {
      if (r != null && r.equalsIgnoreCase(role)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "HopAuthenticatedPrincipal{name='" + name + "', roles=" + roles + '}';
  }
}

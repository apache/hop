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

package org.apache.hop.core.security;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/** A Hop-managed web user for BASIC authentication. */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class HopUser {

  private String username;

  /** PBKDF2 hash from {@link PasswordHasher}; never store clear text. */
  private String passwordHash;

  /** Hop role ids: {@code admin}, {@code user}, {@code operator}, {@code readonly}. */
  private List<String> roles = new ArrayList<>();

  private boolean enabled = true;

  public HopUser() {}

  public HopUser(String username, String passwordHash, List<String> roles) {
    this.username = username;
    this.passwordHash = passwordHash;
    if (roles != null) {
      this.roles = new ArrayList<>(roles);
    }
  }
}

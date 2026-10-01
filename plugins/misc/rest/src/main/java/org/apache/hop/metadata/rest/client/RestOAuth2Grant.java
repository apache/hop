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

package org.apache.hop.metadata.rest.client;

/**
 * The OAuth 2 grants a REST connection can use (issue #6595). Both are usable without a browser,
 * which is what a pipeline needs.
 *
 * <p>Authorization Code is deliberately absent: it requires an interactive redirect, so headless it
 * could only ever consume a refresh token obtained elsewhere — which is what {@link #REFRESH_TOKEN}
 * already does.
 */
public enum RestOAuth2Grant {
  /** Machine-to-machine: the client id and secret are the credentials. */
  CLIENT_CREDENTIALS("client_credentials"),

  /** Exchanges a long-lived refresh token, obtained once interactively, for access tokens. */
  REFRESH_TOKEN("refresh_token");

  private final String wireName;

  RestOAuth2Grant(String wireName) {
    this.wireName = wireName;
  }

  /** The value sent as {@code grant_type}. */
  public String getWireName() {
    return wireName;
  }
}

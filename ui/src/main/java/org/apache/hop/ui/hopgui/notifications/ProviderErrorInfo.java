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

package org.apache.hop.ui.hopgui.notifications;

import java.util.Date;

/** Represents a provider fetch/initialization error for user-facing display. */
public class ProviderErrorInfo {
  private final String providerId;
  private final String providerName;
  private final String message;
  private final Date timestamp;

  public ProviderErrorInfo(String providerId, String providerName, String message, Date timestamp) {
    this.providerId = providerId;
    this.providerName = providerName;
    this.message = message != null ? message : "";
    this.timestamp = timestamp != null ? timestamp : new Date();
  }

  public String getProviderId() {
    return providerId;
  }

  public String getProviderName() {
    return providerName;
  }

  public String getMessage() {
    return message;
  }

  public Date getTimestamp() {
    return timestamp;
  }
}

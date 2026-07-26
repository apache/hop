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

package org.apache.hop.marketplace.env;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.IAttributes;

/**
 * Attribute group/key constants for marketplace settings stored via {@link IAttributes} (e.g. on
 * lifecycle environments). Group name is {@link #GROUP}; keys are short and stable for JSON.
 */
public final class MarketplaceAttributes {

  /** IAttributes group for marketplace settings. */
  public static final String GROUP = "marketplace";

  /** Path to hop-env.yaml / full-client-env.yaml (may contain variables). */
  public static final String KEY_ENV_FILE = "envFile";

  /**
   * When environment is enabled: {@link #ON_ENABLE_OFF}, {@link #ON_ENABLE_WARN}, {@link
   * #ON_ENABLE_ENFORCE}.
   */
  public static final String KEY_ON_ENABLE = "onEnable";

  /** When true, extra marketplace plugins (receipts not in env file) count as drift. */
  public static final String KEY_STRICT = "strict";

  /** When true, apply missing installs on enable (default false). */
  public static final String KEY_AUTO_APPLY = "autoApply";

  public static final String ON_ENABLE_OFF = "off";
  public static final String ON_ENABLE_WARN = "warn";
  public static final String ON_ENABLE_ENFORCE = "enforce";

  private MarketplaceAttributes() {}

  /**
   * Resolve on-enable policy from attributes, falling back to purpose-based defaults when unset.
   *
   * <ul>
   *   <li>Production → enforce
   *   <li>Testing / Acceptance → warn
   *   <li>Development / CI / CB / other → off
   * </ul>
   */
  public static String resolveOnEnable(IAttributes attributes, String purpose) {
    String explicit = attributes != null ? attributes.getAttribute(GROUP, KEY_ON_ENABLE) : null;
    if (StringUtils.isNotBlank(explicit)) {
      return explicit.trim().toLowerCase();
    }
    return defaultOnEnableForPurpose(purpose);
  }

  public static String defaultOnEnableForPurpose(String purpose) {
    if (StringUtils.isBlank(purpose)) {
      return ON_ENABLE_OFF;
    }
    String p = purpose.trim().toLowerCase();
    if (p.contains("production") || p.contains("prod")) {
      return ON_ENABLE_ENFORCE;
    }
    if (p.contains("test") || p.contains("accept")) {
      return ON_ENABLE_WARN;
    }
    return ON_ENABLE_OFF;
  }

  public static boolean isStrict(IAttributes attributes) {
    if (attributes == null) {
      return false;
    }
    String v = attributes.getAttribute(GROUP, KEY_STRICT);
    return "true".equalsIgnoreCase(v) || "Y".equalsIgnoreCase(v);
  }

  public static boolean isAutoApply(IAttributes attributes) {
    if (attributes == null) {
      return false;
    }
    String v = attributes.getAttribute(GROUP, KEY_AUTO_APPLY);
    return "true".equalsIgnoreCase(v) || "Y".equalsIgnoreCase(v);
  }

  public static String envFile(IAttributes attributes) {
    return attributes != null ? attributes.getAttribute(GROUP, KEY_ENV_FILE) : null;
  }
}

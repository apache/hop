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

package org.apache.hop.core.security;

import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;

/**
 * Which browser requests the Hop server accepts, expressed in terms of the <a
 * href="https://developer.mozilla.org/docs/Web/HTTP/Headers/Sec-Fetch-Site">{@code
 * Sec-Fetch-Site}</a> request header.
 *
 * <p>The Hop server servlets answer state-changing operations on {@code GET}, so a page the
 * operator happens to be visiting can drive them simply by pointing an image, a form or a link at
 * the server. {@code Origin} is no help against that shape of request — browsers omit it on
 * cross-site {@code GET} — but {@code Sec-Fetch-Site} is always sent, and non-browser clients
 * (hop-run, the Hop GUI, curl, customer automation) send no {@code Sec-Fetch-*} headers at all.
 * Treating an absent header as "allow" therefore leaves existing integrations untouched while
 * closing the browser-driven path.
 *
 * <p>Unrecognised header values are rejected rather than allowed: a client that cannot send the
 * header correctly can always send nothing at all.
 */
public enum CrossSitePolicy {
  /**
   * Reject {@code cross-site} requests only. A request from another host under the same registrable
   * domain — a second Hop server, an intranet portal, the same host on another port — is still
   * accepted. This is the default.
   */
  SAME_SITE("same-site"),

  /**
   * Reject {@code same-site} requests as well, so only the server's own pages may drive it. Use
   * this when nothing on a neighbouring host of the same domain is trusted to link to the server.
   */
  SAME_ORIGIN("same-origin"),

  /**
   * Do not inspect {@code Sec-Fetch-Site} at all. An escape hatch for a deployment that turns out
   * to need cross-site browser access; it leaves the server open to the requests described above.
   */
  OFF("off");

  /**
   * System property and environment variable that selects the policy, shared by hop-server and the
   * Hop Server API embedded in Hop Web so operators have one knob to learn.
   */
  public static final String CONFIG_KEY = Const.HOP_SERVER_CROSS_SITE_POLICY;

  /** Value of the {@code Sec-Fetch-Site} header for a request with no initiating site. */
  private static final String SITE_NONE = "none";

  /** Value of the {@code Sec-Fetch-Site} header for a request from the server's own origin. */
  private static final String SITE_SAME_ORIGIN = "same-origin";

  /** Value of the {@code Sec-Fetch-Site} header for a request from the same registrable domain. */
  private static final String SITE_SAME_SITE = "same-site";

  /** How the policy is written on the command line, in the environment and in the log. */
  @Getter private final String code;

  CrossSitePolicy(String code) {
    this.code = code;
  }

  /**
   * Decide whether a request carrying these {@code Sec-Fetch-Site} values is accepted.
   *
   * @param secFetchSiteValues every {@code Sec-Fetch-Site} value on the request, in order. Empty or
   *     null for a client that did not send the header.
   * @return true if the request may proceed
   */
  public boolean allows(List<String> secFetchSiteValues) {
    if (this == OFF) {
      return true;
    }
    if (secFetchSiteValues == null) {
      return true;
    }
    List<String> values =
        secFetchSiteValues.stream().filter(value -> value != null && !value.isBlank()).toList();
    if (values.isEmpty()) {
      // A non-browser client: hop-run, the Hop GUI, curl, automation.
      return true;
    }
    if (values.size() > 1) {
      // Contradictory headers, most likely added along the way. There is no safe reading.
      return false;
    }
    String value = values.get(0).trim().toLowerCase(Locale.ROOT);
    return switch (value) {
      case SITE_NONE, SITE_SAME_ORIGIN -> true;
      case SITE_SAME_SITE -> this == SAME_SITE;
        // "cross-site", and anything this version does not recognise.
      default -> false;
    };
  }

  /**
   * Read a policy from its {@code code}, as written on the command line or in the environment.
   *
   * @param value the code to read, case-insensitive. Null or blank selects {@link #SAME_SITE}.
   * @return the matching policy
   * @throws HopException if the value is not a policy code
   */
  public static CrossSitePolicy parse(String value) throws HopException {
    if (value == null || value.isBlank()) {
      return SAME_SITE;
    }
    String wanted = value.trim().toLowerCase(Locale.ROOT);
    for (CrossSitePolicy policy : values()) {
      if (policy.code.equals(wanted)) {
        return policy;
      }
    }
    throw new HopException(
        "Unknown cross-site policy '"
            + value
            + "'. Valid values are: "
            + String.join(", ", SAME_SITE.code, SAME_ORIGIN.code, OFF.code)
            + ".");
  }
}

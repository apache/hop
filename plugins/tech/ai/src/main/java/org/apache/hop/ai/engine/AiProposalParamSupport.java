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

package org.apache.hop.ai.engine;

import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.util.Utils;

/** Shared parsing helpers for hop_proposals parameters. */
public final class AiProposalParamSupport {

  private AiProposalParamSupport() {}

  public static Location parseLocation(AiProposal proposal) {
    return parseLocation(proposal.parameter("locationX"), proposal.parameter("locationY"));
  }

  public static Location parseLocation(String xValue, String yValue) {
    if (Utils.isEmpty(xValue) || Utils.isEmpty(yValue)) {
      return Location.invalid();
    }
    try {
      return Location.of(Integer.parseInt(xValue.trim()), Integer.parseInt(yValue.trim()));
    } catch (NumberFormatException e) {
      return Location.invalid();
    }
  }

  public static int parseOptionalSize(String value, int defaultValue) {
    if (Utils.isEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  public static boolean parseEnabled(String value) {
    if (Utils.isEmpty(value)) {
      return true;
    }
    return !"N".equalsIgnoreCase(value.trim());
  }

  public static boolean parseUnconditional(String value) {
    if (Utils.isEmpty(value)) {
      return false;
    }
    return "Y".equalsIgnoreCase(value.trim());
  }

  public static boolean parseEvaluation(String value) {
    if (Utils.isEmpty(value)) {
      return true;
    }
    return !"N".equalsIgnoreCase(value.trim());
  }

  public static boolean isYesNo(String value) {
    if (Utils.isEmpty(value)) {
      return false;
    }
    String normalized = value.trim().toUpperCase();
    return "Y".equals(normalized) || "N".equals(normalized);
  }

  public record Location(int x, int y) {
    public static Location of(int x, int y) {
      return new Location(x, y);
    }

    public static Location invalid() {
      return new Location(Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    public boolean isValid() {
      return x != Integer.MIN_VALUE;
    }
  }
}

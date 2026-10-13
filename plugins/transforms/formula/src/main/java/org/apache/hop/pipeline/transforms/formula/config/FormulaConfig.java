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

package org.apache.hop.pipeline.transforms.formula.config;

import org.apache.hop.core.config.HopConfig;

/**
 * The options of the Formula transform in {@code hop-config.json}, edited in the configuration
 * perspective and with {@code hop conf} through {@link FormulaConfigPlugin}.
 *
 * <p>Kept apart from that plugin on purpose: the plugin is a Hop GUI listener, and the transform
 * reads these options wherever it runs, Hop GUI or not.
 */
public final class FormulaConfig {

  /** Whether the Formula transform may use the fast evaluator at all; true when not set. */
  public static final String OPTION_FAST_PATH_ENABLED = "FormulaFastPathEnabled";

  private FormulaConfig() {}

  /**
   * @return false when the fast evaluator was switched off in {@code hop-config.json}
   */
  public static boolean isFastPathEnabled() {
    try {
      return HopConfig.readOptionBoolean(OPTION_FAST_PATH_ENABLED, true);
    } catch (Exception e) {
      // No readable hop-config.json, on a remote worker say: the default stands.
      return true;
    }
  }

  /**
   * Switch the fast evaluator on or off for every pipeline, and save that in {@code
   * hop-config.json}.
   *
   * @param enabled false to always evaluate with Apache POI
   */
  public static void setFastPathEnabled(boolean enabled) {
    HopConfig.getInstance().saveOption(OPTION_FAST_PATH_ENABLED, enabled);
  }
}

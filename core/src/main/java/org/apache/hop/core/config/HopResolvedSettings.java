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

package org.apache.hop.core.config;

import java.util.Map;
import org.apache.hop.core.Const;

/**
 * Resolves Hop settings from layered configuration so callers can treat {@code hop-config.json},
 * the process environment, and the JVM consistently.
 *
 * <p><b>Precedence</b> (each layer replaces the previous when that layer defines the name; the last
 * applicable layer wins):
 *
 * <ol>
 *   <li>{@code hop-config.json} described variables &mdash; defaults shipped with the app or image
 *   <li>{@linkplain System#getenv(String) Environment variables} &mdash; overrides from Docker,
 *       Kubernetes, CI/CD, or the shell ({@code containsKey} on the environment map)
 *   <li>Java {@linkplain System#getProperty(String) system properties} &mdash; explicit runtime
 *       overrides such as {@code -Dname=value} ({@code containsKey} on {@link
 *       System#getProperties()})
 * </ol>
 *
 * <p>This ordering matches a common operations pattern: bake stable defaults into the image or
 * config file, let the platform inject environment at deploy time, and reserve {@code -D} for
 * ad-hoc or debugging overrides that should win over everything else.
 *
 * <p><b>Note:</b> After full Hop environment initialization, described variables from {@code
 * hop-config.json} are copied into {@link System#setProperty(String, String)}. For names that
 * appear in the config file, {@link System#getProperties()} will then usually repeat the file
 * value, so the JVM tier can override environment variables until that copy is aligned with this
 * resolver. Call sites that run <em>before</em> that copy (for example {@link
 * org.apache.hop.core.HopClientEnvironment}) see the intended hop → env → {@code -D} behavior.
 */
public final class HopResolvedSettings {

  private HopResolvedSettings() {}

  /**
   * @param name setting / variable name (for example {@link Const#HOP_DISABLE_CONSOLE_LOGGING})
   * @param defaultValue used when no layer supplies a value
   * @return the effective string value
   */
  public static String resolveString(String name, String defaultValue) {
    String value = HopConfig.readStringVariable(name, null);
    Map<String, String> env = System.getenv();
    if (env.containsKey(name)) {
      value = env.get(name);
    }
    if (System.getProperties().containsKey(name)) {
      value = System.getProperty(name);
    }
    return Const.NVL(value, defaultValue);
  }
}

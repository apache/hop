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
 * <p>A system property that only repeats the hop-config value is the copy {@code HopEnvironment}
 * makes at startup, not an explicit {@code -D}. It does not hide an environment variable. A
 * property that differs from the file is an explicit override and wins over the environment.
 */
public final class HopResolvedSettings {

  private HopResolvedSettings() {}

  /**
   * @param name setting / variable name (for example {@link Const#HOP_DISABLE_CONSOLE_LOGGING})
   * @param defaultValue used when no layer supplies a value
   * @return the effective string value
   */
  public static String resolveString(String name, String defaultValue) {
    String resolved =
        resolveLayered(
            HopConfig.readStringVariable(name, null),
            System.getenv(name),
            System.getProperty(name));
    return Const.NVL(resolved, defaultValue);
  }

  /**
   * Resolve one setting from the three layers, without reading the live JVM.
   *
   * <p>A system property that equals the hop-config value (or is empty when the file has no value)
   * is treated as the startup copy of that file. An environment variable then wins over it. A
   * system property with any other value is an explicit {@code -D} and wins over the environment.
   *
   * @param configValue value from {@code hop-config.json}, or null when the name is not configured
   * @param environmentValue exported environment variable, or null when it is not set
   * @param systemPropertyValue system property, or null when it is not set
   * @return the winning value, which may be null when every layer is absent
   */
  public static String resolveLayered(
      String configValue, String environmentValue, String systemPropertyValue) {
    String value = configValue;
    if (systemPropertyValue != null && !systemPropertyValue.equals(Const.NVL(configValue, ""))) {
      value = systemPropertyValue;
    } else if (environmentValue != null) {
      value = environmentValue;
    }
    return value;
  }
}

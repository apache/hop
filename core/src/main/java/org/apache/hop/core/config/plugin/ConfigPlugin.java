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

package org.apache.hop.core.config.plugin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** This annotation signals to the plugin system that the class is a configuration plugin. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ConfigPlugin {
  String CATEGORY_ROOT = "root";
  String CATEGORY_CONFIG = "config";
  String CATEGORY_RUN = "run";
  String CATEGORY_SEARCH = "search";
  String CATEGORY_IMPORT = "import";
  String CATEGORY_SERVER = "server";
  String CATEGORY_DOC = "doc";
  String CATEGORY_PYTHON = "python";
  String CATEGORY_NAMING = "naming";
  String CATEGORY_GUI = "gui";
  String CATEGORY_EXPORT = "export";

  String id();

  String description() default "";

  String category() default CATEGORY_CONFIG;

  /**
   * Plugins sharing a group share a single class loader. Set this when the config plugin lives in a
   * plugin folder that also uses {@code classLoaderGroup} on metadata or GUI types.
   */
  String classLoaderGroup() default "";

  /**
   * The key this plugin's options are stored under in {@code hop-config.json}, for example {@code
   * googleCloud}. Empty when the plugin writes its options as individual top-level options rather
   * than as one block. Documentation generators use this to name the JSON block they describe.
   *
   * @return The hop-config.json key, or an empty String
   */
  String configKey() default "";

  /**
   * The plain configuration object holding this plugin's settings and their default values, for
   * example {@code GoogleCloudConfig.class}. Its no-argument constructor must set the defaults and
   * must not need a running Hop: documentation generators instantiate it at build time and read the
   * fields to report what each option defaults to.
   *
   * <p>A field is matched to this class by name, so a widget field and the setting it edits have to
   * be called the same thing. Where there is no matching field - the plugin reads the option
   * straight from {@link org.apache.hop.core.config.HopConfig}, say - the default belongs on {@code
   * GuiWidgetElement#defaultValue()} instead.
   *
   * @return The configuration class, or {@link Void} when the plugin has none
   */
  Class<?> configClass() default Void.class;
}

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

package org.apache.hop.core.gui.plugin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** This annotation signals to the plugin system that the class is a GUI plugin. */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GuiPlugin {
  String id() default ""; // defaults to class name

  String name() default "";

  String description() default "";

  /**
   * The class loader group this GUI plugin belongs to. Plugins sharing a group share a single class
   * loader. Set this on a {@code @GuiPlugin} that is also a {@code @HopMetadata} (or otherwise
   * lives in a grouped plugin folder) so editor widgets and Test buttons see the same class as the
   * metadata serializer. Without it {@code GuiPluginType} loads a second copy and a button method
   * that casts the editor object throws {@link ClassCastException}.
   *
   * @return the class loader group, empty for the default one-class-loader-per-plugin-folder
   */
  String classLoaderGroup() default "";
}

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
package org.apache.hop.core.notifications;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an {@link INotificationProvider} implementation so the plugin registry finds it.
 *
 * <p>A plugin that has something to tell the user - a newer version of itself, a job that finished,
 * an expiring licence - annotates a provider and is done. The provider is discovered from the jar
 * like every other Hop plugin, so it needs no registration call, appears in the Notifications
 * settings on its own, and disappears again when the plugin is uninstalled.
 *
 * <pre>
 * {@literal @}NotificationProviderPlugin(
 *     id = "my-plugin-notifications",
 *     name = "My Plugin",
 *     description = "Tells you when a new version of My Plugin is published")
 * public class MyPluginNotificationProvider implements INotificationProvider {
 *   ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface NotificationProviderPlugin {

  /**
   * @return The identifier of this provider, unique across Hop. It is also the identifier of the
   *     source that the Notifications settings show, and the prefix of the notification ids.
   */
  String id();

  /**
   * @return The name shown in the notification panel and settings
   */
  String name() default "";

  /**
   * @return What this provider reports on
   */
  String description() default "";
}

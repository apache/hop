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

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** The plugin type of the notification providers that plugins contribute. */
@PluginMainClassType(INotificationProvider.class)
@PluginAnnotationType(NotificationProviderPlugin.class)
public class NotificationProviderPluginType extends BasePluginType<NotificationProviderPlugin> {

  private static NotificationProviderPluginType pluginType;

  private NotificationProviderPluginType() {
    super(NotificationProviderPlugin.class, "NOTIFICATION_PROVIDERS", "Notification providers");
  }

  public static NotificationProviderPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new NotificationProviderPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractID(NotificationProviderPlugin annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(NotificationProviderPlugin annotation) {
    return annotation.name();
  }

  @Override
  protected String extractDesc(NotificationProviderPlugin annotation) {
    return annotation.description();
  }
}

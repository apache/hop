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

package org.apache.hop.core.auth;

import org.apache.hop.core.auth.core.AuthenticationFactoryException;
import org.apache.hop.core.auth.core.AuthenticationManager;
import org.apache.hop.core.auth.core.IAuthenticationConsumer;
import org.apache.hop.core.auth.core.IAuthenticationProvider;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;

public class AuthenticationPersistenceManager {
  private static final Class<?> PKG = AuthenticationPersistenceManager.class; // For Translator
  private static final ILogChannel log =
      new LogChannel(AuthenticationPersistenceManager.class.getName());

  public static AuthenticationManager getAuthenticationManager() {
    AuthenticationManager manager = new AuthenticationManager();
    manager.registerAuthenticationProvider(new NoAuthenticationAuthenticationProvider());

    // TODO: Register providers from metadata

    for (IPlugin plugin :
        PluginRegistry.getInstance().getPlugins(AuthenticationConsumerPluginType.class)) {
      try {
        Object pluginMain = PluginRegistry.getInstance().loadClass(plugin);
        if (pluginMain instanceof IAuthenticationConsumerType) {
          Class<? extends IAuthenticationConsumer<?, ?>> consumerClass =
              ((IAuthenticationConsumerType) pluginMain).getConsumerClass();
          manager.registerConsumerClass(consumerClass);
        } else {
          throw new HopPluginException(
              BaseMessages.getString(
                  PKG,
                  "AuthenticationPersistenceManager.NotConsumerType",
                  pluginMain,
                  IAuthenticationConsumerType.class));
        }
      } catch (HopPluginException e) {
        log.logError(e.getMessage(), e);
      } catch (AuthenticationFactoryException e) {
        log.logError(e.getMessage(), e);
      }
    }
    return manager;
  }

  public static void persistAuthenticationProvider(IAuthenticationProvider authenticationProvider) {
    // TODO: Persist to metadata
  }
}

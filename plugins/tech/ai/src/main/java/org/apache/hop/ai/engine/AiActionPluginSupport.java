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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/** Loads workflow action plugin metadata with plugin defaults for AI proposal application. */
public final class AiActionPluginSupport {

  private AiActionPluginSupport() {}

  public static ActionMeta newActionMeta(String pluginId, String name) throws HopException {
    try {
      IAction action =
          PluginRegistry.getInstance().loadClass(ActionPluginType.class, pluginId, IAction.class);
      ActionMeta actionMeta = new ActionMeta(action);
      actionMeta.setName(name);
      return actionMeta;
    } catch (Exception e) {
      throw new HopException("Unable to load workflow action plugin: " + pluginId, e);
    }
  }
}

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

package org.apache.hop.hop.plugin;

import org.apache.hop.core.plugins.BasePluginType;
import org.apache.hop.core.plugins.PluginAnnotationType;
import org.apache.hop.core.plugins.PluginMainClassType;

/** This class represents the transform plugin type. */
@PluginMainClassType(IHopCommand.class)
@PluginAnnotationType(HopCommand.class)
public class HopCommandPluginType extends BasePluginType<HopCommand> {
  private static HopCommandPluginType pluginType;

  private HopCommandPluginType() {
    super(HopCommand.class, "HOP_SUB_COMMAND", "Hop Sub Command");
  }

  public static HopCommandPluginType getInstance() {
    if (pluginType == null) {
      pluginType = new HopCommandPluginType();
    }
    return pluginType;
  }

  @Override
  protected String extractDesc(HopCommand annotation) {
    return annotation.description();
  }

  @Override
  protected String extractID(HopCommand annotation) {
    return annotation.id();
  }

  @Override
  protected String extractName(HopCommand annotation) {
    return null;
  }
}

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

package org.apache.hop.core.exception;

import org.apache.hop.core.Const;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * This Exception is throws when an error occurs loading plugins.
 *
 * @author Matt
 * @since 9-12-2004
 */
public class HopMissingPluginsException extends HopException {
  private static final long serialVersionUID = -3008319146447259788L;

  public static class PluginDetails {
    private final Class<? extends IPluginType> pluginTypeClass;
    private final String pluginId;

    public PluginDetails( Class<? extends IPluginType> pluginTypeClass, String pluginId ) {
      super();
      this.pluginTypeClass = pluginTypeClass;
      this.pluginId = pluginId;
    }

	public Class<? extends IPluginType> getPluginTypeClass() {
		return pluginTypeClass;
	}

	public String getPluginId() {
		return pluginId;
	}
  }

  private final List<PluginDetails> missingPluginDetailsList;

  /**
   * Constructs a new throwable with the specified detail message.
   *
   * @param message - the detail message. The detail message is saved for later retrieval by the getMessage() method.
   */
  public HopMissingPluginsException( String message ) {
    super( message );
    this.missingPluginDetailsList = new ArrayList<>();
  }

  /**
   * Add a missing plugin id for a given plugin type.
   *
   * @param pluginTypeClass The class of the plugin type (ex. TransformPluginType.class)
   * @param pluginId        The id of the missing plugin
   */
  public void addMissingPluginDetails( Class<? extends IPluginType> pluginTypeClass, String pluginId ) {
    missingPluginDetailsList.add( new PluginDetails( pluginTypeClass, pluginId ) );
  }

  public List<PluginDetails> getMissingPluginDetailsList() {
    return missingPluginDetailsList;
  }

  @Override
  public String getMessage() {
    StringBuilder message = new StringBuilder( super.getMessage() );
    message.append( getPluginsMessage() );
    return message.toString();
  }

  public String getPluginsMessage() {
    StringBuilder message = new StringBuilder();
    for ( PluginDetails details : missingPluginDetailsList ) {
      message.append( Const.CR );
      try {
        IPluginType pluginType = PluginRegistry.getInstance().getPluginType( details.pluginTypeClass );
        message.append( pluginType.getName() );
      } catch ( Exception e ) {
        message.append( "UnknownPluginType-" ).append( details.getPluginTypeClass().getName() );
      }
      message.append( " : " ).append( details.getPluginId() );
    }
    return message.toString();
  }

}

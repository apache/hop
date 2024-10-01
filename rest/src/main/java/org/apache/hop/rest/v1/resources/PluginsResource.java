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
 *
 */

package org.apache.hop.rest.v1.resources;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.rest.Hop;

@Path("/plugins")
public class PluginsResource extends BaseResource {
  private final Hop hop = Hop.getInstance();

  /**
   * List all the plugin type classes
   *
   * @return A list with all the plugin types from the plugin registry
   */
  @GET
  @Path("/types")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTypes() {
    PluginRegistry registry = PluginRegistry.getInstance();
    List<Class<? extends IPluginType>> types = registry.getPluginTypes();
    return Response.ok(types).build();
  }

  /**
   * List all the plugins for a given type class
   *
   * @param typeClassName the metadata key to use
   * @return A list with all the available plugins
   */
  @GET
  @Path("/list/{typeClassName}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listPlugins(@PathParam("typeClassName") String typeClassName) {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();

      // Make sure that the requested type class is actually available in the plugin registry.
      // If we don't do this any class in the classpath can be constructed which is not secure.
      //
      List<Class<? extends IPluginType>> pluginTypes = registry.getPluginTypes();
      Set<String> typeClassesSet = new HashSet<>();
      pluginTypes.forEach(c -> typeClassesSet.add(c.getName()));
      if (!typeClassesSet.contains(typeClassName)) {
        throw new HopException(
            "Type class name is not available in the plugin registry: " + typeClassName);
      }

      Class<IPluginType<?>> typeClass = (Class<IPluginType<?>>) Class.forName(typeClassName);
      List<IPlugin> plugins = registry.getPlugins(typeClass);
      return Response.ok(plugins).build();
    } catch (Exception e) {
      String errorMessage =
          "Unexpected error retrieving the list of plugins for plugin type class " + typeClassName;
      return getServerError(errorMessage, e);
    }
  }
}

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

package org.apache.hop.www.api;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.www.HopServerSingleton;
import org.apache.hop.www.api.v1.resources.ExecutionResource;
import org.apache.hop.www.api.v1.resources.LocationResource;
import org.apache.hop.www.api.v1.resources.MetadataResource;
import org.apache.hop.www.api.v1.resources.PluginsResource;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

/**
 * The JSON API of Hop Server, served by Jersey.
 *
 * <p>Resources are registered explicitly rather than discovered by scanning a package: the resource
 * set is small and fixed, and classpath scanning behaves badly under Hop's plugin classloaders.
 *
 * <p>There is deliberately no {@code @ApplicationPath} annotation. The prefix is decided by the
 * Jetty context this application is mounted in, and a plain {@code ServletContextHandler} does not
 * run the servlet initializer which would honour the annotation, so a second declaration of the
 * prefix would only be able to disagree with the real one.
 */
public class HopApiApplication extends ResourceConfig {

  /** The context path the API is mounted at, both in Hop Server and in the Hop Web war. */
  public static final String CONTEXT_PATH = "/hop/api/v1";

  /**
   * Used by Hop Server, which knows its own maps and log channel.
   *
   * @param context the server state to hand to the resources
   */
  public HopApiApplication(HopServerApiContext context) {
    register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(context).to(HopServerApiContext.class);
          }
        });
    // Jackson ships its own mappers for a malformed body; they answer in plain text, which would
    // punch a hole in the JSON error contract. Ours handles those instead.
    register(JacksonFeature.withoutExceptionMappers());
    register(new HopApiExceptionMapper(context.getLog()));
    register(ExecutionResource.class);
    register(LocationResource.class);
    register(MetadataResource.class);
    register(PluginsResource.class);
    property(ServerProperties.WADL_FEATURE_DISABLE, true);
  }

  /**
   * Used when the API is mounted by a servlet container from a {@code web.xml}, as in the Hop Web
   * war. There is no way to hand state in through a servlet declaration, so the server state is
   * taken from {@link HopServerSingleton}.
   */
  public HopApiApplication() {
    this(fromSingleton());
  }

  private static HopServerApiContext fromSingleton() {
    ILogChannel log = new LogChannel("Hop API");
    HopServerSingleton singleton = HopServerSingleton.getInstance();
    return new HopServerApiContext(singleton.getPipelineMap(), singleton.getWorkflowMap(), log);
  }
}

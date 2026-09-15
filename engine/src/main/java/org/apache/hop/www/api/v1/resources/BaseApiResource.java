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

package org.apache.hop.www.api.v1.resources;

import jakarta.inject.Inject;
import org.apache.hop.www.api.HopServerApiContext;

/**
 * Base for the JSON API resources.
 *
 * <p>Everything a resource needs comes from the injected {@link HopServerApiContext}. Resources
 * must never build their own metadata provider or variables: doing so would make the JSON API
 * operate on different metadata than the servlets on the same server.
 */
public abstract class BaseApiResource {

  @Inject protected HopServerApiContext context;
}

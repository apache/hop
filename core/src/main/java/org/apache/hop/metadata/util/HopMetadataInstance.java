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
 *
 */

package org.apache.hop.metadata.util;

import org.apache.hop.core.scope.IHopScope;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

/**
 * The metadata Hop is currently working with, for the code that has nothing else to go on.
 *
 * <p>How far "currently" reaches is a property of the runtime, not of this class. In a client or a
 * server it is the whole process: one project, or one server configuration, set once at startup. In
 * Hop Web it is the session - every user has their own project open in the same JVM, and a process
 * wide answer would hand one user's metadata to another. A runtime that needs a different reach
 * installs its own scope with {@link #setScope(IHopScope)} before anything reads this.
 */
public class HopMetadataInstance {
  private static HopMetadataInstance instance;

  private IHopScope<MultiMetadataProvider> scope = IHopScope.process();

  private HopMetadataInstance() {
    // Nothing to do here.
  }

  public static HopMetadataInstance getInstance() {
    if (instance == null) {
      instance = new HopMetadataInstance();
    }
    return instance;
  }

  /**
   * Change how far "current" reaches. Call this once at startup, before anything reads the metadata
   * provider: Hop Web installs a per session scope here, everything else leaves the process wide
   * default in place.
   *
   * @param scope the scope to hold the metadata provider in, null restores the process wide default
   */
  public static void setScope(IHopScope<MultiMetadataProvider> scope) {
    getInstance().scope = scope == null ? IHopScope.process() : scope;
  }

  public static void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    getInstance().scope.set(metadataProvider);
  }

  public static MultiMetadataProvider getMetadataProvider() {
    return getInstance().scope.get();
  }
}

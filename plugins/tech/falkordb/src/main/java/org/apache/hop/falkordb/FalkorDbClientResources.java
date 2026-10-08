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

package org.apache.hop.falkordb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Transports;

/**
 * The Lettuce client resources shared by all FalkorDB connections: the I/O event loops, the
 * computation threads and the timer. Without them every connection would start and stop threads of
 * its own, and connections are opened often: by the editors, to list indexes, for every execution
 * logged.
 *
 * <p>A client created with shared resources leaves them running when it shuts down. The event loop
 * group of the transport is allocated once more here, so that it isn't shut down and started again
 * when the last open connection closes. The threads of Lettuce are daemon threads: they don't keep
 * the JVM from exiting, so the resources are never shut down.
 */
final class FalkorDbClientResources {

  private static volatile ClientResources resources;

  private FalkorDbClientResources() {}

  /** The shared resources, created the first time they are needed. */
  static ClientResources get() {
    ClientResources shared = resources;
    if (shared == null) {
      synchronized (FalkorDbClientResources.class) {
        shared = resources;
        if (shared == null) {
          shared = DefaultClientResources.create();
          // Keep a reference to the event loop group, which is released by every client
          shared.eventLoopGroupProvider().allocate(Transports.eventLoopGroupClass());
          resources = shared;
        }
      }
    }
    return shared;
  }

  /** A client using the shared resources. Shutting it down leaves them running. */
  static RedisClient createClient(RedisURI uri) {
    return RedisClient.create(get(), uri);
  }
}

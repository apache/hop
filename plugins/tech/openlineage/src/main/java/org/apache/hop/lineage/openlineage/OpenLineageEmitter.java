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

package org.apache.hop.lineage.openlineage;

import io.openlineage.client.OpenLineage.RunEvent;

/**
 * Sends a single OpenLineage {@link RunEvent} to a collector. Abstracted from {@link
 * OpenLineageHttpClient} so the async dispatcher can be tested without real HTTP.
 */
interface OpenLineageEmitter extends AutoCloseable {

  /** Emits one event; returns {@code true} on success. Must not throw on delivery failure. */
  boolean emit(RunEvent event);

  @Override
  void close();
}

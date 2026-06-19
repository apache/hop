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

package org.apache.hop.lineage.spi;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.model.LineageEvent;

/**
 * Plugin SPI: receives batches of lineage events from {@link
 * org.apache.hop.lineage.hub.LineageHub}. Implementations must be registered with {@link
 * org.apache.hop.lineage.plugin.LineageSinkPlugin}.
 */
public interface ILineageSink {

  /**
   * Called once before the hub delivers the first batch. Implementations may no-op.
   *
   * @param variables resolved Hop variables (includes system and hop-config)
   * @param log channel for the hub; sinks may use it or create their own
   */
  default void init(IVariables variables, ILogChannel log) throws HopException {
    // default no-op
  }

  /**
   * Deliver a non-empty batch of events. Implementations should not retain references to the list
   * or events after returning.
   */
  void accept(List<LineageEvent> events) throws HopException;

  /** Called when the hub shuts down or sinks are reloaded. */
  default void shutdown() throws HopException {
    // default no-op
  }
}

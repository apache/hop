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

package org.apache.hop.lineage.hub;

import org.apache.hop.lineage.spi.LineageSinkHealth;

/**
 * One lineage sink as the hub knows it, for a status page.
 *
 * @param name the name of the lineage sink metadata object, or the sink plugin id for a sink
 *     configured with variables
 * @param type the name of the sink plugin, such as OpenLineage or DataHub
 * @param fromRunConfiguration whether runs selected it in a run configuration, rather than it being
 *     configured with variables for every run
 * @param started whether the sink runs: false until its first event, or when it failed to start
 * @param health how its delivery is going; null when it is not started or doesn't say
 * @param startError why it failed to start, or null
 */
public record LineageSinkStatus(
    String name,
    String type,
    boolean fromRunConfiguration,
    boolean started,
    LineageSinkHealth health,
    String startError) {}

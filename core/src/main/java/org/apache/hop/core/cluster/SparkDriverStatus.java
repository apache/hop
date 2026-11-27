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
package org.apache.hop.core.cluster;

/**
 * What the cluster says about a submitted driver.
 *
 * @param state the cluster's own state name, for the log (standalone {@code RUNNING}, YARN {@code
 *     ACCEPTED}, ...)
 * @param location where the driver runs: a worker, a node or a tracking URL; may be null
 * @param message diagnostics of the cluster, for a failure; may be null
 * @param isFinal true once the driver no longer runs
 * @param isSuccess true when it ended successfully
 */
public record SparkDriverStatus(
    String state, String location, String message, boolean isFinal, boolean isSuccess) {}

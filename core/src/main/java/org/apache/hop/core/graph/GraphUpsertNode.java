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

package org.apache.hop.core.graph;

import java.util.Map;

/**
 * A node to create or update: the node with the given label and key values, created when it doesn't
 * exist, after which its properties are set.
 *
 * @param label The label of the node
 * @param keys The key properties which identify the node, never empty
 * @param properties The other properties to set, may be empty
 */
public record GraphUpsertNode(
    String label, Map<String, Object> keys, Map<String, Object> properties) {}

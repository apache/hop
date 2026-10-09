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
 * A relationship (edge) in the results of a graph database statement. Every graph database
 * connection returns its relationships as this, whatever the database calls them.
 *
 * @param id The id of the relationship in the database, as text
 * @param type The relationship type, its label on databases which call it so
 * @param startNodeId The id of the node the relationship starts from
 * @param endNodeId The id of the node the relationship points to
 * @param properties The properties of the relationship, plain Java values
 */
public record GraphRelationshipValue(
    String id, String type, String startNodeId, String endNodeId, Map<String, Object> properties) {}

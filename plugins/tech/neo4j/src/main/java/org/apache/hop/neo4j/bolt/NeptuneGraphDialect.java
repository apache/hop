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

package org.apache.hop.neo4j.bolt;

import java.util.Set;
import org.apache.hop.core.graph.GraphConstraintType;

/** Amazon Neptune manages its indexes itself and has no constraints. */
public class NeptuneGraphDialect extends BoltGraphDialect {

  public static final NeptuneGraphDialect INSTANCE = new NeptuneGraphDialect();

  public NeptuneGraphDialect() {
    super("NEPTUNE");
  }

  @Override
  public boolean isSupportingNodeIndexes() {
    return false;
  }

  @Override
  public boolean isSupportingRelationshipIndexes() {
    return false;
  }

  @Override
  public boolean isSupportingVectorIndexes() {
    return false;
  }

  @Override
  public Set<GraphConstraintType> getNodeConstraintTypes() {
    return Set.of();
  }

  @Override
  public Set<GraphConstraintType> getRelationshipConstraintTypes() {
    return Set.of();
  }

  @Override
  public boolean isSupportingShortestPath() {
    return false;
  }
}

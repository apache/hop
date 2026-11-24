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

package org.apache.hop.core.layout;

import java.util.List;

/**
 * A read-only, ordered graph boundary with no dependency on diagram models or rendering.
 *
 * <p>Nodes must be non-null and distinct by object identity. Distinct instances are separate nodes
 * even when they compare equal; equals/hashCode are not used for node indexing or result lookup.
 * Every edge endpoint must be the same instance as a node in {@link #nodes()}. Stable node and edge
 * order gives repeatable coordinates, including tie-breaking, but the result map's iteration order
 * is undefined. Visit {@link #nodes()} to read results in input order. Callers must not mutate the
 * graph during layout.
 */
public interface LayoutGraph<N, E> {

  List<N> nodes();

  List<E> edges();

  N source(E edge);

  N target(E edge);
}

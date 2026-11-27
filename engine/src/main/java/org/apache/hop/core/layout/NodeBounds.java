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

import lombok.Value;

/**
 * Complete node footprint relative to its anchor, including labels and decorations. Coordinates may
 * be negative; width and height must be nonnegative. All values use the caller's diagram units.
 */
@Value
public class NodeBounds {
  int x;
  int y;
  int width;
  int height;

  public NodeBounds(final int x, final int y, final int width, final int height) {
    if (width < 0 || height < 0) {
      throw new IllegalArgumentException("Node bounds dimensions must be nonnegative");
    }
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
  }
}

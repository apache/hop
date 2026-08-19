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

package org.apache.hop.www;

import org.apache.hop.core.exception.HopException;

/** The server refused a registration because it is already at {@code max_concurrent}. */
public class HopServerAtCapacityException extends HopException {
  private final int occupyingSlots;
  private final int maxConcurrent;

  public HopServerAtCapacityException(int occupyingSlots, int maxConcurrent) {
    super(
        HopServerAdmission.RESULT_AT_CAPACITY
            + ": "
            + occupyingSlots
            + " occupying slots, max "
            + maxConcurrent);
    this.occupyingSlots = occupyingSlots;
    this.maxConcurrent = maxConcurrent;
  }

  public int getOccupyingSlots() {
    return occupyingSlots;
  }

  public int getMaxConcurrent() {
    return maxConcurrent;
  }
}

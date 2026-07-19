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

package org.apache.hop.ui.core.gui;

public interface IGuiPluginCompositeButtonsListener {
  /**
   * Called when a composite button is pressed, <strong>before</strong> the annotated button method
   * runs. Typical use: flush widget contents into {@code sourceObject}.
   *
   * @param sourceObject The source object behind the composite widgets
   */
  void buttonPressed(Object sourceObject);

  /**
   * Called after the annotated button method returns successfully. Typical use: re-bind widgets
   * from {@code sourceObject} when the button mutated it (e.g. load template).
   *
   * @param sourceObject The source object behind the composite widgets
   */
  default void afterButtonPressed(Object sourceObject) {
    // optional
  }
}

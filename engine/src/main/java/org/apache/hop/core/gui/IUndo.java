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

package org.apache.hop.core.gui;

import org.apache.hop.core.undo.ChangeAction;

public interface IUndo {
  /**
   * Add an undo operation to the undo list
   *
   * @param from           array of objects representing the old state
   * @param to             array of objectes representing the new state
   * @param pos            An array of object locations
   * @param prev           An array of points representing the old positions
   * @param curr           An array of points representing the new positions
   * @param type_of_change The type of change that's being done to the pipeline.
   * @param nextAlso       indicates that the next undo operation needs to follow this one.
   */
  void addUndo( Object[] from, Object[] to, int[] pos, Point[] prev, Point[] curr, int typeOfChange,
                boolean nextAlso );

  /**
   * Get the maximum number of undo operations possible
   *
   * @return The maximum number of undo operations that are allowed.
   */
  int getMaxUndo();

  /**
   * Sets the maximum number of undo operations that are allowed.
   *
   * @param mu The maximum number of undo operations that are allowed.
   */
  void setMaxUndo( int mu );

  /**
   * Get the previous undo operation and change the undo pointer
   *
   * @return The undo transaction to be performed.
   */
  ChangeAction previousUndo();

  /**
   * View current undo, don't change undo position
   *
   * @return The current undo transaction
   */
  ChangeAction viewThisUndo();

  /**
   * View previous undo, don't change undo position
   *
   * @return The previous undo transaction
   */
  ChangeAction viewPreviousUndo();

  /**
   * Get the next undo transaction on the list. Change the undo pointer.
   *
   * @return The next undo transaction (for redo)
   */
  ChangeAction nextUndo();

  /**
   * Get the next undo transaction on the list.
   *
   * @return The next undo transaction (for redo)
   */
  ChangeAction viewNextUndo();

}

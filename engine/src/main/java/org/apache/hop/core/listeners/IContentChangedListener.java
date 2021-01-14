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

package org.apache.hop.core.listeners;

/**
 * This listener will be called by the parent object when its content changes.
 *
 * @author matt
 */
public interface IContentChangedListener {

  /**
   * This method will be called when the parent object to which this listener is added, has been changed.
   *
   * @param parentObject The changed object.
   */
  void contentChanged( Object parentObject );

  /**
   * This method will be called when the parent object has been declared safe (or saved, persisted, ...)
   *
   * @param parentObject The safe object.
   */
  void contentSafe( Object parentObject );
}

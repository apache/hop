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

package org.apache.hop.metadata.util;

/** What happened when a metadata object was pointed at another provider. */
public enum MetadataMoveStatus {
  /** The object was already stored in the selected place. */
  UNCHANGED,
  /** The object has not been saved yet. The next save goes to the selected place. */
  RETARGETED,
  /** The object was written to the selected place and removed from the previous one. */
  MOVED,
  /** The selected place already has an object of this name. Nothing was written. */
  COLLISION,
  /**
   * The object was written to the selected place, and removing the previous copy failed. The
   * selected place wins the next time the object is loaded.
   */
  DELETE_FAILED
}

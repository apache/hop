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

package org.apache.hop.ui.hopgui.perspective;

/**
 * Optional interface for perspectives that can open metadata when it is dropped onto the tab folder
 * (e.g. from the metadata tree). When the DropTarget on the tab folder receives metadata transfer
 * data, it delegates to this interface if the perspective implements it.
 */
public interface IMetadataDropReceiver {

  /**
   * Open the given metadata object as a new tab. Called when the user drops a metadata element onto
   * the canvas.
   *
   * @param objectKey the metadata type key (e.g. "pipeline", "workflow")
   * @param name the name of the metadata object
   */
  void openDroppedMetadata(String objectKey, String name);
}

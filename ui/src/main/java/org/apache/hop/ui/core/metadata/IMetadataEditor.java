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

package org.apache.hop.ui.core.metadata;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadata;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;

public interface IMetadataEditor<T extends IHopMetadata> {

  T getMetadata();

  void setMetadata(T metadata);

  /**
   * Return the title of the editor
   *
   * @return The title of this editor
   */
  String getTitle();

  /**
   * Returns the title image of this editor
   *
   * @return
   */
  Image getTitleImage();

  /**
   * Returns the title tool tip text of this editor
   *
   * @return
   */
  String getTitleToolTip();

  void createControl(Composite _parent);

  /**
   * Returns whether the contents of this editor have changed since the last save operation.
   *
   * @return
   */
  boolean hasChanged();

  /** Flag the editor as changed */
  void setChanged();

  /** Clear the changed flag */
  void resetChanged();

  /** Save the editor input */
  void save() throws HopException;

  /** Save the editor input to file after asking for a filename */
  void saveAs(String filename) throws HopException;

  boolean setFocus();

  void dispose();
}

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

package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;

public class HopGuiMetadataSearchable implements ISearchable<IHopMetadata> {

  private IHopMetadataProvider metadataProvider;
  private IHopMetadataSerializer<IHopMetadata> serializer;
  private IHopMetadata searchableObject;
  private Class<IHopMetadata> managedClass;

  public HopGuiMetadataSearchable(
      IHopMetadataProvider metadataProvider,
      IHopMetadataSerializer<IHopMetadata> serializer,
      IHopMetadata searchableObject,
      Class<IHopMetadata> managedClass) {
    this.metadataProvider = metadataProvider;
    this.serializer = serializer;
    this.searchableObject = searchableObject;
    this.managedClass = managedClass;
  }

  @Override
  public String getLocation() {
    return metadataProvider.getDescription();
  }

  @Override
  public String getName() {
    return searchableObject.getName();
  }

  @Override
  public String getType() {
    return serializer.getDescription();
  }

  @Override
  public String getFilename() {
    return null;
  }

  @Override
  public ISearchableCallback getSearchCallback() {
    return (searchable, searchResult) -> {
      // Open the metadata object...
      //
      new MetadataManager(HopGui.getInstance().getVariables(), metadataProvider, managedClass)
          .editMetadata(searchable.getName());
    };
  }

  /**
   * Gets searchableObject
   *
   * @return value of searchableObject
   */
  @Override
  public IHopMetadata getSearchableObject() {
    return searchableObject;
  }

  /** @param searchableObject The searchableObject to set */
  public void setSearchableObject(IHopMetadata searchableObject) {
    this.searchableObject = searchableObject;
  }

  /**
   * Gets managedClass
   *
   * @return value of managedClass
   */
  public Class<IHopMetadata> getManagedClass() {
    return managedClass;
  }

  /** @param managedClass The managedClass to set */
  public void setManagedClass(Class<IHopMetadata> managedClass) {
    this.managedClass = managedClass;
  }
}

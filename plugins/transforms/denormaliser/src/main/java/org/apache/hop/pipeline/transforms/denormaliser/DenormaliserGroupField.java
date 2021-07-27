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

package org.apache.hop.pipeline.transforms.denormaliser;

import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * Utility class that contains the groupfield name, created for backwards compatibility with
 * existing xml
 */
public class DenormaliserGroupField implements Cloneable {

  /** The value to group on */
  @HopMetadataProperty(
      key = "name",
      injectionKeyDescription = "DenormaliserDialog.ColumnInfo.GroupField")
  private String name;

  public DenormaliserGroupField() {}

  public DenormaliserGroupField(DenormaliserGroupField g) {
    this.name = g.name;
  }

  public DenormaliserGroupField clone() {
    return new DenormaliserGroupField(this);
  }

  /**
   * get the group field name
   *
   * @return name of the groupfield
   */
  public String getName() {
    return name;
  }

  /**
   * set the group field name
   *
   * @param name to set the name of the groupfield
   */
  public void setName(String name) {
    this.name = name;
  }
}

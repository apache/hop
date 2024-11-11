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

package org.apache.hop.partition;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * A partition schema allow you to partition a transform according into a number of partitions that
 * run independendly. It allows us to "map"
 */
@HopMetadata(
    key = "partition",
    name = "i18n::PartitionSchema.name",
    description = "i18n::PartitionSchema.description",
    image = "ui/images/partition_schema.svg",
    documentationUrl = "/metadata-types/partition-schema.html",
    hopMetadataPropertyType = HopMetadataPropertyType.PARTITION_SCHEMA)
public class PartitionSchema extends HopMetadataBase implements Cloneable, IHopMetadata {

  @HopMetadataProperty private List<String> partitionIDs;

  @HopMetadataProperty private boolean dynamicallyDefined;

  @HopMetadataProperty private String numberOfPartitions;

  public PartitionSchema() {
    this.dynamicallyDefined = true;
    this.numberOfPartitions = "4";
    this.partitionIDs = new ArrayList<>();
  }

  /**
   * @param name
   * @param partitionIDs
   */
  public PartitionSchema(String name, List<String> partitionIDs) {
    this.name = name;
    this.partitionIDs = partitionIDs;
  }

  @Override
  public Object clone() {
    PartitionSchema partitionSchema = new PartitionSchema();
    partitionSchema.replaceMeta(this);
    return partitionSchema;
  }

  public void replaceMeta(PartitionSchema partitionSchema) {
    this.name = partitionSchema.name;
    this.partitionIDs = new ArrayList<>();
    this.partitionIDs.addAll(partitionSchema.partitionIDs);

    this.dynamicallyDefined = partitionSchema.dynamicallyDefined;
    this.numberOfPartitions = partitionSchema.numberOfPartitions;
  }

  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || name == null) {
      return false;
    }
    return name.equals(((PartitionSchema) obj).name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  public List<String> calculatePartitionIds(IVariables variables) {
    int nrPartitions = Const.toInt(variables.resolve(numberOfPartitions), 0);
    if (dynamicallyDefined) {
      List<String> list = new ArrayList<>();
      for (int i = 0; i < nrPartitions; i++) {
        list.add("Partition-" + (i + 1));
      }
      return list;
    } else {
      return partitionIDs;
    }
  }

  /**
   * @return the partitionIDs
   */
  public List<String> getPartitionIDs() {
    return partitionIDs;
  }

  /**
   * @param partitionIDs the partitionIDs to set
   */
  public void setPartitionIDs(List<String> partitionIDs) {
    this.partitionIDs = partitionIDs;
  }

  /**
   * @return the dynamicallyDefined
   */
  public boolean isDynamicallyDefined() {
    return dynamicallyDefined;
  }

  /**
   * @param dynamicallyDefined the dynamicallyDefined to set
   */
  public void setDynamicallyDefined(boolean dynamicallyDefined) {
    this.dynamicallyDefined = dynamicallyDefined;
  }

  /**
   * @return the number of partitions
   */
  public String getNumberOfPartitions() {
    return numberOfPartitions;
  }

  /**
   * @param numberOfPartitions the number of partitions to set...
   */
  public void setNumberOfPartitions(String numberOfPartitions) {
    this.numberOfPartitions = numberOfPartitions;
  }
}

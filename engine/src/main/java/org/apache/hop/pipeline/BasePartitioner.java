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

package org.apache.hop.pipeline;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;

/**
 * Implements common functionality needed by partitioner plugins.
 */
public abstract class BasePartitioner implements IPartitioner {

  protected TransformPartitioningMeta meta;
  protected int nrPartitions = -1;
  protected String id;
  protected String description;

  /**
   * Instantiates a new base partitioner.
   */
  public BasePartitioner() {
  }

  /*
   * (non-Javadoc)
   *
   * @see java.lang.Object#clone()
   */
  public IPartitioner clone() {
    IPartitioner partitioner = getInstance();
    partitioner.setId( id );
    partitioner.setDescription( description );
    partitioner.setMeta( meta );
    return partitioner;
  }

  /**
   * Gets the nr partitions.
   *
   * @return the nr partitions
   */
  public int getNrPartitions() {
    return nrPartitions;
  }

  /**
   * Sets the nr partitions.
   *
   * @param nrPartitions the new nr partitions
   */
  public void setNrPartitions( int nrPartitions ) {
    this.nrPartitions = nrPartitions;
  }

  /**
   * Initialises the partitioner.
   *
   * @param variables the variables to use to resolve variables expressions
   * @param rowMeta the row meta
   * @throws HopException the hop exception
   */
  public void init( IVariables variables, IRowMeta rowMeta ) throws HopException {

    if ( nrPartitions < 0 ) {
      nrPartitions = meta.getPartitionSchema().calculatePartitionIds(variables).size();
    }

  }

  /**
   * Gets the meta.
   *
   * @return the meta
   */
  public TransformPartitioningMeta getMeta() {
    return meta;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.IPartitioner#setMeta(org.apache.hop.pipeline.transform.TransformPartitioningMeta)
   */
  public void setMeta( TransformPartitioningMeta meta ) {
    this.meta = meta;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.IPartitioner#getInstance()
   */
  public abstract IPartitioner getInstance();

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.IPartitioner#getDescription()
   */
  public String getDescription() {
    return description;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.IPartitioner#setDescription(java.lang.String)
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.IPartitioner#getId()
   */
  public String getId() {
    return id;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.pipeline.IPartitioner#setId(java.lang.String)
   */
  public void setId( String id ) {
    this.id = id;
  }

}

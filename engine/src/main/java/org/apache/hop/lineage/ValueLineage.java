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

package org.apache.hop.lineage;

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * This class contains value lineage information.<br>
 * That means that we will have information on where and how a certain value is originating, being manipulated etc.<br>
 *
 * @author matt
 */
public class ValueLineage {
  private PipelineMeta pipelineMeta;
  private IValueMeta valueMeta;

  private List<TransformMeta> sourceTransforms;

  /**
   * Create a new ValueLineage object with an empty set of source transforms.
   *
   * @param valueMeta
   */
  public ValueLineage( PipelineMeta pipelineMeta, IValueMeta valueMeta ) {
    this.pipelineMeta = pipelineMeta;
    this.valueMeta = valueMeta;
    this.sourceTransforms = new ArrayList<>();
  }

  /**
   * @return the pipelineMeta
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta the pipelineMeta to set
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  /**
   * @return the valueMeta
   */
  public IValueMeta getValueMeta() {
    return valueMeta;
  }

  /**
   * @param valueMeta the valueMeta to set
   */
  public void setValueMeta( IValueMeta valueMeta ) {
    this.valueMeta = valueMeta;
  }

  /**
   * @return the sourceTransforms
   */
  public List<TransformMeta> getSourceTransforms() {
    return sourceTransforms;
  }

  /**
   * @param sourceTransforms the sourceTransforms to set
   */
  public void setSourceTransforms( List<TransformMeta> sourceTransforms ) {
    this.sourceTransforms = sourceTransforms;
  }

}

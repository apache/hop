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

package org.apache.hop.www.jaxrs;

import org.apache.hop.pipeline.transform.TransformStatus;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class PipelineStatus {

  private String id;
  private String name;
  private String status;
  private List<TransformStatus> transformStatuses = new ArrayList<>();

  public PipelineStatus() {
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus( String status ) {
    this.status = status;
  }

  public List<TransformStatus> getTransformStatuses() {
    return transformStatuses;
  }

  public void setTransformStatuses( List<TransformStatus> transformStatuses ) {
    this.transformStatuses = transformStatuses;
  }

  public void addTransformStatus( TransformStatus status ) {
    transformStatuses.add( status );
  }

}

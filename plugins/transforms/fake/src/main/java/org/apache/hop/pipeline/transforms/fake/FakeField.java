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
package org.apache.hop.pipeline.transforms.fake;

import org.apache.commons.lang.StringUtils;

public class FakeField {
  private String name;
  private String type;
  private String topic;

  public FakeField() {
  }

  public FakeField( String name, String type, String topic ) {
    this.name = name;
    this.type = type;
    this.topic = topic;
  }

  public FakeField( FakeField f ) {
    this.name = f.name;
    this.type = f.type;
    this.topic = f.topic;
  }

  public boolean isValid() {
    return ( StringUtils.isNotEmpty( name ) && StringUtils.isNotEmpty( type ) && StringUtils.isNotEmpty( topic ) );
  }


  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType( String type ) {
    this.type = type;
  }

  /**
   * Gets topic
   *
   * @return value of topic
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic The topic to set
   */
  public void setTopic( String topic ) {
    this.topic = topic;
  }
}

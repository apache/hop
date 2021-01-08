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

package org.apache.hop.testing;

import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * This class simply replaces all occurrences of a certain database connection with another one.
 * It allows developers to point to a test database for lookup data and database related transforms like
 * database lookup, dimension lookup and so on.
 *
 * @author matt
 */
public class PipelineUnitTestDatabaseReplacement {

  @HopMetadataProperty( key = "original_connection" )
  private String originalDatabaseName;

  @HopMetadataProperty( key = "replacement_connection" )
  private String replacementDatabaseName;

  public PipelineUnitTestDatabaseReplacement( String originalDatabaseName, String replacementDatabaseName ) {
    this();
    this.originalDatabaseName = originalDatabaseName;
    this.replacementDatabaseName = replacementDatabaseName;
  }

  public PipelineUnitTestDatabaseReplacement() {
  }

  public String getOriginalDatabaseName() {
    return originalDatabaseName;
  }

  public void setOriginalDatabaseName( String originalDatabaseName ) {
    this.originalDatabaseName = originalDatabaseName;
  }

  public String getReplacementDatabaseName() {
    return replacementDatabaseName;
  }

  public void setReplacementDatabaseName( String replacementDatabaseName ) {
    this.replacementDatabaseName = replacementDatabaseName;
  }
}

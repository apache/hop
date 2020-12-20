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

package org.apache.hop.pipeline.transforms.tokenreplacement;


import org.apache.hop.core.Const;
import org.apache.hop.core.injection.Injection;

/**
 * Describes a single field in a text file
 *
 */
public class TokenReplacementField implements Cloneable {
  @Injection(name="TOKEN_FIELDNAME")
  private String name;

    @Injection(name="TOKEN_NAME")
    private String tokenName;

 public TokenReplacementField( String name, String tokenName ) {
    this.name = name;
    this.tokenName = tokenName;
  }

  public TokenReplacementField() {
  }
  
  public boolean equals( Object obj ) {
    if( obj instanceof TokenReplacementField ) {
      TokenReplacementField field = (TokenReplacementField) obj;

      return name.equals( field.getName() ) && tokenName.equals( field.tokenName );
    }
    return false;
  }

  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * Get the stream field name.
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the stream field name.
   * @param fieldname the name of the field
   */
  public void setName( String fieldname ) {
    this.name = fieldname;
  }

  /**
   * Get the name of the token
   * If the token name is null returns the stream field name
   * @return tokenName
   */
  public String getTokenName() {
	  return Const.NVL( tokenName, name );
  }

  /**
   * Set the name of the token
   * @param tokenName the name of the token
   */
  public void setTokenName( String tokenName ) {
	  this.tokenName = tokenName;
  }

  public String toString() {
    return name;
  }


}

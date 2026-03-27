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

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a text file */
@Getter
@Setter
public class TokenReplacementField implements Cloneable {
  @HopMetadataProperty(
      key = "field_name",
      injectionKey = "TOKEN_FIELDNAME",
      injectionKeyDescription = "TokenReplacement.Injection.TOKEN_FIELDNAME")
  private String name;

  @HopMetadataProperty(
      key = "token_name",
      injectionKey = "TOKEN_NAME",
      injectionKeyDescription = "TokenReplacement.Injection.TOKEN_NAME")
  private String tokenName;

  public TokenReplacementField() {}

  public TokenReplacementField(TokenReplacementField f) {
    this();
    this.name = f.name;
    this.tokenName = f.tokenName;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TokenReplacementField that = (TokenReplacementField) o;
    return Objects.equals(name, that.name) && Objects.equals(tokenName, that.tokenName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, tokenName);
  }

  @Override
  public TokenReplacementField clone() {
    return new TokenReplacementField(this);
  }
}

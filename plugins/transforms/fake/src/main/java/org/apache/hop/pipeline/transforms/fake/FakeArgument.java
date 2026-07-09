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

import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * A single argument passed to a parameterized DataFaker generator.
 *
 * <p>The ordered list of argument {@link #type}s of a {@link FakeField} is also its method
 * signature, which is what lets the runtime pick the correct overload (e.g. {@code
 * numberBetween(int,int)} versus {@code numberBetween(long,long)}).
 */
public class FakeArgument {

  /**
   * The declared parameter type. Either a primitive keyword ({@code int}, {@code long}, {@code
   * double}, {@code float}, {@code short}, {@code byte}, {@code boolean}, {@code char}), the token
   * {@code string}, or the fully qualified class name of an enum.
   */
  @HopMetadataProperty(injectionKeyDescription = "Fake.Injection.Argument.Type")
  private String type;

  /** The argument value. May contain pipeline variables, resolved at runtime. */
  @HopMetadataProperty(injectionKeyDescription = "Fake.Injection.Argument.Value")
  private String value;

  public FakeArgument() {}

  public FakeArgument(String type, String value) {
    this.type = type;
    this.value = value;
  }

  public FakeArgument(FakeArgument a) {
    this.type = a.type;
    this.value = a.value;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}

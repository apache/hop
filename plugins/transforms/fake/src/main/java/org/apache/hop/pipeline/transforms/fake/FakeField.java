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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * One generated output field of the Fake transform.
 *
 * <p>{@code type} is the DataFaker {@code Faker} accessor (e.g. {@code name}), {@code topic} is the
 * provider method to call (e.g. {@code firstName}). {@code arguments} carries the parameters for
 * parameterized generators and is empty for the simple zero-argument ones - which keeps pipelines
 * created before parameter support was added fully compatible.
 */
public class FakeField {
  @HopMetadataProperty(injectionKeyDescription = "Fake.Injection.Name")
  private String name;

  @HopMetadataProperty(injectionKeyDescription = "Fake.Injection.Type")
  private String type;

  @HopMetadataProperty(injectionKeyDescription = "Fake.Injection.Topic")
  private String topic;

  @HopMetadataProperty(
      key = "argument",
      groupKey = "arguments",
      injectionGroupDescription = "Fake.Injection.Arguments")
  private List<FakeArgument> arguments;

  public FakeField() {
    this.arguments = new ArrayList<>();
  }

  public FakeField(String name, String type, String topic) {
    this(name, type, topic, new ArrayList<>());
  }

  public FakeField(String name, String type, String topic, List<FakeArgument> arguments) {
    this.name = name;
    this.type = type;
    this.topic = topic;
    this.arguments = arguments == null ? new ArrayList<>() : arguments;
  }

  public FakeField(FakeField f) {
    this.name = f.name;
    this.type = f.type;
    this.topic = f.topic;
    this.arguments = new ArrayList<>();
    for (FakeArgument argument : f.getArguments()) {
      this.arguments.add(new FakeArgument(argument));
    }
  }

  public boolean isValid() {
    return StringUtils.isNotEmpty(name)
        && StringUtils.isNotEmpty(type)
        && StringUtils.isNotEmpty(topic);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public List<FakeArgument> getArguments() {
    if (arguments == null) {
      arguments = new ArrayList<>();
    }
    return arguments;
  }

  public void setArguments(List<FakeArgument> arguments) {
    this.arguments = arguments;
  }
}

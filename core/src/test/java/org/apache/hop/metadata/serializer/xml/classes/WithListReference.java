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
 *
 */

package org.apache.hop.metadata.serializer.xml.classes;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHasName;

@Getter
@Setter
public class WithListReference {
  @HopMetadataProperty(groupKey = "steps", key = "step")
  private List<Step> steps;

  @HopMetadataProperty(groupKey = "order", key = "hop")
  private List<Hop> hops;

  public WithListReference() {
    this.steps = new ArrayList<>();
    this.hops = new ArrayList<>();
  }

  @Getter
  @Setter
  public static class Hop {
    @HopMetadataProperty(storeWithName = true, lookupInList = "steps")
    private Step from;

    @HopMetadataProperty(storeWithName = true, lookupInList = "steps")
    private Step to;

    public Hop() {
      // Empty
    }

    public Hop(Step from, Step to) {
      this.from = from;
      this.to = to;
    }
  }

  @Getter
  @Setter
  public static class Step implements IHasName {
    @HopMetadataProperty private String name;
    @HopMetadataProperty private String description;
    @HopMetadataProperty private boolean enabled;

    public Step() {
      // Empty
    }

    public Step(String name, String description, boolean enabled) {
      this.name = name;
      this.description = description;
      this.enabled = enabled;
    }
  }
}

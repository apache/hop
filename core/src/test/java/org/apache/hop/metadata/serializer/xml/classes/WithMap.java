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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class WithMap {

  @HopMetadataProperty(
      key = "mapping",
      groupKey = "mappings",
      inline = true,
      mapKeyClass = Key.class,
      mapValueClass = Value.class)
  private Map<Key, Value> mappings;

  public WithMap() {
    mappings = new HashMap<>();
  }

  @Getter
  @Setter
  public static class Key {
    @HopMetadataProperty(key = "f1")
    private String f1;

    @HopMetadataProperty(key = "f2")
    private String f2;

    public Key() {
      // Nothing
    }

    public Key(String f1, String f2) {
      this();
      this.f1 = f1;
      this.f2 = f2;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Key key)) return false;
      return Objects.equals(f1, key.f1) && Objects.equals(f2, key.f2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(f1, f2);
    }
  }

  @Getter
  @Setter
  public static class Value {
    @HopMetadataProperty(key = "v1")
    private String v1;

    @HopMetadataProperty(key = "v2")
    private String v2;

    public Value() {
      // Nothing
    }

    public Value(String v1, String v2) {
      this();
      this.v1 = v1;
      this.v2 = v2;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Value value)) return false;
      return Objects.equals(v1, value.v1) && Objects.equals(v2, value.v2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(v1, v2);
    }
  }
}

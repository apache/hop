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

package org.apache.hop.neo4j.execution.builder;

public class CypherMatchBuilder extends BaseCypherBuilder {

  protected CypherMatchBuilder() {
    super();
  }

  public static CypherMatchBuilder of() {
    return new CypherMatchBuilder();
  }

  public CypherMatchBuilder withMatch(String label, String alias, String id1, Object value1) {
    String key1=alias+"_"+id1;
    cypher
        .append("MATCH(")
        .append(alias)
        .append(":")
        .append(label)
        .append("{ ")
        .append(id1)
        .append(" : $")
        .append(key1)
        .append(" }) ");
    addParameter(key1, value1);
    return this;
  }

  public CypherMatchBuilder withMatch(
      String label, String alias, String id1, Object value1, String id2, Object value2) {
    String key1=alias+"_"+id1;
    String key2=alias+"_"+id2;
    cypher
        .append("MATCH(")
        .append(alias)
        .append(":")
        .append(label)
        .append("{ ")
        .append(id1)
        .append(" : $")
        .append(key1)
        .append(", ")
        .append(id2)
        .append(" : $")
        .append(key2)
        .append(" }) ");
    addParameter(key1, value1);
    addParameter(key2, value2);
    return this;
  }

  public CypherMatchBuilder withMatch(
      String label,
      String alias,
      String id1,
      Object value1,
      String id2,
      Object value2,
      String id3,
      Object value3) {
    String key1=alias+"_"+id1;
    String key2=alias+"_"+id2;
    String key3=alias+"_"+id3;
    cypher
        .append("MATCH(")
        .append(alias)
        .append(":")
        .append(label)
        .append("{ ")
        .append(id1)
        .append(" : $")
        .append(key1)
        .append(", ")
        .append(id2)
        .append(" : $")
        .append(key2)
        .append(", ")
        .append(id3)
        .append(" : $")
        .append(key3)
        .append(" }) ");
    addParameter(key1, value1);
    addParameter(key2, value2);
    addParameter(key3, value3);
    return this;
  }

  public CypherMatchBuilder withMatch(
      String label,
      String alias,
      String id1,
      Object value1,
      String id2,
      Object value2,
      String id3,
      Object value3,
      String id4,
      Object value4) {
    String key1=alias+"_"+id1;
    String key2=alias+"_"+id2;
    String key3=alias+"_"+id3;
    String key4=alias+"_"+id4;
    cypher
        .append("MATCH(")
        .append(alias)
        .append(":")
        .append(label)
        .append("{ ")
        .append(id1)
        .append(" : $")
        .append(key1)
        .append(", ")
        .append(id2)
        .append(" : $")
        .append(key2)
        .append(", ")
        .append(id3)
        .append(" : $")
        .append(key3)
        .append(", ")
        .append(id4)
        .append(" : $")
        .append(key4)
        .append(" }) ");
    addParameter(key1, value1);
    addParameter(key2, value2);
    addParameter(key3, value3);
    addParameter(key4, value4);
    return this;
  }
}

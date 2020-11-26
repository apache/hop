/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.databases.cassandra.util;

/** @author Tatsiana_Kasiankova */
public enum Partitioners {
  MURMUR3("Murmur3Partitioner", "org.apache.cassandra.db.marshal.LongType"),
  RANDOM("RandomPartitioner", "org.apache.cassandra.db.marshal.IntegerType"),
  BYTEORDERED("ByteOrderedPartitioner", "org.apache.cassandra.db.marshal.BytesType");

  private final String name;

  private final String type;

  /**
   * @param name
   * @param type
   */
  private Partitioners(String name, String type) {
    this.name = name;
    this.type = type;
  }

  /** @return the name */
  public String getName() {
    return name;
  }

  /** @return the type */
  public String getType() {
    return type;
  }

  public static Partitioners getFromString(String string) {
    if (string == null) {
      return MURMUR3;
    }

    for (Partitioners prs : Partitioners.values()) {
      if (string.endsWith(prs.getName())) {
        return prs;
      }
    }
    return MURMUR3;
  }
}

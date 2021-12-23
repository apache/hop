/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.shared;

import org.apache.hop.core.logging.ILogChannel;
import org.neo4j.driver.Session;

import java.util.List;

public class NeoConnectionUtils {
  private static final Class<?> PKG =
      NeoConnectionUtils.class; // for i18n purposes, needed by Translator2!!

  public static final void createNodeIndex(
      ILogChannel log, Session session, List<String> labels, List<String> keyProperties) {

    // If we have no properties or labels, we have nothing to do here
    //
    if (keyProperties.size() == 0) {
      return;
    }
    if (labels.size() == 0) {
      return;
    }

    // We only use the first label for index or constraint
    //
    String labelsClause = ":" + labels.get(0);

    // CREATE CONSTRAINT ON (n:NodeLabel) ASSERT n.property1 IS UNIQUE
    //
    if (keyProperties.size() == 1) {
      String property = keyProperties.get(0);
      String constraintCypher =
          "CREATE CONSTRAINT IF NOT EXISTS ON (n"
              + labelsClause
              + ") ASSERT n."
              + property
              + " IS UNIQUE;";

      log.logDetailed("Creating constraint : " + constraintCypher);
      session.run(constraintCypher);

      // This creates an index, no need to go further here...
      //
      return;
    }

    // Composite index case...
    //
    // CREATE INDEX ON :NodeLabel(property, property2, ...)
    //
    String indexCypher = "CREATE INDEX IF NOT EXISTS ON ";

    indexCypher += labelsClause;
    indexCypher += "(";
    boolean firstProperty = true;
    for (String property : keyProperties) {
      if (firstProperty) {
        firstProperty = false;
      } else {
        indexCypher += ", ";
      }
      indexCypher += property;
    }
    indexCypher += ")";

    log.logDetailed("Creating index : " + indexCypher);
    session.run(indexCypher);
  }
}

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
 *
 */

package org.apache.hop.neo4j.transforms.cypher;

import org.apache.hop.core.exception.HopException;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import java.util.Map;

public class CypherTransactionWork implements TransactionWork<Void> {
  private final Cypher transform;
  private final Object[] currentRow;
  private final boolean unwind;
  private String cypher;
  private Map<String, Object> unwindMap;

  public CypherTransactionWork(
      Cypher transform,
      Object[] currentRow,
      boolean unwind,
      String cypher,
      Map<String, Object> unwindMap) {
    this.transform = transform;
    this.currentRow = currentRow;
    this.unwind = unwind;
    this.cypher = cypher;
    this.unwindMap = unwindMap;
  }

  @Override
  public Void execute(Transaction tx) {
    try {
      Result result = tx.run(cypher, unwindMap);
      transform.getResultRows(result, currentRow, unwind);
      return null;
    } catch (HopException e) {
      throw new RuntimeException("Unable to execute cypher statement '" + cypher + "'", e);
    }
  }

  /**
   * Gets cypher
   *
   * @return value of cypher
   */
  public String getCypher() {
    return cypher;
  }

  /** @param cypher The cypher to set */
  public void setCypher(String cypher) {
    this.cypher = cypher;
  }

  /**
   * Gets unwindMap
   *
   * @return value of unwindMap
   */
  public Map<String, Object> getUnwindMap() {
    return unwindMap;
  }

  /** @param unwindMap The unwindMap to set */
  public void setUnwindMap(Map<String, Object> unwindMap) {
    this.unwindMap = unwindMap;
  }
}

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

package org.apache.hop.neo4j.perspective;

import org.apache.hop.core.Const;

import java.util.ArrayList;
import java.util.List;

public class HistoryResult {

  private String id;
  private String name;
  private String type;
  private String copy;
  private Long input;
  private Long output;
  private Long read;
  private Long written;
  private Long rejected;
  private Long errors;
  private Long durationMs;
  private String loggingText;
  private String registrationDate;

  private List<List<HistoryResult>> shortestPaths;
  private Boolean root;

  public HistoryResult() {
    shortestPaths = new ArrayList<>();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /** @param type The type to set */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets copy
   *
   * @return value of copy
   */
  public String getCopy() {
    return copy;
  }

  /** @param copy The copy to set */
  public void setCopy(String copy) {
    this.copy = copy;
  }

  /**
   * Gets input
   *
   * @return value of input
   */
  public Long getInput() {
    return input;
  }

  /** @param input The input to set */
  public void setInput(Long input) {
    this.input = input;
  }

  /**
   * Gets output
   *
   * @return value of output
   */
  public Long getOutput() {
    return output;
  }

  /** @param output The output to set */
  public void setOutput(Long output) {
    this.output = output;
  }

  /**
   * Gets read
   *
   * @return value of read
   */
  public Long getRead() {
    return read;
  }

  /** @param read The read to set */
  public void setRead(Long read) {
    this.read = read;
  }

  /**
   * Gets written
   *
   * @return value of written
   */
  public Long getWritten() {
    return written;
  }

  /** @param written The written to set */
  public void setWritten(Long written) {
    this.written = written;
  }

  /**
   * Gets rejected
   *
   * @return value of rejected
   */
  public Long getRejected() {
    return rejected;
  }

  /** @param rejected The rejected to set */
  public void setRejected(Long rejected) {
    this.rejected = rejected;
  }

  /**
   * Gets errors
   *
   * @return value of errors
   */
  public Long getErrors() {
    return errors;
  }

  /** @param errors The errors to set */
  public void setErrors(Long errors) {
    this.errors = errors;
  }

  /**
   * Gets loggingText
   *
   * @return value of loggingText
   */
  public String getLoggingText() {
    return loggingText;
  }

  /** @param loggingText The loggingText to set */
  public void setLoggingText(String loggingText) {
    this.loggingText = loggingText;
  }

  /**
   * Gets registrationDate
   *
   * @return value of registrationDate
   */
  public String getRegistrationDate() {
    return registrationDate;
  }

  /** @param registrationDate The registrationDate to set */
  public void setRegistrationDate(String registrationDate) {
    this.registrationDate = registrationDate;
  }

  /**
   * Gets shortestPaths
   *
   * @return value of shortestPaths
   */
  public List<List<HistoryResult>> getShortestPaths() {
    return shortestPaths;
  }

  /** @param shortestPaths The shortestPaths to set */
  public void setShortestPaths(List<List<HistoryResult>> shortestPaths) {
    this.shortestPaths = shortestPaths;
  }

  /**
   * Gets durationMs
   *
   * @return value of durationMs
   */
  public Long getDurationMs() {
    return durationMs;
  }

  /** @param durationMs The durationMs to set */
  public void setDurationMs(Long durationMs) {
    this.durationMs = durationMs;
  }

  public void setRoot(Boolean root) {
    this.root = root;
  }

  public Boolean isRoot() {
    return root;
  }

  public String getExecutionInfoCommand() {
    StringBuilder cmd = new StringBuilder();

    cmd.append(
            "MATCH(ex:Execution { name : \""
                + getName()
                + "\", type : \""
                + getType()
                + "\", id : \""
                + getId()
                + "\"}) ")
        .append(Const.CR);
    cmd.append("RETURN ex ").append(Const.CR);

    return cmd.toString();
  }

  public String getErrorPathCommand() {
    StringBuilder cmd = new StringBuilder();

    cmd.append(
            "MATCH(top:Execution { name : \""
                + getName()
                + "\", type : \""
                + getType()
                + "\", id : \""
                + getId()
                + "\"})-[rel:EXECUTES*]-(err:Execution) ")
        .append(Const.CR);
    cmd.append("   , p=shortestpath((top)-[:EXECUTES*]-(err)) ").append(Const.CR);
    cmd.append("WHERE top.registrationDate IS NOT NULL ").append(Const.CR);
    cmd.append("  AND err.errors > 0 ").append(Const.CR);
    cmd.append("  AND size((err)-[:EXECUTES]->())=0 ").append(Const.CR);
    cmd.append("RETURN p ").append(Const.CR);
    cmd.append("ORDER BY size(RELATIONSHIPS(p)) DESC ").append(Const.CR);
    cmd.append("LIMIT 5").append(Const.CR);

    return cmd.toString();
  }

  public String getErrorPathWithMetadataCommand(int pathIndex) {
    StringBuilder cmd = new StringBuilder();

    cmd.append(
            "MATCH(top:Execution { name : \""
                + getName()
                + "\", type : \""
                + getType()
                + "\", id : \""
                + getId()
                + "\"})-[rel:EXECUTES*]-(err:Execution) ")
        .append(Const.CR);
    cmd.append("   , p=shortestpath((top)-[:EXECUTES*]-(err)) ").append(Const.CR);
    cmd.append("WHERE top.registrationDate IS NOT NULL ").append(Const.CR);
    cmd.append("  AND err.errors > 0 ").append(Const.CR);
    cmd.append("  AND size((err)-[:EXECUTES]->())=0 ").append(Const.CR);

    // Now link the metadata...
    //
    StringBuilder returns = new StringBuilder("RETURN p");
    int matchIndex = 1;

    List<HistoryResult> shortestPath = getShortestPaths().get(pathIndex);

    for (HistoryResult result : shortestPath) {
      String metaLabel = null;
      if (result.getType().equals("PIPELINE")) {
        metaLabel = "Pipeline";
      } else if (result.getType().equals("WORKFLOW")) {
        metaLabel = "Workflow";
      } else if (result.getType().equals("ACTION")) {
        metaLabel = "Action";
      } else if (result.getType().equals("TRANSFORM")) {
        metaLabel = "Transform";
      }
      if (metaLabel != null) {
        cmd.append(
                "MATCH (:Execution { type : \""
                    + result.getType()
                    + "\", id : \""
                    + result.getId()
                    + "\"})-[metaRel"
                    + matchIndex
                    + "]->(meta"
                    + matchIndex
                    + ":"
                    + metaLabel
                    + ") ")
            .append(Const.CR);
        returns.append(", metaRel" + matchIndex + ", meta" + matchIndex);
        matchIndex++;
      }
    }
    cmd.append(returns.toString()).append(" ").append(Const.CR);
    cmd.append("LIMIT 1 ").append(Const.CR);

    return cmd.toString();
  }
}

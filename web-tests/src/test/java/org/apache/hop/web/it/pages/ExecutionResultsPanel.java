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

package org.apache.hop.web.it.pages;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;

/**
 * The panel that appears under a graph once it has been run: Logging, Metrics and Problems.
 *
 * <p>Only the metrics table is read. The logging tab is a StyledText, which RAP does not paint as
 * text a browser can read back, so a test cannot see what it says; the metrics table is an ordinary
 * table and says the same thing more precisely anyway - how many rows each transform actually moved
 * and whether it finished.
 */
public class ExecutionResultsPanel {

  /** The metrics table has fifteen columns; no other table in the panel comes close. */
  private static final int MIN_COLUMNS = 10;

  /**
   * The metrics table, as headers and rows of cells.
   *
   * <p>Found by its own column names rather than by position, and taken from the innermost element
   * that has both those names and rows under it - the table is nested several containers deep and
   * every one of them "contains" the header text.
   *
   * <p>A row is a run of absolutely positioned leaf elements: one background per column, plus one
   * text element for every cell that has ever held a value. The text elements cannot be read in
   * document order. RAP creates a cell's text element the first time the cell gets a value and
   * appends it to the row, so a metric that was still empty on the first paint ("Duration") ends up
   * after "Status" and shifts every column read by position. Each text element is put in the column
   * whose header starts at or left of it instead, and a cell that never held a value stays empty.
   */
  private static final String METRICS =
      "const left=e=>parseFloat(e.style.left)||0;"
          + "const candidates=[...document.querySelectorAll('div')].filter("
          + "d=>d.textContent.includes('Transform Name')&&d.textContent.includes('Written (rows)'));"
          + "for(let i=candidates.length-1;i>=0;i--){"
          + "const table=candidates[i];"
          + "const head=[...table.children].find(c=>c.textContent.includes('Transform Name'));"
          + "if(!head)continue;"
          + "const columns=[...head.children].filter(c=>c.textContent.trim().length>0)"
          + ".map(c=>({name:c.textContent.trim(),left:left(c)})).sort((a,b)=>a.left-b.left);"
          + "if(columns.length<"
          + MIN_COLUMNS
          + ")continue;"
          // The grid also paints empty filler rows below the last transform: skip those.
          + "const rows=[...table.querySelectorAll('div')].filter(d=>d!==head"
          + "&&d.children.length>=columns.length&&d.textContent.trim().length>0"
          + "&&[...d.children].every(c=>c.children.length===0));"
          + "if(rows.length===0)continue;"
          + "return {headers:columns.map(c=>c.name),rows:rows.map(r=>{"
          + "const cells=columns.map(()=>'');"
          + "for(const c of r.children){const text=c.textContent.trim();if(!text)continue;"
          + "let n=-1;columns.forEach((col,k)=>{if(col.left<=left(c))n=k;});"
          + "if(n>=0)cells[n]=text;}"
          + "return cells;})};}"
          + "return {headers:[],rows:[]};";

  private final WebDriver driver;
  private final HopGuiPage hopGui;

  ExecutionResultsPanel(WebDriver driver, HopGuiPage hopGui) {
    this.driver = driver;
    this.hopGui = hopGui;
  }

  /** Brings one of the panel's tabs to the front. */
  public ExecutionResultsPanel selectTab(String name) {
    hopGui.clickButton(name);
    return this;
  }

  /**
   * One entry per transform, each column of the metrics table keyed by its header.
   *
   * <p>Empty until a run has actually produced metrics, which is what callers wait on.
   */
  public List<Map<String, String>> metrics() {
    @SuppressWarnings("unchecked")
    Map<String, Object> table =
        (Map<String, Object>) ((JavascriptExecutor) driver).executeScript(METRICS);
    @SuppressWarnings("unchecked")
    List<String> headers = (List<String>) table.get("headers");
    @SuppressWarnings("unchecked")
    List<List<String>> rows = (List<List<String>>) table.get("rows");
    return rows.stream().map(row -> row(headers, row)).toList();
  }

  private static Map<String, String> row(List<String> headers, List<String> cells) {
    if (headers.size() != cells.size()) {
      throw new IllegalStateException(
          "The metrics table has " + headers.size() + " columns but a row of " + cells.size());
    }
    Map<String, String> row = new LinkedHashMap<>();
    for (int i = 0; i < headers.size(); i++) {
      row.put(headers.get(i), cells.get(i));
    }
    return row;
  }

  /** The metrics of one transform, or null while it has none. */
  public Map<String, String> metricsOf(String transformName) {
    return metrics().stream()
        .filter(row -> transformName.equals(row.get("Transform Name")))
        .findFirst()
        .orElse(null);
  }
}

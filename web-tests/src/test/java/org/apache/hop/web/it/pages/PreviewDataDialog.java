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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;

/**
 * "Examine preview data": the rows a transform produced when it was previewed.
 *
 * <p>This is the only place in Hop Web where a test can see actual data rather than a count of it,
 * which makes it the strongest thing the suite can assert. A preview is also a whole pipeline
 * execution of its own, run through a different path from the Run button, so it is worth having
 * even where the counts alone would do.
 */
public class PreviewDataDialog {

  /** The header line the dialog puts above the grid, for example "... add constants (1 rows)". */
  private static final Pattern ROW_COUNT = Pattern.compile("\\((\\d+) rows?\\)");

  /**
   * The cells of every row in the grid, row by row.
   *
   * <p>Rows lay out as alternating spacer and cell elements, as everywhere else in a RAP table, so
   * only the odd children are cells. Rows that are entirely empty are the grid's own filler and are
   * left out.
   */
  private static final String GRID_ROWS =
      "const shells=[...document.body.children].filter(d=>{"
          + "if(d.tagName!=='DIV')return false;"
          + "const z=parseInt(getComputedStyle(d).zIndex);"
          + "const r=d.getBoundingClientRect();"
          + "return z>=100000&&r.width>100&&r.height>100;});"
          + "const top=shells[shells.length-1];"
          + "if(!top)return [];"
          + "return [...top.querySelectorAll('div')]"
          + ".filter(d=>d.children.length>=4&&d.children.length%2===0"
          + "&&[...d.children].every(c=>c.children.length===0))"
          + ".map(r=>[...r.children].filter((c,n)=>n%2===1).map(c=>c.textContent.trim()))"
          + ".filter(cells=>cells.some(c=>c.length>0));";

  private final WebDriver driver;
  private final HopGuiPage hopGui;

  PreviewDataDialog(WebDriver driver, HopGuiPage hopGui) {
    this.driver = driver;
    this.hopGui = hopGui;
  }

  /** The rows of the grid, each as its list of cell values. */
  public List<List<String>> rows() {
    @SuppressWarnings("unchecked")
    List<List<String>> rows =
        (List<List<String>>) ((JavascriptExecutor) driver).executeScript(GRID_ROWS);
    return rows;
  }

  /**
   * How many rows the dialog says it is showing.
   *
   * <p>Read from its own heading rather than counted off the grid: the grid is virtual and only
   * paints what fits on screen, so counting would report the size of the window.
   */
  public int rowCount() {
    Matcher matcher = ROW_COUNT.matcher(hopGui.topDialogText());
    if (!matcher.find()) {
      throw new AssertionError("The preview dialog does not say how many rows it has");
    }
    return Integer.parseInt(matcher.group(1));
  }

  public void close() {
    hopGui.clickButton("Close");
    hopGui.awaitNoDialog();
  }
}

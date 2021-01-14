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

package org.apache.hop.pipeline.transforms.filemetadata.util.delimiters;

import org.apache.hop.core.logging.ILogChannel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.ArrayList;

public class DelimiterDetector {

  private ArrayList<Character> delimiterCandidates;
  private ArrayList<Character> enclosureCandidates;
  private BufferedReader input;

  private long maxBadHeaderLines = 30;
  private long maxBadFooterLines = 30;

  private ArrayList<DetectionResult> potentialResults = new ArrayList<>(4);
  private ILogChannel log;
  private long rowLimit;

  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  public long getRowLimit() {
    return rowLimit;
  }


  class LineResult {
    long streak;
    long frequency;
    boolean consistentEnclosure;
    boolean enclosureSeen;
  }

  public class DetectionResult {

    private ArrayDeque<LineResult> lineResults = new ArrayDeque<>(8);

    private Character delimiter = null;
    private Character enclosure = null;

    private long badHeaders = 0;
    private long badFooters = 0;
    private long dataLines = 0;
    private long dataLineFrequency = 0;
    private boolean consistentEnclosure = true;
    private boolean enclosureSeen = false;

    void addLineResult(LineResult lineResult){
      // first line result?
      if (lineResults.isEmpty()){
        lineResults.add(lineResult);
        return;
      }

      // following up, may merge with previous one
      LineResult prev = lineResults.peekLast();
      if (prev.frequency == lineResult.frequency && (enclosure == null || (prev.consistentEnclosure && lineResult.consistentEnclosure))){
        prev.streak += 1;
        if (!prev.enclosureSeen){
          prev.enclosureSeen = lineResult.enclosureSeen;
        }

      }
      else{
        lineResults.add(lineResult);
      }
    }


    public void setDelimiter(Character delimiter) {
      this.delimiter = delimiter;
    }

    public void setEnclosure(Character enclosure) {
      this.enclosure = enclosure;
    }

    int getStreaks(){
      return lineResults.size();
    }

    void evaluate(){

      if (lineResults.isEmpty()){
        return;
      }

      LineResult[] streaks = new LineResult[lineResults.size()];
      lineResults.toArray(streaks);

      long currentMaxStreak = streaks[0].streak;
      badHeaders = 0;
      dataLines = streaks[0].streak;
      dataLineFrequency = streaks[0].frequency;
      badFooters = 0;
      consistentEnclosure = streaks[0].consistentEnclosure;
      enclosureSeen = streaks[0].enclosureSeen;
      int size = streaks.length;

      // note: skipping first item
      for(int i=1;i<size;i++){
        LineResult streak = streaks[i];
        if (!enclosureSeen){
          enclosureSeen = streak.enclosureSeen;
        }
        if (streak.streak >= currentMaxStreak && streak.consistentEnclosure){
          badHeaders += dataLines+badFooters;
          badFooters = 0;
          dataLines = streak.streak;
          dataLineFrequency = streak.frequency;
          currentMaxStreak = streak.streak;
        }
        else{
          badFooters += streak.streak;
        }
      }

    }

    public Character getDelimiter() {
      return delimiter;
    }

    public Character getEnclosure() {
      return enclosure;
    }

    public long getBadHeaders() {
      return badHeaders;
    }

    public long getBadFooters() {
      return badFooters;
    }

    public long getDataLines() {
      return dataLines;
    }

    public long getDataLineFrequency() {
      return dataLineFrequency;
    }

    public boolean isConsistentEnclosure(){
      return consistentEnclosure;
    }

    public boolean isEnclosureSeen(){
      return enclosureSeen;
    }

    public boolean hasEnclosure() {
      return getEnclosure() != null;
    }
  }

  public ArrayList<Character> getDelimiterCandidates() {
    return delimiterCandidates;
  }

  void setDelimiterCandidates(ArrayList<Character> delimiterCandidates) {
    this.delimiterCandidates = delimiterCandidates;
  }

  public ArrayList<Character> getEnclosureCandidates() {
    return enclosureCandidates;
  }

  void setEnclosureCandidates(ArrayList<Character> enclosureCandidates) {
    this.enclosureCandidates = enclosureCandidates;
  }

  Reader getInput() {
    return input;
  }

  void setInput(BufferedReader input) {
    this.input = input;
  }

  public long getMaxBadHeaderLines() {
    return maxBadHeaderLines;
  }

  void setMaxBadHeaderLines(long maxBadHeaderLines) {
    this.maxBadHeaderLines = maxBadHeaderLines;
  }

  public long getMaxBadFooterLines() {
    return maxBadFooterLines;
  }

  void setMaxBadFooterLines(long maxBadFooterLines) {
    this.maxBadFooterLines = maxBadFooterLines;
  }

  public DetectionResult detectDelimiters() throws IOException {

    // potential configuration candidates with enclosure
    for (Character delimiterCandidate : delimiterCandidates) {
      for (Character enclosureCandidate : enclosureCandidates) {
        DetectionResult detectionResult = new DetectionResult();
        detectionResult.setDelimiter(delimiterCandidate);
        detectionResult.setEnclosure(enclosureCandidate);
        potentialResults.add(detectionResult);
      }
      // no enclosure variant, lower priority
      DetectionResult detectionResult = new DetectionResult();
      detectionResult.setDelimiter(delimiterCandidate);
      detectionResult.setEnclosure(null);
      potentialResults.add(detectionResult);
    }

    // scan the file

    long lineNr = 0;

    // keep track of delimiter frequencies for each result
    int[] frequencies = new int[potentialResults.size()];
    boolean[] enclosureOpen = new boolean[potentialResults.size()];
    boolean[] enclosureSeen = new boolean[potentialResults.size()];
    boolean[] enclosureConsistent = new boolean[potentialResults.size()];

    String s = "";
    try {

      while ((rowLimit <= 0 || lineNr <= rowLimit) && (s = input.readLine()) != null) {
        lineNr++;

        int remainingResults = potentialResults.size();

        // clear occurrences for each char
        for (int j = 0; j < remainingResults; j++) {
          frequencies[j] = 0;
          enclosureOpen[j] = false;
          enclosureConsistent[j] = true;
        }

        // find occurrences for each char
        for (int i = 0; i < s.length(); i++) {
          char sc = s.charAt(i);
          for (int j = 0; j < remainingResults; j++) {
            DetectionResult detectionResult = potentialResults.get(j);

            char c = detectionResult.getDelimiter();
            Character enc = detectionResult.getEnclosure();
            boolean hasEnclosure = enc != null;

            // if enclosure is involved, ignore enclosed delimiters
            if (hasEnclosure) {

              if (!enclosureOpen[j] && sc == c) {
                frequencies[j] += 1;
              }

              if (sc == enc) {
                enclosureSeen[j] = true;
                enclosureConsistent[j] = enclosureConsistent[j] && (i == 0 && !enclosureOpen[j] ||
                    i == s.length() - 1 && enclosureOpen[j] ||
                    i > 0 && s.charAt(i - 1) == c && !enclosureOpen[j] ||
                    i > 0 && s.charAt(i - 1) == enc && !enclosureOpen[j] ||
                    s.length() > i + 1 && s.charAt(i + 1) == c && enclosureOpen[j] ||
                    s.length() > i + 1 && s.charAt(i + 1) == enc && enclosureOpen[j]

                );
                enclosureOpen[j] = !enclosureOpen[j];


              }

            }
            // no enclosure logic, just delimiters
            else {

              if (sc == c) {
                frequencies[j] += 1;
              }

            }
          }
        }

        // add the frequency counts to the results
        for (int j = 0; j < remainingResults; j++) {
          LineResult result = new LineResult();
          result.streak = 1;
          result.frequency = frequencies[j];
          result.consistentEnclosure = enclosureConsistent[j] && !enclosureOpen[j];
          result.enclosureSeen = enclosureSeen[j];
          DetectionResult d = potentialResults.get(j);
          d.addLineResult(result);
        }

        // can any results be eliminated now?
        for (int j = 0; j < potentialResults.size(); ) {
          DetectionResult d = potentialResults.get(j);

          if (isPlausible(d)) {
            // check the next one
            j++;
          } else {
            // remove it, recheck index
            potentialResults.remove(j);
          }

        }
      }

      // final evaluation
      for (int j = 0; j < potentialResults.size(); ) {
        DetectionResult d = potentialResults.get(j);
        d.evaluate();
        if (qualifies(d)) {
          j++;
        } else {
          potentialResults.remove(j);
        }
      }

      if (potentialResults.isEmpty()) {
        if(log != null)
          log.logError("All possible configurations dismissed. Inconsistent fields?");
        return null;
      }

      return potentialResults.get(0);
    }
    catch(ArrayIndexOutOfBoundsException ex1){
      if (log != null){
        log.logError("Inconsistent separators on line "+lineNr+". Line breaks in fields?");
        if(s != null){
          log.logError("offending line: "+s);
        }

      }
      ex1.printStackTrace();
      return null;
    }
    catch(IOException ex2){
      if (log != null){
        log.logError("Error reading around line "+lineNr+". Invalid charset?");
        if(s != null){
          log.logError("offending line: "+s);
        }
      }
      ex2.printStackTrace();
      return null;
    }


  }

  private boolean qualifies(DetectionResult d) {
    return d.getDataLineFrequency() > 0 &&
               d.getBadFooters() <= maxBadFooterLines &&
               d.getBadHeaders() <= maxBadHeaderLines &&
               (!d.hasEnclosure() || d.isConsistentEnclosure() && d.isEnclosureSeen());
  }

  // called before evaluation
  // used to cull results that have too many streaks to
  // plausibly qualify
  private boolean isPlausible(DetectionResult d) {
    return d.getStreaks() < maxBadFooterLines + maxBadHeaderLines;
  }

  public void setLog(ILogChannel log) {
    this.log = log;
  }

  public ILogChannel getLog() {
    return log;
  }


}

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
import java.util.ArrayList;
import java.util.List;

public class DelimiterDetectorBuilder {

  private ArrayList<Character> delimiterCandidates = new ArrayList<>(5);
  private ArrayList<Character> enclosureCandidates = new ArrayList<>(5);
  private BufferedReader input = null;
  private ILogChannel log;

  private long maxBadHeaderLines = 10;
  private long maxBadFooterLines = 10;

  private long rowLimit = 0;

  public DelimiterDetectorBuilder() {
  }

  public DelimiterDetectorBuilder withDelimiterCandidates(char ... candidates){
    delimiterCandidates.clear();
    for (char c : candidates) {
      delimiterCandidates.add(c);
    }
    return this;
  }

  public DelimiterDetectorBuilder withDelimiterCandidates(List<Character> candidates){
    delimiterCandidates.clear();
    for (char c : candidates) {
      delimiterCandidates.add(c);
    }
    return this;
  }


  public DelimiterDetectorBuilder withEnclosureCandidates(char ... candidates){
    enclosureCandidates.clear();
    for (char c : candidates) {
      enclosureCandidates.add(c);
    }
    return this;
  }

  public DelimiterDetectorBuilder withEnclosureCandidates(List<Character> candidates){
    enclosureCandidates.clear();
    for (char c : candidates) {
      enclosureCandidates.add(c);
    }
    return this;
  }

  public DelimiterDetectorBuilder withInput(BufferedReader input){
    this.input = input;
    return this;
  }

  public DelimiterDetectorBuilder withLogger(ILogChannel log){
    this.log = log;
    return this;
  }

  public DelimiterDetectorBuilder withMaxBadLines(long header, long footer){
    maxBadHeaderLines = header;
    maxBadFooterLines = footer;
    return this;
  }

  public DelimiterDetector build(){
    DelimiterDetector d = new DelimiterDetector();
    d.setDelimiterCandidates(delimiterCandidates);
    d.setEnclosureCandidates(enclosureCandidates);
    d.setInput(input);
    d.setMaxBadHeaderLines(maxBadHeaderLines);
    d.setMaxBadFooterLines(maxBadFooterLines);
    d.setLog(log);
    d.setRowLimit(rowLimit);
    return d;
  }


  public DelimiterDetectorBuilder withRowLimit(long limitRows) {
    rowLimit = limitRows;
    return this;
  }
}

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

package org.apache.hop.pipeline.debug;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.row.IRowMeta;

/** A debug session in memory: the description written to session.json, plus each hop's rows. */
@Getter
@Setter
public class DebugSession {

  private int formatVersion = DebugSessionStore.FORMAT_VERSION;
  private String pipelineFilename;
  private String pipelineName;
  private String project;
  private String environment;
  private String pipelineHash;
  private String selectedTransform;
  private int processCount = 1;
  private long savedAt;
  private final List<HopState> hops = new ArrayList<>();

  /** One hop buffer. Rows are the committed queue, including rows already behind the cursor. */
  @Getter
  @Setter
  public static class HopState {
    private String origin;
    private int originCopy;
    private String destination;
    private int destinationCopy;
    private int cursor;
    private IRowMeta rowMeta;
    private final List<Object[]> rows = new ArrayList<>();
  }
}

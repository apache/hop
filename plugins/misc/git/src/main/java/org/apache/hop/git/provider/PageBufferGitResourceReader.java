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

package org.apache.hop.git.provider;

import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hop.core.exception.HopException;

class PageBufferGitResourceReader implements GitResourceReader {

  private final int maxRecords;
  private final GitResourcePageLoader loader;
  private List<GitResourceRecord> buffer = List.of();
  private int bufferIndex;
  private int count;
  private boolean exhausted;
  private boolean rowCapReached;

  PageBufferGitResourceReader(int maxRecords, GitResourcePageLoader loader) {
    this.maxRecords = maxRecords;
    this.loader = loader;
  }

  @Override
  public boolean hasNext() throws HopException {
    if (count >= maxRecords) {
      rowCapReached = maxRecords != GitListOptions.UNLIMITED_MAX_RECORDS;
      return false;
    }
    if (exhausted) {
      return false;
    }
    if (bufferIndex < buffer.size()) {
      return true;
    }
    // A page that maps to no rows is not the end of the data: keep asking until the loader says
    // it is finished. Without this a page of issues that are all pull requests ends the read.
    do {
      buffer = loader.loadNextPage();
      bufferIndex = 0;
    } while (buffer.isEmpty() && !loader.isExhausted());

    if (buffer.isEmpty()) {
      exhausted = true;
      return false;
    }
    return true;
  }

  @Override
  public GitResourceRecord next() throws HopException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    GitResourceRecord record = buffer.get(bufferIndex++);
    count++;
    return record;
  }

  @Override
  public int count() {
    return count;
  }

  @Override
  public void close() {
    buffer = List.of();
    bufferIndex = 0;
    loader.close();
  }

  @Override
  public String getTruncationNote() {
    if (rowCapReached) {
      return "the configured row cap of "
          + maxRecords
          + " was reached; raise page size or max pages to read more";
    }
    return loader.getTruncationNote();
  }
}

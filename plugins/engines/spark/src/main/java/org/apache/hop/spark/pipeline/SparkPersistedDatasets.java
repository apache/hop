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
package org.apache.hop.spark.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;

/**
 * The Datasets one run keeps, so that the transforms before them run once. A run releases all of
 * them when it ends: Hop GUI and Hop Server use one Spark session for many runs, and without the
 * release every run would leave a cached copy of its rows in that session.
 *
 * <p>The converter and the transform handlers persist through this class only, never with {@link
 * Dataset#persist} directly.
 */
public class SparkPersistedDatasets {

  private final List<Dataset<Row>> datasets = new ArrayList<>();

  /**
   * Keeps the rows in memory, and on disk when they don't fit, until {@link #unpersistAll()}.
   *
   * @return the Dataset, persisted
   */
  public synchronized Dataset<Row> persist(Dataset<Row> dataset) {
    for (Dataset<Row> kept : datasets) {
      if (kept == dataset) {
        // Kept already: persisting it again only makes Spark warn
        return dataset;
      }
    }
    dataset.persist(StorageLevel.MEMORY_AND_DISK());
    datasets.add(dataset);
    return dataset;
  }

  /** Releases every Dataset kept since the last release. */
  public synchronized void unpersistAll() {
    for (Dataset<Row> dataset : datasets) {
      try {
        dataset.unpersist();
      } catch (Exception e) {
        // The session may already be gone
      }
    }
    datasets.clear();
  }
}

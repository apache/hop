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

package org.apache.hop.pipeline.engines.local.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointMap;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.pipeline.transform.BaseTransform;

/**
 * Runs transforms the way a transactional pipeline does, for tests that drive transforms directly:
 * every transform's database joins one connection group, so they share one connection, and each
 * transform runs on a thread of its own at the same time as the others. At the end the transaction
 * is committed, or rolled back when a transform failed, as {@code LocalPipelineEngine} does.
 *
 * <pre>{@code
 * SharedConnectionRun run = new SharedConnectionRun();
 * run.join(updateA);
 * run.join(updateB);
 * run.run(); // init, processRow until done, dispose, commit
 * }</pre>
 */
public class SharedConnectionRun {

  private final String group = "test-transaction-" + UUID.randomUUID();
  private final List<BaseTransform<?, ?>> transforms = new ArrayList<>();
  private long timeoutSeconds = 300;

  /**
   * Registers the extension point that puts a transform's database in its pipeline's connection
   * group. Call once before creating databases; HopClientEnvironment doesn't scan for it.
   */
  public static void registerConnectionGroupExtensionPoint() throws HopException {
    ExtensionPointPluginType.getInstance().registerClassPathPlugin(DatabaseCreatedXp.class);
    ExtensionPointMap.getInstance().reInitialize();
  }

  /** The connection group, as the pipeline would name it. */
  public String getGroup() {
    return group;
  }

  public SharedConnectionRun timeoutSeconds(long timeoutSeconds) {
    this.timeoutSeconds = timeoutSeconds;
    return this;
  }

  /** Adds a transform to the transaction, before its {@code init()}. */
  public <T extends BaseTransform<?, ?>> T join(T transform) {
    transform.getExtensionDataMap().put(Const.CONNECTION_GROUP, group);
    transforms.add(transform);
    return transform;
  }

  /**
   * Initializes the transforms, runs them at the same time until each has no more rows, disposes
   * them, then ends the transaction.
   *
   * @return true if the transaction was committed, false if a transform failed and it was rolled
   *     back
   * @throws Exception when a transform doesn't initialize, throws, or doesn't finish in time
   */
  public boolean run() throws Exception {
    for (BaseTransform<?, ?> transform : transforms) {
      if (!transform.init()) {
        throw new IllegalStateException("Transform " + transform.getTransformName() + " init");
      }
    }
    ExecutorService threads = Executors.newFixedThreadPool(transforms.size());
    List<CompletableFuture<Void>> running = new ArrayList<>();
    try {
      for (BaseTransform<?, ?> transform : transforms) {
        running.add(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    while (transform.processRow()) {
                      // keep going
                    }
                  } catch (HopException e) {
                    throw new IllegalStateException(
                        "Transform " + transform.getTransformName() + " failed", e);
                  }
                },
                threads));
      }
      CompletableFuture.allOf(running.toArray(new CompletableFuture[0]))
          .get(timeoutSeconds, TimeUnit.SECONDS);
    } finally {
      threads.shutdownNow();
      for (BaseTransform<?, ?> transform : transforms) {
        transform.dispose();
      }
    }
    return endTransaction();
  }

  private boolean endTransaction() throws HopException {
    long errors = 0;
    for (BaseTransform<?, ?> transform : transforms) {
      errors += transform.getErrors();
    }
    List<Database> databases = DatabaseConnectionMap.getInstance().getDatabases(group);
    for (Database database : databases) {
      try {
        if (errors == 0) {
          database.commit(true);
        } else {
          database.rollback(true);
        }
      } finally {
        database.closeConnectionOnly();
      }
    }
    return errors == 0;
  }
}

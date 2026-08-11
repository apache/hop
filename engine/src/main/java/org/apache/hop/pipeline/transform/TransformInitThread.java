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

package org.apache.hop.pipeline.transform;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.Metrics;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;

public class TransformInitThread implements Runnable {
  private static final Class<?> PKG = Pipeline.class;

  @Getter public boolean ok;
  @Getter public boolean finished;

  @Setter @Getter public boolean doIt;

  @Setter @Getter private TransformMetaDataCombi combi;

  @Setter @Getter private Pipeline pipeline;

  private ILogChannel log;

  public TransformInitThread(TransformMetaDataCombi combi, Pipeline pipeline, ILogChannel log) {
    this.combi = combi;
    this.pipeline = pipeline;
    this.log = combi.transform.getLogChannel();
    this.ok = false;
    this.finished = false;
    this.doIt = true;
  }

  public String toString() {
    return combi.transformName;
  }

  @Override
  public void run() {
    // Set the internal variables also on the initialization thread!

    if (!doIt) {
      // An extension point plugin decided we should not initialize the transform.
      // Logging, error handling, finished flag... should all be handled in the extension point.
      //
      return;
    }

    // Already marked as failed (e.g. invalid number of copies) - skip init, same outcome as init
    // failure
    if (combi.data.getStatus() == ComponentExecutionStatus.STATUS_STOPPED) {
      finished = true;
      ok = false;
      return;
    }

    try {
      combi.transform.getLogChannel().snap(Metrics.METRIC_TRANSFORM_INIT_START);

      if (combi.transform.init()) {
        combi.data.setStatus(ComponentExecutionStatus.STATUS_IDLE);
        ok = true;
      } else {
        combi.transform.setErrors(1);
      }
    } catch (Throwable e) {
      log.logError(
          BaseMessages.getString(
              PKG, "Pipeline.Log.ErrorInitializingTransform", combi.transform.getTransformName()));
      log.logError(Const.getStackTracker(e));
    } finally {
      combi.transform.getLogChannel().snap(Metrics.METRIC_TRANSFORM_INIT_STOP);
    }

    finished = true;
  }
}

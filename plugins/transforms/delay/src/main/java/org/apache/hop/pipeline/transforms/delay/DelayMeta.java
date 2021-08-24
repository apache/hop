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

package org.apache.hop.pipeline.transforms.delay;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

@Transform(
    id = "Delay",
    image = "delay.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Delay",
    description = "i18n::BaseTransform.TypeTooltipDesc.Delay",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/delay.html")
public class DelayMeta extends BaseTransformMeta implements ITransformMeta<Delay, DelayData> {
  private static final Class<?> PKG = DelayMeta.class; // For Translator

  @HopMetadataProperty(key = "timeout", injectionKeyDescription = "Delay.Injection.Timeout")
  private String timeout;

  @HopMetadataProperty(key = "scaletime", injectionKeyDescription = "Delay.Injection.Scaletime")
  private String scaletime;

  private static final String DEFAULT_SCALE_TIME = "seconds";

  private static final String[] SCALE_TIME_CODE = {"milliseconds", "seconds", "minutes", "hours"};

  public DelayMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public String getScaletime() {
    return scaletime;
  }

  public void setScaletime(String scaletime) {
    this.scaletime = scaletime;
  }

  public String getTimeout() {
    return timeout;
  }

  public void setTimeout(String timeout) {
    this.timeout = timeout;
  }

  public void setScaleTimeCode(int scaleTimeIndex) {
    switch (scaleTimeIndex) {
      case 0:
        scaletime = SCALE_TIME_CODE[0]; // milliseconds
        break;
      case 1:
        scaletime = SCALE_TIME_CODE[1]; // second
        break;
      case 2:
        scaletime = SCALE_TIME_CODE[2]; // minutes
        break;
      case 3:
        scaletime = SCALE_TIME_CODE[3]; // hours
        break;
      default:
        scaletime = SCALE_TIME_CODE[1]; // seconds
        break;
    }
  }

  public int getScaleTimeCode() {
    int retval = 1; // DEFAULT: seconds
    if (scaletime == null) {
      return retval;
    }
    if (scaletime.equals(SCALE_TIME_CODE[0])) {
      retval = 0;
    } else if (scaletime.equals(SCALE_TIME_CODE[1])) {
      retval = 1;
    } else if (scaletime.equals(SCALE_TIME_CODE[2])) {
      retval = 2;
    } else if (scaletime.equals(SCALE_TIME_CODE[3])) {
      retval = 3;
    }

    return retval;
  }

  @Override
  public void setDefault() {
    timeout = "1"; // default one second
    scaletime = DEFAULT_SCALE_TIME; // defaults to "seconds"
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Not needed for this Transform
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    String errorMessage = "";

    if (Utils.isEmpty(timeout)) {
      errorMessage = BaseMessages.getString(PKG, "DelayMeta.CheckResult.TimeOutMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
    } else {
      errorMessage = BaseMessages.getString(PKG, "DelayMeta.CheckResult.TimeOutOk");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
    }
    remarks.add(cr);

    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "DelayMeta.CheckResult.NotReceivingFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "DelayMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "DelayMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "DelayMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);
  }

  public Delay createTransform(
      TransformMeta transformMeta, DelayData data, int cnr, PipelineMeta tr, Pipeline pipeline) {
    return new Delay(transformMeta, this, data, cnr, tr, pipeline);
  }

  public DelayData getTransformData() {
    return new DelayData();
  }
}

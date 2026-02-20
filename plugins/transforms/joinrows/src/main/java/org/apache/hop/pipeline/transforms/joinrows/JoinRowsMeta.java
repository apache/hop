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

package org.apache.hop.pipeline.transforms.joinrows;

import java.io.File;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Condition;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IStringObjectConverter;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "JoinRows",
    image = "joinrows.svg",
    name = "i18n::BaseTransform.TypeLongDesc.JoinRows",
    description = "i18n::BaseTransform.TypeTooltipDesc.JoinRows",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    keywords = "i18n::JoinRowsMeta.keyword",
    documentationUrl = "/pipeline/transforms/joinrows.html")
@Getter
@Setter
public class JoinRowsMeta extends BaseTransformMeta<JoinRows, JoinRowsData> {
  private static final Class<?> PKG = JoinRowsMeta.class;

  @HopMetadataProperty(
      key = "directory",
      injectionKey = "TEMP_DIR",
      injectionKeyDescription = "JoinRows.Injection.TEMP_DIR")
  private String directory;

  @HopMetadataProperty(
      key = "prefix",
      injectionKey = "TEMP_FILE_PREFIX",
      injectionKeyDescription = "JoinRows.Injection.TEMP_FILE_PREFIX")
  private String prefix;

  @HopMetadataProperty(
      key = "cache_size",
      injectionKey = "MAX_CACHE_SIZE",
      injectionKeyDescription = "JoinRows.Injection.MAX_CACHE_SIZE")
  private int cacheSize;

  /** Which transform is providing the lookup data? */
  private TransformMeta mainTransform;

  /** Which transform is providing the lookup data? */
  @HopMetadataProperty(
      key = "main",
      injectionKey = "MAIN_TRANSFORM",
      injectionKeyDescription = "JoinRows.Injection.MAIN_TRANSFORM")
  private String mainTransformName;

  /** Optional condition to limit the join (where clause) */
  @HopMetadataProperty(
      key = "compare",
      injectionKey = "CONDITION",
      injectionKeyDescription = "JoinRows.Injection.CONDITION",
      injectionStringObjectConverter = ConditionXmlConverter.class)
  private JRCompare compare;

  public JoinRowsMeta() {
    super(); // allocate BaseTransformMeta
    compare = new JRCompare();
  }

  @Override
  public void setDefault() {
    directory = "%%java.io.tmpdir%%";
    prefix = "out";
    cacheSize = 500;
    mainTransformName = null;
  }

  @Override
  public void getFields(
      PipelineMeta pipelineMeta,
      IRowMeta rowMeta,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    TransformMeta[] transforms = pipelineMeta.getPrevTransforms(pipelineMeta.findTransform(origin));
    TransformMeta firstTransform = pipelineMeta.findTransform(getMainTransformName());
    rowMeta.clear();
    if (firstTransform != null) {
      rowMeta.addRowMeta(pipelineMeta.getTransformFields(variables, firstTransform));
    }
    for (TransformMeta transform : transforms) {
      if (!transform.equals(firstTransform)) {
        IRowMeta transformFields = pipelineMeta.getTransformFields(variables, transform);
        rowMeta.addRowMeta(transformFields);
      }
    }
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

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JoinRowsMeta.CheckResult.TransformReceivingDatas", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      // Check the sort directory
      String realDirectory = variables.resolve(directory);
      File f = new File(realDirectory);
      if (f.exists()) {
        if (f.isDirectory()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  "["
                      + realDirectory
                      + BaseMessages.getString(PKG, "JoinRowsMeta.CheckResult.DirectoryExists"),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  "["
                      + realDirectory
                      + BaseMessages.getString(
                          PKG, "JoinRowsMeta.CheckResult.DirectoryExistsButNotValid"),
                  transformMeta);
          remarks.add(cr);
        }
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "JoinRowsMeta.CheckResult.DirectoryDoesNotExist", realDirectory),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "JoinRowsMeta.CheckResult.CouldNotFindFieldsFromPreviousTransforms"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "JoinRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "JoinRowsMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public String getLookupTransformName() {
    if (mainTransform != null
        && mainTransform.getName() != null
        && !mainTransform.getName().isEmpty()) {
      return mainTransform.getName();
    }
    return null;
  }

  /**
   * Returns the I/O meta with one INFO stream when "Main transform to read from" is set (like
   * Stream Lookup).
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, false, false, false, false);
      IStream stream =
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "JoinRowsMeta.InfoStream.Description"),
              StreamIcon.INFO,
              mainTransformName);
      ioMeta.addStream(stream);
      setTransformIOMeta(ioMeta);
    }
    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset
  }

  /**
   * @param transforms optionally search the info transform in a list of transforms
   */
  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    if (infoStreams.isEmpty()) {
      mainTransform = TransformMeta.findTransform(transforms, mainTransformName);
      return;
    }
    IStream stream = infoStreams.get(0);

    String[] prev = null;
    if (parentTransformMeta != null && parentTransformMeta.getParentPipelineMeta() != null) {
      prev = parentTransformMeta.getParentPipelineMeta().getPrevTransformNames(parentTransformMeta);
    }

    // Clear name when no longer in prev (transform removed)
    if (prev != null
        && mainTransformName != null
        && !ArrayUtils.contains(prev, mainTransformName)
        && (stream.getTransformMeta() == null
            || !ArrayUtils.contains(prev, stream.getTransformName()))) {
      mainTransformName = null;
      setChanged();
    }

    // Resolve: prefer stream when name is stale (rename / insert-in-the-middle / detach).
    // Do not re-fill from stream when mainTransformName is empty (user chose to clear / no main).
    boolean nameStale =
        Utils.isEmpty(mainTransformName)
            || (prev != null && !ArrayUtils.contains(prev, mainTransformName))
            || TransformMeta.findTransform(transforms, mainTransformName) == null;
    boolean preferStream =
        stream.getTransformMeta() != null
            && prev != null
            && ArrayUtils.contains(prev, stream.getTransformName())
            && nameStale
            && !Utils.isEmpty(mainTransformName);

    TransformMeta tm = null;
    if (preferStream) {
      mainTransformName = stream.getTransformName();
      mainTransform = stream.getTransformMeta();
      tm = mainTransform;
      setChanged();
    }
    if (tm == null) {
      tm = TransformMeta.findTransform(transforms, mainTransformName);
      if (tm == null && !Utils.isEmpty(mainTransformName) && stream.getTransformMeta() != null) {
        mainTransformName = stream.getTransformName();
        tm = TransformMeta.findTransform(transforms, mainTransformName);
        setChanged();
      }
      mainTransform = tm;
    }
    stream.setTransformMeta(tm);
    if (tm != null) {
      stream.setSubject(tm.getName());
    } else {
      stream.setSubject(null);
    }
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public boolean cleanAfterHopToRemove(TransformMeta fromTransform) {
    boolean hasChanged = false;

    // If the hop we're removing comes from a Transform that is being used as the main transform for
    // the Join, we have to clear
    // that reference
    if (null != fromTransform && fromTransform.equals(getMainTransform())) {
      setMainTransform(null);
      setMainTransformName(null);
      hasChanged = true;
    }

    return hasChanged;
  }

  public static final class ConditionXmlConverter implements IStringObjectConverter {
    @Override
    public String getString(Object object) throws HopException {
      if (!(object instanceof JRCompare)) {
        throw new HopException("We only support XML serialization of Condition objects here");
      }
      try {
        return ((JRCompare) object).getCondition().getXml();
      } catch (Exception e) {
        throw new HopException("Error serializing Condition to XML", e);
      }
    }

    @Override
    public Object getObject(String xml) throws HopException {
      try {
        return new JRCompare(new Condition(xml));
      } catch (Exception e) {
        throw new HopException("Error serializing Condition from XML", e);
      }
    }
  }

  @Getter
  @Setter
  public static final class JRCompare {
    @HopMetadataProperty(key = "condition")
    private Condition condition;

    public JRCompare() {
      condition = new Condition();
    }

    public JRCompare(Condition condition) {
      this.condition = condition;
    }
  }

  /**
   * @return Returns the condition.
   */
  public Condition getCondition() {
    return compare.condition;
  }

  /**
   * @param condition The condition to set.
   */
  public void setCondition(Condition condition) {
    this.compare.condition = condition;
  }
}

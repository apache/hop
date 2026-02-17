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

package org.apache.hop.pipeline.transforms.multimerge;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.w3c.dom.Node;

@Transform(
    id = "MultiwayMergeJoin",
    image = "multimergejoin.svg",
    name = "i18n::MultiwayMergeJoin.Name",
    description = "i18n::MultiwayMergeJoin.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Joins",
    keywords = "i18n::MultiMergeJoinMeta.keyword",
    documentationUrl = "/pipeline/transforms/multimerge.html")
@Getter
@Setter
public class MultiMergeJoinMeta extends BaseTransformMeta<MultiMergeJoin, MultiMergeJoinData> {
  private static final Class<?> PKG = MultiMergeJoinMeta.class;

  public static final String[] joinTypes = {"INNER", "FULL OUTER"};
  public static final boolean[] optionals = {false, true};

  @HopMetadataProperty(
      key = "join_type",
      injectionKey = "JOIN_TYPE",
      injectionKeyDescription = "MultiMergeJoinMeta.Injection.JoinType")
  private String joinType;

  /** comma separated key values for each stream */
  @HopMetadataProperty(key = "key", groupKey = "keys", injectionKey = "KEY_FIELDS")
  private List<String> keyFields;

  /** input stream names */
  @HopMetadataProperty(
      key = "transform",
      groupKey = "transforms",
      injectionKey = "INPUT_TRANSFORMS")
  private List<String> inputTransforms;

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  public MultiMergeJoinMeta() {
    super();
    this.keyFields = new ArrayList<>();
    this.inputTransforms = new ArrayList<>();
  }

  /**
   * keep loadXml to load old style xml information for the transform
   *
   * @deprecated
   * @param transformNode the XML node from the pipeline
   * @param metadataProvider metadata provider to resolve things
   * @throws HopXmlException when we can't read the XML correctly
   */
  @Override
  @Deprecated(since = "2.17")
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    super.loadXml(transformNode, metadataProvider);
    // keep for backwards compatibility
    readData(transformNode);
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      String numInputStreamsStr = XmlHandler.getTagValue(transformNode, "number_input");

      // Skip if number_input doesn't exist (null or empty)
      if (numInputStreamsStr == null || numInputStreamsStr.trim().isEmpty()) {
        return;
      }

      int nInputStreams = Integer.parseInt(numInputStreamsStr);
      for (int i = 0; i < nInputStreams; i++) {
        inputTransforms.add(XmlHandler.getTagValue(transformNode, "transform" + i));
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "MultiMergeJoinMeta.Exception.UnableToLoadTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    joinType = joinTypes[0];
  }

  /**
   * Returns the I/O meta with INFO streams so the pipeline marks input hops as info streams (like
   * Merge Join / Append).
   */
  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, false, false, false, false);
      int n = (inputTransforms != null && !inputTransforms.isEmpty()) ? inputTransforms.size() : 2;
      for (int i = 0; i < n; i++) {
        ioMeta.addStream(
            new Stream(
                StreamType.INFO,
                null,
                BaseMessages.getString(PKG, "MultiMergeJoin.InfoStream.Description"),
                StreamIcon.INFO,
                null));
      }
      setTransformIOMeta(ioMeta);
    }
    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    ITransformIOMeta ioMeta = getTransformIOMeta();
    List<IStream> infoStreams = ioMeta.getInfoStreams();

    String[] prev = null;
    if (parentTransformMeta != null && parentTransformMeta.getParentPipelineMeta() != null) {
      prev = parentTransformMeta.getParentPipelineMeta().getPrevTransformNames(parentTransformMeta);
    }

    // Auto-fill when empty and we have connected transforms
    if ((inputTransforms == null || inputTransforms.isEmpty())
        && prev != null
        && prev.length >= 2) {
      inputTransforms = new ArrayList<>();
      for (String p : prev) {
        inputTransforms.add(p);
      }
      setChanged();
    }
    if (inputTransforms == null) {
      inputTransforms = new ArrayList<>();
    }

    // Clear names that no longer exist in prev; keep and update name when it's a rename (stream's
    // transform is in prev)
    if (prev != null) {
      List<String> newInputTransforms = new ArrayList<>();
      List<String> newKeyFields = (keyFields != null) ? new ArrayList<>() : null;
      for (int i = 0; i < inputTransforms.size(); i++) {
        String name = inputTransforms.get(i);
        if (Utils.isEmpty(name) || ArrayUtils.contains(prev, name)) {
          newInputTransforms.add(name);
          if (newKeyFields != null && i < keyFields.size()) {
            newKeyFields.add(keyFields.get(i));
          }
        } else if (i < infoStreams.size()) {
          IStream stream = infoStreams.get(i);
          if (stream.getTransformMeta() != null
              && ArrayUtils.contains(prev, stream.getTransformName())) {
            // Renamed: keep entry with updated name
            newInputTransforms.add(stream.getTransformName());
            if (newKeyFields != null && i < keyFields.size()) {
              newKeyFields.add(keyFields.get(i));
            }
            setChanged();
          }
        } else {
          setChanged();
        }
      }
      inputTransforms.clear();
      inputTransforms.addAll(newInputTransforms);
      if (keyFields != null) {
        keyFields.clear();
        keyFields.addAll(newKeyFields);
      }
    }

    // Resolve each slot and build the list of streams to set (getInfoStreams() returns a copy, so
    // we must replace via clearStreams + addStream)
    List<IStream> resolvedStreams = new ArrayList<>();
    String streamDescription = BaseMessages.getString(PKG, "MultiMergeJoin.InfoStream.Description");

    for (int i = 0; i < inputTransforms.size(); i++) {
      String name = inputTransforms.get(i);
      IStream existingStream = (i < infoStreams.size()) ? infoStreams.get(i) : null;

      boolean nameStale =
          Utils.isEmpty(name)
              || (prev != null && !ArrayUtils.contains(prev, name))
              || TransformMeta.findTransform(transforms, name) == null;
      boolean preferStream =
          existingStream != null
              && existingStream.getTransformMeta() != null
              && prev != null
              && ArrayUtils.contains(prev, existingStream.getTransformName())
              && nameStale;

      TransformMeta tm = null;
      if (preferStream) {
        name = existingStream.getTransformName();
        inputTransforms.set(i, name);
        tm = existingStream.getTransformMeta();
        setChanged();
      }
      if (tm == null) {
        tm = TransformMeta.findTransform(transforms, name);
        if (tm == null && existingStream != null && existingStream.getTransformMeta() != null) {
          name = existingStream.getTransformName();
          inputTransforms.set(i, name);
          tm = TransformMeta.findTransform(transforms, name);
        }
      }
      String subject = (tm != null) ? tm.getName() : null;
      resolvedStreams.add(
          new Stream(StreamType.INFO, tm, streamDescription, StreamIcon.INFO, subject));
    }

    // Sync keyFields size
    if (keyFields != null) {
      while (keyFields.size() > inputTransforms.size()) {
        keyFields.remove(keyFields.size() - 1);
      }
      while (keyFields.size() < inputTransforms.size()) {
        keyFields.add("");
      }
    }

    // Replace ioMeta streams so the pipeline sees INFO streams (like Merge Join / Append)
    if (ioMeta instanceof TransformIOMeta) {
      ((TransformIOMeta) ioMeta).clearStreams();
      for (IStream s : resolvedStreams) {
        ioMeta.addStream(s);
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
    /*
     * @todo Need to check for the following: 1) Join type must be one of INNER / LEFT OUTER / RIGHT OUTER / FULL OUTER
     * 2) Number of input streams must be two (for now at least) 3) The field names of input streams must be unique
     */
    CheckResult cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_WARNING,
            BaseMessages.getString(PKG, "MultiMergeJoinMeta.CheckResult.TransformNotVerified"),
            transformMeta);
    remarks.add(cr);
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just merge in the info fields.
    //
    if (info != null) {
      for (IRowMeta iRowMeta : info) {
        if (iRowMeta != null) {
          r.mergeRowMeta(iRowMeta);
        }
      }
    }

    for (int i = 0; i < r.size(); i++) {
      r.getValueMeta(i).setOrigin(name);
    }
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }

  @Override
  public boolean cleanAfterHopToRemove(TransformMeta fromTransform) {
    if (fromTransform == null || fromTransform.getName() == null) {
      return false;
    }

    if (inputTransforms == null || inputTransforms.isEmpty()) {
      return false;
    }

    String fromTransformName = fromTransform.getName();

    for (int i = 0; i < inputTransforms.size(); i++) {
      if (fromTransformName.equals(inputTransforms.get(i))) {
        inputTransforms.remove(i);
        if (keyFields != null && i < keyFields.size()) {
          keyFields.remove(i);
        }
        return true;
      }
    }

    return false;
  }
}

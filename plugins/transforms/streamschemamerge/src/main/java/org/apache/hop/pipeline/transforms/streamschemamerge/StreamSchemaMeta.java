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

package org.apache.hop.pipeline.transforms.streamschemamerge;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

@Transform(
    id = "StreamSchema",
    image = "streamschemamerge.svg",
    name = "i18n::StreamSchemaTransform.Name",
    description = "i18n::StreamSchemaTransform.TooltipDesc",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    keywords = "i18n::StreamSchemaMeta.keyword",
    documentationUrl = "/pipeline/transforms/streamschemamerge.html")
@Getter
@Setter
public class StreamSchemaMeta extends BaseTransformMeta<StreamSchema, StreamSchemaData> {
  private static final Class<?> PKG = StreamSchemaMeta.class;

  @Getter
  @Setter
  public static class TransformToMerge {
    @HopMetadataProperty(key = "name")
    private String name;

    public TransformToMerge() {}

    public TransformToMerge(TransformToMerge m) {
      this.name = m.name;
    }
  }

  /** Stores the names of the transforms to merge into the output */
  @HopMetadataProperty(key = "transform", groupKey = "transforms")
  private List<TransformToMerge> transformsToMerge;

  /**
   * Constructor should call super() to make sure the base class has a chance to initialize
   * properly.
   */
  public StreamSchemaMeta() {
    super();
    transformsToMerge = new ArrayList<>();
  }

  public StreamSchemaMeta(StreamSchemaMeta m) {
    this();
    m.transformsToMerge.forEach(t -> this.transformsToMerge.add(new TransformToMerge(t)));
  }

  /**
   * This method is used when a transform is duplicated in Spoon. It needs to return a deep copy of
   * this transform meta object. Be sure to create proper deep copies if the transform configuration
   * is stored in modifiable objects.
   *
   * <p>See RowGeneratorMeta.clone() for an example on creating a deep copy.
   *
   * @return a deep copy of this
   */
  @Override
  public StreamSchemaMeta clone() {
    return new StreamSchemaMeta(this);
  }

  /**
   * Prevents error box from popping up when sending in different row formats. Note you will still
   * get an error if you try to run the pipeline in safe mode.
   *
   * @return true
   */
  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  /**
   * This method is called to determine the changes the transform is making to the row-stream. To
   * that end a IRowMeta object is passed in, containing the row-stream structure as it is when
   * entering the transform. This method must apply any changes the transform makes to the row
   * stream. Usually a transform adds fields to the row-stream.
   *
   * @param inputRowMeta the row structure coming in to the transform
   * @param name the name of the transform making the changes
   * @param info row structures of any info transforms coming in
   * @param nextTransform the description of a transform this transform is passing rows to
   * @param variables the variable variables for resolving variables
   * @param metadataProvider the metadata provider to optionally read from
   */
  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    /*
     * We don't have any input fields so we ignore inputRowMeta
     */
    try {
      SchemaMapper schemaMapping =
          new SchemaMapper(info); // compute the union of the info fields being passed in
      IRowMeta base = schemaMapping.getRowMeta();

      for (int i = 0; i < base.size(); i++) {
        base.getValueMeta(i).setOrigin(name);
      }
      inputRowMeta.mergeRowMeta(base);
    } catch (HopPluginException e) {
      throw new HopTransformException("Hop plugin exception trying to resolve fields");
    }
  }

  /**
   * This method is called when the user selects the "Verify Transformation" option in Spoon. A list
   * of remarks is passed in that this method should add to. Each remark is a comment, warning,
   * error, or ok. The method should perform as many checks as necessary to catch design-time
   * errors.
   *
   * @param remarks the list of remarks to append to
   * @param pipelineMeta the description of the transformation
   * @param transformMeta the description of the transform
   * @param prev the structure of the incoming row-stream
   * @param input names of transforms sending input to the transform
   * @param output names of transforms this transform is sending output to
   * @param info fields coming in from info transforms
   * @param metadataProvider metadataProvider to optionally read from
   */
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

    // See if there are input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "StreamSchemaTransform.CheckResult.ReceivingRows.OK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "StreamSchemaTransform.CheckResult.ReceivingRows.ERROR"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    for (IStream stream : getTransformIOMeta().getInfoStreams()) {
      stream.setTransformMeta(
          TransformMeta.findTransform(transforms, (String) stream.getSubject()));
    }
  }

  @Override
  public void resetTransformIoMeta() {
    setTransformIOMeta(null);
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, false, false, false, false);

      for (TransformToMerge transformToMerge : transformsToMerge) {
        ioMeta.addStream(
            new Stream(
                IStream.StreamType.INFO,
                null,
                BaseMessages.getString(
                    PKG, "StreamSchemaMeta.InfoStream.Description", transformToMerge.name),
                StreamIcon.INFO,
                transformToMerge.name));
      }
      setTransformIOMeta(ioMeta);
    }

    return ioMeta;
  }
}

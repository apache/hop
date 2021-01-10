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

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Transform(
    id = "StreamSchema",
    image = "GenericTransform.svg",
    name = "i18n::StreamSchemaTransform.Name",
    description = "i18n::StreamSchemaTransform.TooltipDesc",
    categoryDescription = "i18n::StreamSchemaTransform.Category",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/streamschemamerge.html")
public class StreamSchemaMeta extends BaseTransformMeta
    implements ITransformMeta<StreamSchema, StreamSchemaData> {

  private static final Class<?> PKG = StreamSchemaMeta.class; // For Translator

  /** Stores the names of the transforms to merge into the output */
  private ArrayList<String> transformsToMerge = new ArrayList<>();

  /**
   * Constructor should call super() to make sure the base class has a chance to initialize
   * properly.
   */
  public StreamSchemaMeta() {
    super();
  }

  /**
   * Prevents error box from popping up when sending in different row formats. Note you will still
   * get an error if you try to run the pipeline in safe mode.
   *
   * @return true
   */
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  /**
   * This method is called every time a new transform is created and should allocate/set the transform
   * configuration to sensible defaults. The values set here will be used by Spoon when a new transform
   * is created.
   */
  public void setDefault() {
    // intentionally empty
  }

  /**
   * Getter for the fields that should be merged
   *
   * @return array of field names
   */
  public String[] getTransformsToMerge() {
    if (transformsToMerge == null) {
      return new String[0];
    } else {
      return transformsToMerge.toArray(new String[transformsToMerge.size()]);
    }
  }

  /**
   * Determine the number of transforms we're planning to merge
   *
   * @return number of items to merge, 0 if none
   */
  public int getNumberOfTransforms() {
    if (transformsToMerge == null) {
      return 0;
    } else {
      return transformsToMerge.size();
    }
  }

  /**
   * Set transforms to merge
   *
   * @param arrayOfTransforms Names of transforms to merge
   */
  public void setTransformsToMerge(String[] arrayOfTransforms) {
    transformsToMerge = new ArrayList<>();
    Collections.addAll(transformsToMerge, arrayOfTransforms);
  }

  /**
   * This method is used when a transform is duplicated in Spoon. It needs to return a deep copy of this
   * transform meta object. Be sure to create proper deep copies if the transform configuration is stored in
   * modifiable objects.
   *
   * <p>See org.pentaho.di.trans.transforms.rowgenerator.RowGeneratorMeta.clone() for an example on
   * creating a deep copy.
   *
   * @return a deep copy of this
   */
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  /**
   * This method is called by Hop Gui when a transform needs to serialize its configuration to XML.
   * The expected return value is an XML fragment consisting of one or more XML tags.
   *
   * <p>Please use XmlHandler to conveniently generate the XML.
   *
   * @return a string containing the XML serialization of this transform
   */
  public String getXml() throws HopValueException {
    StringBuilder xml = new StringBuilder();
    xml.append("    <transforms>" + Const.CR);
    for (String transformName : transformsToMerge) {
      xml.append("      <transform>" + Const.CR);
      xml.append("        " + XmlHandler.addTagValue("name", transformName));
      xml.append("        </transform>" + Const.CR);
    }
    xml.append("      </transforms>" + Const.CR);
    return xml.toString();
  }

  /**
   * This method is called by Hop when a transform needs to load its configuration from XML.
   *
   * <p>Please use XmlHandler to conveniently read from the XML node passed in.
   *
   * @param transformNode the XML node containing the configuration
   * @param metadataProvider the metadataProvider to optionally read from
   */
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    readData(transformNode);
  }

  /**
   * Helper methods to read in the XML
   *
   * @param transformNode XML node for the transform
   * @throws HopXmlException If there is an error reading the configuration
   */
  private void readData(Node transformNode) throws HopXmlException {
    try {
      // TODO put the strings in a config file or make constants in this file
      Node transforms = XmlHandler.getSubNode(transformNode, "transforms");
      int nrtransforms = XmlHandler.countNodes(transforms, "transform");

      transformsToMerge.clear();

      // we need to add a stream for each transform we want to merge to ensure it gets treated as an info
      // stream
      for (int i = 0; i < nrtransforms; i++) {
        getTransformIOMeta()
            .addStream(
                new Stream(
                    IStream.StreamType.INFO, null, "Streams to Merge", StreamIcon.INFO, null));
      }

      List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
      for (int i = 0; i < nrtransforms; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(transforms, "transform", i);
        String name = XmlHandler.getTagValue(fnode, "name");
        transformsToMerge.add(name);
        infoStreams.get(i).setSubject(name);
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
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
     * We don't have any input fields so we ingore inputRowMeta
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
      throw new HopTransformException("Kettle plugin exception trying to resolve fields");
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
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String input[],
      String output[],
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    CheckResult cr;

    // See if there are input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "StreamSchemaTransform.CheckResult.ReceivingRows.OK"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
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

  public void resetTransformIoMeta() {
    // Do nothing, don't reset as there is no need to do this.
  }

  /** Has original function of resetTransformIoMeta, but we only want to call it when appropriate */
  /*
  	public void wipeTransformIoMeta() {
  		ioMeta = null;
  	}
  */

  /**
   * Called by PDI to get a new instance of the transform implementation. A standard implementation
   * passing the arguments to the constructor of the transform class is recommended.
   *
   * @param transformMeta description of the transform
   * @param data instance of a transform data class
   * @param copyNr copy number
   * @param pipelineMeta description of the pipeline
   * @param pipeline runtime implementation of the pipeline
   * @return the new instance of a transform implementation
   */
  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      StreamSchemaData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new StreamSchema(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  /** Called by Hop to get a new instance of the transform data class. */
  @Override
  public StreamSchemaData getTransformData() {
    return new StreamSchemaData();
  }
}

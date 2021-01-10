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

package org.apache.hop.beam.transforms.window;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.w3c.dom.Node;

@Transform(
        id = "BeamWindow",
        name = "Beam Window",
        description = "Create a Beam Window",
        image = "beam-window.svg",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/beamwindow.html"
)
public class BeamWindowMeta extends BaseTransformMeta implements ITransformMeta<Dummy, DummyData> {

  public static final String WINDOW_TYPE = "window_type";
  public static final String DURATION = "duration";
  public static final String EVERY = "every";
  public static final String MAX_WINDOW_FIELD = "max_window_field";
  public static final String START_WINDOW_FIELD = "start_window_field";
  public static final String END_WINDOW_FIELD = "end_window_field";

  private String windowType;
  private String duration;
  private String every;

  private String maxWindowField;
  private String startWindowField;
  private String endWindowField;

  public BeamWindowMeta() {
    super();
  }

  @Override public void setDefault() {
    windowType = BeamDefaults.WINDOW_TYPE_FIXED;
    duration = "60";
    every = "";
    startWindowField = "startWindow";
    endWindowField = "endWindow";
    maxWindowField = "maxWindow";
  }

  @Override public Dummy createTransform( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Dummy( transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline );
  }

  @Override public DummyData getTransformData() {
    return new DummyData();
  }

  @Override public String getDialogClassName() {
    return BeamWindowDialog.class.getName();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {

    if ( StringUtils.isNotEmpty( startWindowField ) ) {
      ValueMetaDate valueMeta = new ValueMetaDate( variables.resolve( startWindowField ) );
      valueMeta.setOrigin( name );
      valueMeta.setConversionMask( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
      inputRowMeta.addValueMeta( valueMeta );
    }
    if ( StringUtils.isNotEmpty( endWindowField ) ) {
      ValueMetaDate valueMeta = new ValueMetaDate( variables.resolve( endWindowField ) );
      valueMeta.setOrigin( name );
      valueMeta.setConversionMask( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
      inputRowMeta.addValueMeta( valueMeta );
    }
    if ( StringUtils.isNotEmpty( maxWindowField ) ) {
      ValueMetaDate valueMeta = new ValueMetaDate( variables.resolve( maxWindowField ) );
      valueMeta.setOrigin( name );
      valueMeta.setConversionMask( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK );
      inputRowMeta.addValueMeta( valueMeta );
    }

  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( WINDOW_TYPE, windowType ) );
    xml.append( XmlHandler.addTagValue( DURATION, duration ) );
    xml.append( XmlHandler.addTagValue( EVERY, every ) );
    xml.append( XmlHandler.addTagValue( MAX_WINDOW_FIELD, maxWindowField ) );
    xml.append( XmlHandler.addTagValue( START_WINDOW_FIELD, startWindowField ) );
    xml.append( XmlHandler.addTagValue( END_WINDOW_FIELD, endWindowField ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    windowType = XmlHandler.getTagValue( transformNode, WINDOW_TYPE );
    duration = XmlHandler.getTagValue( transformNode, DURATION );
    every = XmlHandler.getTagValue( transformNode, EVERY );
    maxWindowField = XmlHandler.getTagValue( transformNode, MAX_WINDOW_FIELD );
    startWindowField = XmlHandler.getTagValue( transformNode, START_WINDOW_FIELD );
    endWindowField = XmlHandler.getTagValue( transformNode, END_WINDOW_FIELD );
  }


  /**
   * Gets windowType
   *
   * @return value of windowType
   */
  public String getWindowType() {
    return windowType;
  }

  /**
   * @param windowType The windowType to set
   */
  public void setWindowType( String windowType ) {
    this.windowType = windowType;
  }

  /**
   * Gets duration
   *
   * @return value of duration
   */
  public String getDuration() {
    return duration;
  }

  /**
   * @param duration The duration to set
   */
  public void setDuration( String duration ) {
    this.duration = duration;
  }

  /**
   * Gets every
   *
   * @return value of every
   */
  public String getEvery() {
    return every;
  }

  /**
   * @param every The every to set
   */
  public void setEvery( String every ) {
    this.every = every;
  }

  /**
   * Gets maxWindowField
   *
   * @return value of maxWindowField
   */
  public String getMaxWindowField() {
    return maxWindowField;
  }

  /**
   * @param maxWindowField The maxWindowField to set
   */
  public void setMaxWindowField( String maxWindowField ) {
    this.maxWindowField = maxWindowField;
  }

  /**
   * Gets startWindowField
   *
   * @return value of startWindowField
   */
  public String getStartWindowField() {
    return startWindowField;
  }

  /**
   * @param startWindowField The startWindowField to set
   */
  public void setStartWindowField( String startWindowField ) {
    this.startWindowField = startWindowField;
  }

  /**
   * Gets endWindowField
   *
   * @return value of endWindowField
   */
  public String getEndWindowField() {
    return endWindowField;
  }

  /**
   * @param endWindowField The endWindowField to set
   */
  public void setEndWindowField( String endWindowField ) {
    this.endWindowField = endWindowField;
  }
}

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

package org.apache.hop.beam.core.transform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopToStringFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class BeamOutputTransform extends PTransform<PCollection<HopRow>, PDone> {

  // These non-transient privates get serialized to spread across nodes
  //
  private String transformName;
  private String outputLocation;
  private String filePrefix;
  private String fileSuffix;
  private String separator;
  private String enclosure;
  private String rowMetaJson;
  private boolean windowed;
  private List<String> transformPluginClasses;
  private List<String> xpPluginClasses;

  // Log and count errors.
  private static final Logger LOG = LoggerFactory.getLogger( BeamOutputTransform.class );
  private static final Counter numErrors = Metrics.counter( "main", "BeamOutputError" );

  public BeamOutputTransform() {
  }

  public BeamOutputTransform( String transformName, String outputLocation, String filePrefix, String fileSuffix, String separator, String enclosure, boolean windowed, String rowMetaJson, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    this.transformName = transformName;
    this.outputLocation = outputLocation;
    this.filePrefix = filePrefix;
    this.fileSuffix = fileSuffix;
    this.separator = separator;
    this.enclosure = enclosure;
    this.windowed = windowed;
    this.rowMetaJson = rowMetaJson;
    this.transformPluginClasses = transformPluginClasses;
    this.xpPluginClasses = xpPluginClasses;
  }

  @Override public PDone expand( PCollection<HopRow> input ) {

    try {
      // Only initialize once on this node/vm
      //
      BeamHop.init(transformPluginClasses, xpPluginClasses);

      // Inflate the metadata on the node where this is running...
      //
      IRowMeta rowMeta = JsonRowMeta.fromJson( rowMetaJson );

      // This is the end of a computing chain, we write out the results
      // We write a bunch of Strings, one per line basically
      //
      PCollection<String> stringCollection = input.apply( transformName, ParDo.of( new HopToStringFn( transformName, outputLocation, separator, enclosure, rowMetaJson, transformPluginClasses, xpPluginClasses ) ) );

      // We need to transform these lines into a file and then we're PDone
      //
      TextIO.Write write = TextIO.write();
      if ( StringUtils.isNotEmpty(outputLocation)) {
        String outputPrefix = outputLocation;
        if (!outputPrefix.endsWith( File.separator)) {
          outputPrefix+=File.separator;
        }
        if (StringUtils.isNotEmpty( filePrefix )) {
          outputPrefix+=filePrefix;
        }
        write = write.to( outputPrefix );
      }
      if (StringUtils.isNotEmpty( fileSuffix )) {
        write = write.withSuffix( fileSuffix );
      }

      // For streaming data sources...
      //
      if (windowed) {
        write = write.withWindowedWrites().withNumShards( 4 ); // TODO config
      }

      stringCollection.apply(write);

      // Get it over with
      //
      return PDone.in(input.getPipeline());

    } catch ( Exception e ) {
      numErrors.inc();
      LOG.error( "Error in beam output transform", e );
      throw new RuntimeException( "Error in beam output transform", e );
    }
  }


  /**
   * Gets transformName
   *
   * @return value of transformName
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * @param transformName The transformName to set
   */
  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  /**
   * Gets outputLocation
   *
   * @return value of outputLocation
   */
  public String getOutputLocation() {
    return outputLocation;
  }

  /**
   * @param outputLocation The outputLocation to set
   */
  public void setOutputLocation( String outputLocation ) {
    this.outputLocation = outputLocation;
  }

  /**
   * Gets filePrefix
   *
   * @return value of filePrefix
   */
  public String getFilePrefix() {
    return filePrefix;
  }

  /**
   * @param filePrefix The filePrefix to set
   */
  public void setFilePrefix( String filePrefix ) {
    this.filePrefix = filePrefix;
  }

  /**
   * Gets fileSuffix
   *
   * @return value of fileSuffix
   */
  public String getFileSuffix() {
    return fileSuffix;
  }

  /**
   * @param fileSuffix The fileSuffix to set
   */
  public void setFileSuffix( String fileSuffix ) {
    this.fileSuffix = fileSuffix;
  }

  /**
   * Gets separator
   *
   * @return value of separator
   */
  public String getSeparator() {
    return separator;
  }

  /**
   * @param separator The separator to set
   */
  public void setSeparator( String separator ) {
    this.separator = separator;
  }

  /**
   * Gets enclosure
   *
   * @return value of enclosure
   */
  public String getEnclosure() {
    return enclosure;
  }

  /**
   * @param enclosure The enclosure to set
   */
  public void setEnclosure( String enclosure ) {
    this.enclosure = enclosure;
  }

  /**
   * Gets inputRowMetaJson
   *
   * @return value of inputRowMetaJson
   */
  public String getRowMetaJson() {
    return rowMetaJson;
  }

  /**
   * @param rowMetaJson The inputRowMetaJson to set
   */
  public void setRowMetaJson( String rowMetaJson ) {
    this.rowMetaJson = rowMetaJson;
  }
}

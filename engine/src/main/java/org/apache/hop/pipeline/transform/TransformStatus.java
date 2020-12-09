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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.owasp.encoder.Encode;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.bind.annotation.XmlRootElement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


@XmlRootElement
public class TransformStatus {
  public static final String XML_TAG = "transform_status";

  private String transformName;
  private int copy;
  private long linesRead;
  private long linesWritten;
  private long linesInput;
  private long linesOutput;
  private long linesUpdated;
  private long linesRejected;
  private long inputBufferSize;
  private long outputBufferSize;
  private long errors;
  private String statusDescription;
  private double seconds;
  private String speed;
  private String priority;
  private boolean stopped;
  private boolean paused;
  private long accumulatedRuntime;

  private IRowMeta sampleRowMeta;
  private List<Object[]> sampleRows;
  private final DecimalFormat speedDf = new DecimalFormat( "#,###,###,###,##0" );

  public TransformStatus() {
    sampleRows = Collections.synchronizedList( new LinkedList<>() );
  }

  public TransformStatus( IEngineComponent component ) {
    updateAll( component );
  }

  public synchronized void updateAll( IEngineComponent component ) {
    // Proc: nr of lines processed: input + output!

    this.transformName = component.getName();
    this.copy = component.getCopyNr();
    this.linesRead = linesRead + component.getLinesRead();
    this.linesWritten = linesWritten + component.getLinesWritten();
    this.linesInput = linesInput + component.getLinesInput();
    this.linesOutput = linesOutput + component.getLinesOutput();
    this.linesUpdated = linesUpdated + component.getLinesUpdated();
    this.linesRejected = linesRejected + component.getLinesRejected();
    this.errors = errors + component.getErrors();
    this.accumulatedRuntime = accumulatedRuntime + component.getExecutionDuration();
    this.statusDescription = component.getStatusDescription();

    long in_proc = Math.max( linesInput, linesRead );
    long out_proc = Math.max( linesOutput + linesUpdated, linesWritten + linesRejected );

    float lapsed = ( (float) accumulatedRuntime ) / 1000;
    double in_speed = 0;
    double out_speed = 0;

    if ( lapsed != 0 ) {
      in_speed = Math.floor( 10 * ( in_proc / lapsed ) ) / 10;
      out_speed = Math.floor( 10 * ( out_proc / lapsed ) ) / 10;
    }

    double speedNumber = ( in_speed > out_speed ? in_speed : out_speed );

    this.seconds = Math.floor( ( lapsed * 10 ) + 0.5 ) / 10;
    this.speed = lapsed == 0 ? "-" : " " + speedDf.format( speedNumber );
    this.priority = component.isRunning() ? "   " + component.getInputBufferSize() + "/" + component.getOutputBufferSize() : "-";
    this.stopped = component.isStopped();
    this.paused = component.isPaused();

    // get the total input and output buffer size (if there are any)
    //
    this.inputBufferSize+=component.getInputBufferSize();
    this.outputBufferSize+=component.getOutputBufferSize();
  }

  public String getHTMLTableRow( boolean urlInTransformName ) {
    return "<tr> " + "<th>"
      + ( urlInTransformName ? transformName : Encode.forHtml( transformName ) ) + "</th> " + "<th>" + copy + "</th> "
      + "<th>" + linesRead + "</th> " + "<th>" + linesWritten + "</th> " + "<th>" + linesInput + "</th> "
      + "<th>" + linesOutput + "</th> " + "<th>" + linesUpdated + "</th> " + "<th>" + linesRejected + "</th> "
      + "<th>" + errors + "</th> " + "<th>" + Encode.forHtml( statusDescription ) + "</th> " + "<th>"
      + seconds + "</th> " + "<th>" + Encode.forHtml( speed ) + "</th> " + "<th>"
      + Encode.forHtml( priority ) + "</th> " + "</tr>";
  }

  public String getXml() throws HopException {
    try {
      StringBuilder xml = new StringBuilder();
      xml.append( XmlHandler.openTag( XML_TAG ) );

      xml.append( XmlHandler.addTagValue( "transformName", transformName, false ) );
      xml.append( XmlHandler.addTagValue( "copy", copy, false ) );
      xml.append( XmlHandler.addTagValue( "linesRead", linesRead, false ) );
      xml.append( XmlHandler.addTagValue( "linesWritten", linesWritten, false ) );
      xml.append( XmlHandler.addTagValue( "linesInput", linesInput, false ) );
      xml.append( XmlHandler.addTagValue( "linesOutput", linesOutput, false ) );
      xml.append( XmlHandler.addTagValue( "linesUpdated", linesUpdated, false ) );
      xml.append( XmlHandler.addTagValue( "linesRejected", linesRejected, false ) );
      xml.append( XmlHandler.addTagValue( "errors", errors, false ) );
      xml.append( XmlHandler.addTagValue( "input_buffer_size", inputBufferSize, false ) );
      xml.append( XmlHandler.addTagValue( "output_buffer_size", outputBufferSize, false ) );
      xml.append( XmlHandler.addTagValue( "statusDescription", statusDescription, false ) );
      xml.append( XmlHandler.addTagValue( "seconds", seconds, false ) );
      xml.append( XmlHandler.addTagValue( "speed", speed, false ) );
      xml.append( XmlHandler.addTagValue( "priority", priority, false ) );
      xml.append( XmlHandler.addTagValue( "stopped", stopped, false ) );
      xml.append( XmlHandler.addTagValue( "paused", paused, false ) );

      if ( sampleRowMeta != null ) {
        xml.append( XmlHandler.openTag( "samples" ) );
        xml.append( sampleRowMeta.getMetaXml() );
        xml.append( Const.CR );
        if ( sampleRows != null ) {
          synchronized ( sampleRows ) {
            Iterator<Object[]> iterator = sampleRows.iterator();
            while ( iterator.hasNext() ) {
              Object[] sampleRow = iterator.next();
              xml.append( sampleRowMeta.getDataXml( sampleRow ) );
              xml.append( Const.CR );
            }
          }
        }
        xml.append( XmlHandler.closeTag( "samples" ) );
      }

      xml.append( XmlHandler.closeTag( XML_TAG ) );
      return xml.toString();
    } catch ( Exception e ) {
      throw new HopException( "Unable to serialize transform '" + transformName + "' status data to XML", e );
    }
  }

  public TransformStatus( Node node ) throws HopException {
    transformName = XmlHandler.getTagValue( node, "transformName" );
    copy = Integer.parseInt( XmlHandler.getTagValue( node, "copy" ) );
    linesRead = Long.parseLong( XmlHandler.getTagValue( node, "linesRead" ) );
    linesWritten = Long.parseLong( XmlHandler.getTagValue( node, "linesWritten" ) );
    linesInput = Long.parseLong( XmlHandler.getTagValue( node, "linesInput" ) );
    linesOutput = Long.parseLong( XmlHandler.getTagValue( node, "linesOutput" ) );
    linesUpdated = Long.parseLong( XmlHandler.getTagValue( node, "linesUpdated" ) );
    linesRejected = Long.parseLong( XmlHandler.getTagValue( node, "linesRejected" ) );
    errors = Long.parseLong( XmlHandler.getTagValue( node, "errors" ) );
    inputBufferSize = Long.parseLong( XmlHandler.getTagValue( node, "input_buffer_size" ) );
    outputBufferSize = Long.parseLong( XmlHandler.getTagValue( node, "output_buffer_size" ) );
    statusDescription = XmlHandler.getTagValue( node, "statusDescription" );
    seconds = Double.parseDouble( XmlHandler.getTagValue( node, "seconds" ) );
    speed = XmlHandler.getTagValue( node, "speed" );
    priority = XmlHandler.getTagValue( node, "priority" );
    stopped = "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, "stopped" ) );
    paused = "Y".equalsIgnoreCase( XmlHandler.getTagValue( node, "paused" ) );

    Node samplesNode = XmlHandler.getSubNode( node, "samples" );
    if ( samplesNode != null ) {
      Node rowMetaNode = XmlHandler.getSubNode( samplesNode, RowMeta.XML_META_TAG );
      if ( rowMetaNode != null ) {
        sampleRowMeta = new RowMeta( rowMetaNode );
        sampleRows = new ArrayList<>();
        List<Node> dataNodes = XmlHandler.getNodes( samplesNode, RowMeta.XML_DATA_TAG );
        for ( Node dataNode : dataNodes ) {
          Object[] sampleRow = sampleRowMeta.getRow( dataNode );
          sampleRows.add( sampleRow );
        }
      }
    }
  }

  public TransformStatus fromXml(String xml ) throws HopException {
    Document document = XmlHandler.loadXmlString( xml );
    return new TransformStatus( XmlHandler.getSubNode( document, XML_TAG ) );
  }

  public String[] getPipelineLogFields() {
    return getPipelineLogFields( statusDescription );
  }

  public String[] getPipelineLogFields( String overrideDescription ) {
    String[] fields =
      new String[] {
        "", // Row number
        transformName, Integer.toString( copy ), Long.toString( linesRead ), Long.toString( linesWritten ),
        Long.toString( linesInput ), Long.toString( linesOutput ), Long.toString( linesUpdated ),
        Long.toString( linesRejected ), Long.toString( errors ), overrideDescription, convertSeconds( seconds ),
        speed, priority, };

    return fields;
  }

  private String convertSeconds( double seconds ) {
    String retval = seconds + "s";

    if ( seconds < 60 ) {
      return retval;
    }

    double donnee = seconds;
    int mn = (int) donnee / 60;
    int h = mn / 60;
    mn = mn % 60;
    int s = (int) donnee % 60;

    if ( h > 0 ) {
      retval = h + "h " + mn + "mn " + s + "s";
    } else {
      if ( mn > 0 ) {
        retval = mn + "mn " + s + "s";
      } else {
        retval = seconds + "s";
      }
    }

    return retval;
  }

  public String[] getPeekFields() {
    String[] fields =
      new String[] {

        Integer.toString( copy ), Long.toString( linesRead ), Long.toString( linesWritten ),
        Long.toString( linesInput ), Long.toString( linesOutput ), Long.toString( linesUpdated ),
        Long.toString( linesRejected ), Long.toString( errors ), statusDescription, convertSeconds( seconds ),
        speed, priority, };

    return fields;

  }

  /**
   * @return the copy
   */
  public int getCopy() {
    return copy;
  }

  /**
   * @param copy the copy to set
   */
  public void setCopy( int copy ) {
    this.copy = copy;
  }

  /**
   * @return the errors
   */
  public long getErrors() {
    return errors;
  }

  /**
   * @param errors the errors to set
   */
  public void setErrors( long errors ) {
    this.errors = errors;
  }

  /**
   * @return the linesInput
   */
  public long getLinesInput() {
    return linesInput;
  }

  /**
   * @param linesInput the linesInput to set
   */
  public void setLinesInput( long linesInput ) {
    this.linesInput = linesInput;
  }

  /**
   * @return the linesOutput
   */
  public long getLinesOutput() {
    return linesOutput;
  }

  /**
   * @param linesOutput the linesOutput to set
   */
  public void setLinesOutput( long linesOutput ) {
    this.linesOutput = linesOutput;
  }

  /**
   * @return the linesRead
   */
  public long getLinesRead() {
    return linesRead;
  }

  /**
   * @param linesRead the linesRead to set
   */
  public void setLinesRead( long linesRead ) {
    this.linesRead = linesRead;
  }

  /**
   * @return the linesUpdated
   */
  public long getLinesUpdated() {
    return linesUpdated;
  }

  /**
   * @param linesUpdated the linesUpdated to set
   */
  public void setLinesUpdated( long linesUpdated ) {
    this.linesUpdated = linesUpdated;
  }

  /**
   * @return the linesWritten
   */
  public long getLinesWritten() {
    return linesWritten;
  }

  /**
   * @param linesWritten the linesWritten to set
   */
  public void setLinesWritten( long linesWritten ) {
    this.linesWritten = linesWritten;
  }

  /**
   * @return the priority
   */
  public String getPriority() {
    return priority;
  }

  /**
   * @param priority the priority to set
   */
  public void setPriority( String priority ) {
    this.priority = priority;
  }

  /**
   * @return the seconds
   */
  public double getSeconds() {
    return seconds;
  }

  /**
   * @param seconds the seconds to set
   */
  public void setSeconds( double seconds ) {
    this.seconds = seconds;
  }

  /**
   * @return the speed
   */
  public String getSpeed() {
    return speed;
  }

  /**
   * @param speed the speed to set
   */
  public void setSpeed( String speed ) {
    this.speed = speed;
  }

  /**
   * @return the statusDescription
   */
  public String getStatusDescription() {
    return statusDescription;
  }

  /**
   * @param statusDescription the statusDescription to set
   */
  public void setStatusDescription( String statusDescription ) {
    this.statusDescription = statusDescription;
  }

  /**
   * @return the transformName
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * @param transformName the transformName to set
   */
  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  /**
   * @return the linesRejected
   */
  public long getLinesRejected() {
    return linesRejected;
  }

  /**
   * @param linesRejected the linesRejected to set
   */
  public void setLinesRejected( long linesRejected ) {
    this.linesRejected = linesRejected;
  }

  /**
   * @return the stopped
   */
  public boolean isStopped() {
    return stopped;
  }

  /**
   * @param stopped the stopped to set
   */
  public void setStopped( boolean stopped ) {
    this.stopped = stopped;
  }

  /**
   * @return the paused
   */
  public boolean isPaused() {
    return paused;
  }

  /**
   * @param paused the paused to set
   */
  public void setPaused( boolean paused ) {
    this.paused = paused;
  }

  public IRowMeta getSampleRowMeta() {
    return sampleRowMeta;
  }

  public void setSampleRowMeta( IRowMeta sampleRowMeta ) {
    this.sampleRowMeta = sampleRowMeta;
  }

  public List<Object[]> getSampleRows() {
    return sampleRows;
  }

  public void setSampleRows( List<Object[]> sampleRows ) {
    this.sampleRows = sampleRows;
  }

  /**
   * Gets inputBufferSize
   *
   * @return value of inputBufferSize
   */
  public long getInputBufferSize() {
    return inputBufferSize;
  }

  /**
   * @param inputBufferSize The inputBufferSize to set
   */
  public void setInputBufferSize( long inputBufferSize ) {
    this.inputBufferSize = inputBufferSize;
  }

  /**
   * Gets outputBufferSize
   *
   * @return value of outputBufferSize
   */
  public long getOutputBufferSize() {
    return outputBufferSize;
  }

  /**
   * @param outputBufferSize The outputBufferSize to set
   */
  public void setOutputBufferSize( long outputBufferSize ) {
    this.outputBufferSize = outputBufferSize;
  }
}

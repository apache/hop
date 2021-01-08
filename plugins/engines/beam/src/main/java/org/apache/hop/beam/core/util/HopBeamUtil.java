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

package org.apache.hop.beam.core.util;

import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlHandlerCache;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HopBeamUtil {

  public static final String createTargetTupleId(String transformName, String targetTransformName){
    return transformName+" - TARGET - "+targetTransformName;
  }

  public static final String createMainOutputTupleId(String transformName){
    return transformName+" - OUTPUT";
  }

  public static final String createInfoTupleId(String transformName, String infoTransformName){
    return infoTransformName+" - INFO - "+transformName;
  }

  public static final String createMainInputTupleId(String transformName){
    return transformName+" - INPUT";
  }


  public static final HopRow copyHopRow( HopRow hopRow, IRowMeta rowMeta ) throws HopException {
    Object[] newRow = RowDataUtil.createResizedCopy(hopRow.getRow(), rowMeta.size());
    return new HopRow(newRow);
  }

  private static Object object = new Object();

  public static void loadTransformMetadataFromXml( String transformName, ITransformMeta iTransformMeta, String iTransformXml, IHopMetadataProvider metadataProvider ) throws HopException {
    synchronized ( object ) {
      Document transformDocument = XmlHandler.loadXmlString( iTransformXml );
      if ( transformDocument == null ) {
        throw new HopException( "Unable to load transform XML document from : " + iTransformXml );
      }
      Node transformNode = XmlHandler.getSubNode( transformDocument, TransformMeta.XML_TAG );
      if ( transformNode == null ) {
        throw new HopException( "Unable to find XML tag " + TransformMeta.XML_TAG + " from : " + iTransformXml );
      }
      try {
        iTransformMeta.loadXml( transformNode, metadataProvider );
      } catch ( Exception e ) {
        throw new HopException( "There was an error loading transform metadata information (loadXml) for transform '" + transformName + "'", e );
      } finally {
        XmlHandlerCache.getInstance().clear();
      }
    }
  }
}

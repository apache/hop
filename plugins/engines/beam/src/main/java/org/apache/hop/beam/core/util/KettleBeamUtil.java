package org.apache.hop.beam.core.util;

import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlHandlerCache;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class KettleBeamUtil {

  public static final String createTargetTupleId(String transformName, String targetStepname){
    return transformName+" - TARGET - "+targetStepname;
  }

  public static final String createMainOutputTupleId(String transformName){
    return transformName+" - OUTPUT";
  }

  public static final String createInfoTupleId(String transformName, String infoStepname){
    return infoStepname+" - INFO - "+transformName;
  }

  public static final String createMainInputTupleId(String transformName){
    return transformName+" - INPUT";
  }


  public static final HopRow copyHopRow( HopRow kettleRow, IRowMeta rowMeta ) throws HopException {
    Object[] newRow = RowDataUtil.createResizedCopy(kettleRow.getRow(), rowMeta.size());
    return new HopRow(newRow);
  }

  private static Object object = new Object();

  public static void loadTransformMetadataFromXml( String transformName, ITransformMeta iTransformMeta, String stepMetaInterfaceXml, IMetaStore metaStore ) throws HopException {
    synchronized ( object ) {
      Document stepDocument = XmlHandler.loadXmlString( stepMetaInterfaceXml );
      if ( stepDocument == null ) {
        throw new HopException( "Unable to load transform XML document from : " + stepMetaInterfaceXml );
      }
      Node transformNode = XmlHandler.getSubNode( stepDocument, TransformMeta.XML_TAG );
      if ( transformNode == null ) {
        throw new HopException( "Unable to find XML tag " + TransformMeta.XML_TAG + " from : " + stepMetaInterfaceXml );
      }
      try {
        iTransformMeta.loadXml( transformNode, metaStore );
      } catch ( Exception e ) {
        throw new HopException( "There was an error loading transform metadata information (loadXml) for transform '" + transformName + "'", e );
      } finally {
        XmlHandlerCache.getInstance().clear();
      }
    }
  }
}

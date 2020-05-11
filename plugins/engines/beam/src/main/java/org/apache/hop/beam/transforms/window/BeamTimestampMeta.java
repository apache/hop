package org.apache.hop.beam.transforms.window;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.IMetaStore;
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
  id = "BeamTimestamp",
  name = "Beam Timestamp",
  description = "Add timestamps to a bounded data source",
  image = "beam-timestamp.svg",
  categoryDescription = "Big Data"
)
public class BeamTimestampMeta extends BaseTransformMeta implements ITransformMeta<Dummy, DummyData> {

  public static final String FIELD_NAME = "field_name";
  public static final String READ_TIMESTAMP = "read_timestamp";

  private String fieldName;

  private boolean readingTimestamp;

  public BeamTimestampMeta() {
    super();
  }

  @Override public void setDefault() {
    fieldName = "";
  }

  @Override public Dummy createTransform( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Dummy( transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline );
  }

  @Override public DummyData getTransformData() {
    return new DummyData();
  }

  @Override public String getDialogClassName() {
    return BeamTimestampDialog.class.getName();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextStep, IVariables variables, IMetaStore metaStore )
    throws HopTransformException {

    if ( readingTimestamp ) {
      ValueMetaDate valueMeta = new ValueMetaDate( fieldName );
      valueMeta.setOrigin( name );
      inputRowMeta.addValueMeta( valueMeta );
    }
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( FIELD_NAME, fieldName ) );
    xml.append( XmlHandler.addTagValue( READ_TIMESTAMP, readingTimestamp ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IMetaStore metaStore ) throws HopXmlException {
    fieldName = XmlHandler.getTagValue( transformNode, FIELD_NAME );
    readingTimestamp = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, READ_TIMESTAMP ) );
  }


  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * Gets readingTimestamp
   *
   * @return value of readingTimestamp
   */
  public boolean isReadingTimestamp() {
    return readingTimestamp;
  }

  /**
   * @param readingTimestamp The readingTimestamp to set
   */
  public void setReadingTimestamp( boolean readingTimestamp ) {
    this.readingTimestamp = readingTimestamp;
  }
}

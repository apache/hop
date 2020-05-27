package org.apache.hop.beam.transforms.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.metastore.FileDefinition;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
  id = "BeamOutput",
  name = "Beam Output",
  description = "Describes a Beam Output",
  categoryDescription = "Big Data"
)
public class BeamOutputMeta extends BaseTransformMeta implements ITransformMeta<BeamOutput, BeamOutputData> {

  public static final String OUTPUT_LOCATION = "output_location";
  public static final String FILE_DESCRIPTION_NAME = "file_description_name";
  public static final String FILE_PREFIX = "file_prefix";
  public static final String FILE_SUFFIX = "file_suffix";
  public static final String WINDOWED = "windowed";


  private String outputLocation;

  private String fileDescriptionName;

  private String filePrefix;

  private String fileSuffix;

  private boolean windowed;

  @Override public void setDefault() {
  }

  @Override public BeamOutput createTransform( TransformMeta transformMeta, BeamOutputData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {

    return new BeamOutput( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public BeamOutputData getTransformData() {
    return new BeamOutputData();
  }

  public FileDefinition loadFileDefinition( IMetaStore metaStore) throws HopTransformException {
    if ( StringUtils.isEmpty(fileDescriptionName)) {
      throw new HopTransformException("No file description name provided");
    }
    FileDefinition fileDefinition;
    try {
      MetaStoreFactory<FileDefinition> factory = new MetaStoreFactory<>( FileDefinition.class, metaStore );
      fileDefinition = factory.loadElement( fileDescriptionName );
    } catch(Exception e) {
      throw new HopTransformException( "Unable to load file description '"+fileDescriptionName+"' from the metastore", e );
    }

    return fileDefinition;
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer(  );

    xml.append( XmlHandler.addTagValue( OUTPUT_LOCATION, outputLocation ) );
    xml.append( XmlHandler.addTagValue( FILE_DESCRIPTION_NAME, fileDescriptionName) );
    xml.append( XmlHandler.addTagValue( FILE_PREFIX, filePrefix) );
    xml.append( XmlHandler.addTagValue( FILE_SUFFIX, fileSuffix) );
    xml.append( XmlHandler.addTagValue( WINDOWED, windowed) );

    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IMetaStore metaStore ) throws HopXmlException {

    outputLocation = XmlHandler.getTagValue( transformNode, OUTPUT_LOCATION );
    fileDescriptionName = XmlHandler.getTagValue( transformNode, FILE_DESCRIPTION_NAME );
    filePrefix = XmlHandler.getTagValue( transformNode, FILE_PREFIX );
    fileSuffix = XmlHandler.getTagValue( transformNode, FILE_SUFFIX );
    windowed = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, WINDOWED) );

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
   * Gets fileDescriptionName
   *
   * @return value of fileDescriptionName
   */
  public String getFileDescriptionName() {
    return fileDescriptionName;
  }

  /**
   * @param fileDescriptionName The fileDescriptionName to set
   */
  public void setFileDescriptionName( String fileDescriptionName ) {
    this.fileDescriptionName = fileDescriptionName;
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
   * Gets windowed
   *
   * @return value of windowed
   */
  public boolean isWindowed() {
    return windowed;
  }

  /**
   * @param windowed The windowed to set
   */
  public void setWindowed( boolean windowed ) {
    this.windowed = windowed;
  }
}

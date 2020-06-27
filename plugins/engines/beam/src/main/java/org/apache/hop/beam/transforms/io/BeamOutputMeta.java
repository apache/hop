package org.apache.hop.beam.transforms.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
        id = "BeamOutput",
        image = "beam-output.svg",
        name = "Beam Output",
        description = "Describes a Beam Output",
        categoryDescription = "Big Data",
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/transforms/beamoutput.html"
)
public class BeamOutputMeta extends BaseTransformMeta implements ITransformMeta<BeamOutput, BeamOutputData> {

  public static final String OUTPUT_LOCATION = "output_location";
  public static final String FILE_DESCRIPTION_NAME = "file_description_name";
  public static final String FILE_PREFIX = "file_prefix";
  public static final String FILE_SUFFIX = "file_suffix";
  public static final String WINDOWED = "windowed";


  private String outputLocation;

  private String fileDefinitionName;

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

  @Override public String getDialogClassName() {
    return BeamOutputDialog.class.getName();
  }

  public FileDefinition loadFileDefinition( IHopMetadataProvider metadataProvider) throws HopTransformException {
    if ( StringUtils.isEmpty( fileDefinitionName )) {
      throw new HopTransformException("No file description name provided");
    }
    FileDefinition fileDefinition;
    try {
      IHopMetadataSerializer<FileDefinition> serializer = metadataProvider.getSerializer( FileDefinition.class );
      fileDefinition = serializer.load( fileDefinitionName );
    } catch(Exception e) {
      throw new HopTransformException( "Unable to load file description '"+ fileDefinitionName +"' from the metadata", e );
    }

    return fileDefinition;
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer(  );

    xml.append( XmlHandler.addTagValue( OUTPUT_LOCATION, outputLocation ) );
    xml.append( XmlHandler.addTagValue( FILE_DESCRIPTION_NAME, fileDefinitionName ) );
    xml.append( XmlHandler.addTagValue( FILE_PREFIX, filePrefix) );
    xml.append( XmlHandler.addTagValue( FILE_SUFFIX, fileSuffix) );
    xml.append( XmlHandler.addTagValue( WINDOWED, windowed) );

    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {

    outputLocation = XmlHandler.getTagValue( transformNode, OUTPUT_LOCATION );
    fileDefinitionName = XmlHandler.getTagValue( transformNode, FILE_DESCRIPTION_NAME );
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
  public String getFileDefinitionName() {
    return fileDefinitionName;
  }

  /**
   * @param fileDefinitionName The fileDescriptionName to set
   */
  public void setFileDefinitionName( String fileDefinitionName ) {
    this.fileDefinitionName = fileDefinitionName;
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

package org.apache.hop.beam.transforms.io;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.metastore.FileDefinition;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
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
  id = "BeamInput",
  name = "Beam Input",
  description = "Describes a Beam Input",
  categoryDescription = "Big Data"
)
public class BeamInputMeta extends BaseTransformMeta implements ITransformMeta<BeamInput, BeamInputData> {

  public static final String INPUT_LOCATION = "input_location";
  public static final String FILE_DESCRIPTION_NAME = "file_description_name";

  private String inputLocation;

  private String fileDescriptionName;

  public BeamInputMeta() {
    super();
  }

  @Override public void setDefault() {
  }

  @Override public BeamInput createTransform( TransformMeta transformMeta, BeamInputData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new BeamInput( transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override public BeamInputData getTransformData() {
    return new BeamInputData();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextStep, IVariables variables, IMetaStore metaStore )
    throws HopTransformException {

    if (metaStore!=null) {
      FileDefinition fileDefinition = loadFileDefinition( metaStore );

      try {
        inputRowMeta.clear();
        inputRowMeta.addRowMeta( fileDefinition.getRowMeta() );
      } catch ( HopPluginException e ) {
        throw new HopTransformException( "Unable to get row layout of file definition '" + fileDefinition.getName() + "'", e );
      }
    }
  }

  public FileDefinition loadFileDefinition( IMetaStore metaStore) throws HopTransformException {
    if (StringUtils.isEmpty(fileDescriptionName)) {
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

    xml.append( XmlHandler.addTagValue( INPUT_LOCATION, inputLocation ) );
    xml.append( XmlHandler.addTagValue( FILE_DESCRIPTION_NAME, fileDescriptionName) );

    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IMetaStore metaStore ) throws HopXmlException {

    inputLocation = XmlHandler.getTagValue( transformNode, INPUT_LOCATION );
    fileDescriptionName = XmlHandler.getTagValue( transformNode, FILE_DESCRIPTION_NAME );

  }


  /**
   * Gets inputLocation
   *
   * @return value of inputLocation
   */
  public String getInputLocation() {
    return inputLocation;
  }

  /**
   * @param inputLocation The inputLocation to set
   */
  public void setInputLocation( String inputLocation ) {
    this.inputLocation = inputLocation;
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

}

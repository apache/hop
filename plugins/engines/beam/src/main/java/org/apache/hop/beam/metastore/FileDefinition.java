package org.apache.hop.beam.metastore;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.IHopMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;
import org.apache.hop.metastore.persist.MetaStoreFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@MetaStoreElementType(
  name = "Beam File Definition",
  description = "Describes a file layout in a Beam pipeline"
)
public class FileDefinition extends Variables implements Serializable, IVariables, IHopMetaStoreElement<FileDefinition> {

  private String name;

  @MetaStoreAttribute
  private String description;

  @MetaStoreAttribute
  private List<FieldDefinition> fieldDefinitions;

  @MetaStoreAttribute
  private String separator;

  @MetaStoreAttribute
  private String enclosure;

  public FileDefinition() {
    fieldDefinitions = new ArrayList<>();
  }

  public FileDefinition( String name, String description, List<FieldDefinition> fieldDefinitions, String separator, String enclosure ) {
    this.name = name;
    this.description = description;
    this.fieldDefinitions = fieldDefinitions;
    this.separator = separator;
    this.enclosure = enclosure;
  }

  public IRowMeta getRowMeta() throws HopPluginException {
    IRowMeta rowMeta = new RowMeta();

    for ( FieldDefinition fieldDefinition : fieldDefinitions) {
      rowMeta.addValueMeta( fieldDefinition.getValueMeta() );
    }

    return rowMeta;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets fieldDefinitions
   *
   * @return value of fieldDefinitions
   */
  public List<FieldDefinition> getFieldDefinitions() {
    return fieldDefinitions;
  }

  /**
   * @param fieldDefinitions The fieldDefinitions to set
   */
  public void setFieldDefinitions( List<FieldDefinition> fieldDefinitions ) {
    this.fieldDefinitions = fieldDefinitions;
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

  @Override public MetaStoreFactory<FileDefinition> getFactory( IMetaStore metaStore ) {
    return createFactory(metaStore);
  }

  public static final MetaStoreFactory<FileDefinition> createFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<>( FileDefinition.class, metaStore );
  }
}

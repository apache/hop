package org.apache.hop.beam.metastore;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metastore.persist.MetaStoreAttribute;

import java.io.Serializable;

public class FieldDefinition implements Serializable {

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String hopType;

  @MetaStoreAttribute
  private int length;

  @MetaStoreAttribute
  private int precision;

  @MetaStoreAttribute
  private String formatMask;

  public FieldDefinition( ) {
  }

  public FieldDefinition( String name, String hopType, int length, int precision ) {
    this.name = name;
    this.hopType = hopType;
    this.length = length;
    this.precision = precision;
  }

  public FieldDefinition( String name, String hopType, int length, int precision, String formatMask ) {
    this.name = name;
    this.hopType = hopType;
    this.length = length;
    this.precision = precision;
    this.formatMask = formatMask;
  }

  public IValueMeta getValueMeta() throws HopPluginException {
    int type = ValueMetaFactory.getIdForValueMeta( hopType );
    IValueMeta valueMeta = ValueMetaFactory.createValueMeta( name, type, length, precision );
    valueMeta.setConversionMask( formatMask );
    return valueMeta;
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
   * Gets hopType
   *
   * @return value of hopType
   */
  public String getHopType() {
    return hopType;
  }

  /**
   * @param hopType The hopType to set
   */
  public void setHopType( String hopType ) {
    this.hopType = hopType;
  }

  /**
   * Gets length
   *
   * @return value of length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length The length to set
   */
  public void setLength( int length ) {
    this.length = length;
  }

  /**
   * Gets precision
   *
   * @return value of precision
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * @param precision The precision to set
   */
  public void setPrecision( int precision ) {
    this.precision = precision;
  }

  /**
   * Gets formatMask
   *
   * @return value of formatMask
   */
  public String getFormatMask() {
    return formatMask;
  }

  /**
   * @param formatMask The formatMask to set
   */
  public void setFormatMask( String formatMask ) {
    this.formatMask = formatMask;
  }
}

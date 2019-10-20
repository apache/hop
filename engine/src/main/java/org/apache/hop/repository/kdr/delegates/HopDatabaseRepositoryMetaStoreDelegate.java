/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.repository.kdr.delegates;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.timestamp.SimpleTimestampFormat;
import org.apache.hop.repository.LongObjectId;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.kdr.HopDatabaseRepository;
import org.apache.hop.repository.kdr.delegates.metastore.KDBRMetaStoreAttribute;
import org.apache.hop.repository.kdr.delegates.metastore.KDBRMetaStoreElement;
import org.apache.hop.repository.kdr.delegates.metastore.KDBRMetaStoreElementType;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;

public class HopDatabaseRepositoryMetaStoreDelegate extends HopDatabaseRepositoryBaseDelegate {

  public HopDatabaseRepositoryMetaStoreDelegate( HopDatabaseRepository repository ) {
    super( repository );
  }

  /**
   * Retrieve the ID for a namespace
   *
   * @param namespace The namespace to look up
   * @return the ID of the namespace
   * @throws HopException
   */
  public synchronized LongObjectId getNamespaceId( String namespace ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_NAMESPACE ),
      quote( HopDatabaseRepository.FIELD_NAMESPACE_ID_NAMESPACE ),
      quote( HopDatabaseRepository.FIELD_NAMESPACE_NAME ), namespace );
  }

  public ObjectId verifyNamespace( String namespace ) throws MetaStoreException {
    try {
      ObjectId namespaceId = getNamespaceId( namespace );
      if ( namespaceId == null ) {
        throw new MetaStoreException( "Unable to find namespace with name '" + namespace + "'" );
      }
      return namespaceId;
    } catch ( HopException e ) {
      throw new MetaStoreException( "Unable to get id of namespace '" + namespace + "'", e );
    }
  }

  public synchronized LongObjectId getElementTypeId( LongObjectId namespaceId, String elementTypeName ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_TYPE ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_NAME ), elementTypeName,
      new String[] { quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_NAMESPACE ), },
      new ObjectId[] { namespaceId, } );
  }

  public synchronized LongObjectId getElementId( LongObjectId elementTypeId, String elementName ) throws HopException {
    return repository.connectionDelegate.getIDWithValue(
      quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_NAME ), elementName,
      new String[] { quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT_TYPE ), },
      new ObjectId[] { elementTypeId, } );
  }

  public RowMetaAndData getElementType( LongObjectId elementTypeId ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_TYPE ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ), elementTypeId );
  }

  public RowMetaAndData getElement( LongObjectId elementId ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT ), elementId );
  }

  public RowMetaAndData getElementAttribute( LongObjectId elementAttributeId ) throws HopException {
    return repository.connectionDelegate.getOneRow(
      quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_ATTRIBUTE ),
      quote( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE ), elementAttributeId );
  }

  public Collection<RowMetaAndData> getNamespaces() throws HopDatabaseException, HopValueException {
    List<RowMetaAndData> attrs = new ArrayList<RowMetaAndData>();

    String sql = "SELECT * FROM " + quoteTable( HopDatabaseRepository.TABLE_R_NAMESPACE );

    List<Object[]> rows = repository.connectionDelegate.getRows( sql, 0 );
    for ( Object[] row : rows ) {
      RowMetaAndData rowWithMeta = new RowMetaAndData( repository.connectionDelegate.getReturnRowMeta(), row );
      long id = rowWithMeta.getInteger( quote( HopDatabaseRepository.FIELD_NAMESPACE_ID_NAMESPACE ), 0 );
      if ( id > 0 ) {
        attrs.add( rowWithMeta );
      }
    }
    return attrs;
  }

  public Collection<RowMetaAndData> getElementTypes( LongObjectId namespaceId ) throws HopDatabaseException,
    HopValueException {
    List<RowMetaAndData> attrs = new ArrayList<RowMetaAndData>();

    String sql =
      "SELECT * FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_TYPE ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_NAMESPACE ) + " = " + namespaceId.getId();

    List<Object[]> rows = repository.connectionDelegate.getRows( sql, 0 );
    for ( Object[] row : rows ) {
      RowMetaAndData rowWithMeta = new RowMetaAndData( repository.connectionDelegate.getReturnRowMeta(), row );
      long id = rowWithMeta.getInteger( quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ), 0 );
      if ( id > 0 ) {
        attrs.add( rowWithMeta );
      }
    }
    return attrs;
  }

  public Collection<RowMetaAndData> getElements( LongObjectId elementTypeId ) throws HopDatabaseException,
    HopValueException {
    List<RowMetaAndData> attrs = new ArrayList<RowMetaAndData>();

    String sql =
      "SELECT * FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT_TYPE ) + " = " + elementTypeId.getId();

    List<Object[]> rows = repository.connectionDelegate.getRows( sql, 0 );
    for ( Object[] row : rows ) {
      RowMetaAndData rowWithMeta = new RowMetaAndData( repository.connectionDelegate.getReturnRowMeta(), row );
      long id = rowWithMeta.getInteger( quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT_TYPE ), 0 );
      if ( id > 0 ) {
        attrs.add( rowWithMeta );
      }
    }
    return attrs;
  }

  public Collection<RowMetaAndData> getElementAttributes( ObjectId elementId, ObjectId parentAttributeId ) throws HopDatabaseException, HopValueException {
    List<RowMetaAndData> attrs = new ArrayList<RowMetaAndData>();

    String sql =
      "SELECT * FROM "
        + quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_ATTRIBUTE ) + " WHERE "
        + quote( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT ) + " = " + elementId.getId();
    if ( parentAttributeId != null ) {
      sql +=
        " AND "
          + quote( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE_PARENT ) + " = "
          + parentAttributeId.getId();
    }

    List<Object[]> rows = repository.connectionDelegate.getRows( sql, 0 );
    for ( Object[] row : rows ) {
      RowMetaAndData rowWithMeta = new RowMetaAndData( repository.connectionDelegate.getReturnRowMeta(), row );
      long id =
        rowWithMeta.getInteger(
          quote( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE ), 0 );
      if ( id > 0 ) {
        attrs.add( rowWithMeta );
      }
    }
    return attrs;
  }

  public ObjectId insertNamespace( String namespace ) throws HopException {
    ObjectId idNamespace =
      repository.connectionDelegate.getNextID(
        quoteTable( HopDatabaseRepository.TABLE_R_NAMESPACE ),
        quote( HopDatabaseRepository.FIELD_NAMESPACE_ID_NAMESPACE ) );
    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_NAMESPACE_ID_NAMESPACE ), idNamespace );
    table.addValue(
      new ValueMetaString( HopDatabaseRepository.FIELD_NAMESPACE_NAME ), namespace );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_NAMESPACE );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    if ( log.isDebug() ) {
      log.logDebug( "Saved namespace [" + namespace + "]" );
    }

    return idNamespace;
  }

  public ObjectId insertElementType( KDBRMetaStoreElementType type ) throws HopException {
    ObjectId idType =
      repository.connectionDelegate.getNextID(
        quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_TYPE ),
        quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ) );
    RowMetaAndData table = new RowMetaAndData();

    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ), idType );
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_NAMESPACE ), type
      .getNamespaceId() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_NAME ), type.getName() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_DESCRIPTION ), type
      .getDescription() );

    repository.connectionDelegate.getDatabase().prepareInsert(
      table.getRowMeta(), HopDatabaseRepository.TABLE_R_ELEMENT_TYPE );
    repository.connectionDelegate.getDatabase().setValuesInsert( table );
    repository.connectionDelegate.getDatabase().insertRow();
    repository.connectionDelegate.getDatabase().closeInsert();

    type.setId( new LongObjectId( idType ) );

    if ( log.isDebug() ) {
      log.logDebug( "Saved element type [" + type.getName() + "]" );
    }

    return idType;
  }

  public ObjectId updateElementType( ObjectId namespaceId, ObjectId elementTypeId, IMetaStoreElementType type ) throws HopException {

    RowMetaAndData table = new RowMetaAndData();

    table.addValue(
      new ValueMetaInteger(
        HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ),
      elementTypeId );
    table.addValue( new ValueMetaInteger(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_NAMESPACE ), namespaceId );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_NAME ), type.getName() );
    table.addValue( new ValueMetaString(
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_DESCRIPTION ), type
      .getDescription() );

    repository.connectionDelegate.updateTableRow(
      HopDatabaseRepository.TABLE_R_ELEMENT_TYPE,
      HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE, table );

    if ( log.isDebug() ) {
      log.logDebug( "Updated element type [" + type.getName() + "]" );
    }

    return elementTypeId;
  }

  public KDBRMetaStoreElementType parseElementType( String namespace, ObjectId namespaceId,
    RowMetaAndData elementTypeRow ) throws HopValueException {

    Long id = elementTypeRow.getInteger( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE );
    String name = elementTypeRow.getString( HopDatabaseRepository.FIELD_ELEMENT_TYPE_NAME, null );
    String description = elementTypeRow.getString( HopDatabaseRepository.FIELD_ELEMENT_TYPE_DESCRIPTION, null );

    KDBRMetaStoreElementType type = new KDBRMetaStoreElementType( this, namespace, namespaceId, name, description );
    type.setId( new LongObjectId( id ) );

    return type;
  }

  public void deleteElementType( ObjectId elementTypeId ) throws HopException {
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_TYPE ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_ELEMENT_TYPE_ID_ELEMENT_TYPE ) + " = ? ", elementTypeId );
  }

  public void deleteNamespace( ObjectId namespaceId ) throws HopException {
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_NAMESPACE ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_NAMESPACE_ID_NAMESPACE ) + " = ? ", namespaceId );
  }

  public KDBRMetaStoreElement parseElement( IMetaStoreElementType elementType, RowMetaAndData elementRow ) throws HopException {

    Long elementId = elementRow.getInteger( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT );
    String name = elementRow.getString( HopDatabaseRepository.FIELD_ELEMENT_NAME, null );

    KDBRMetaStoreElement element = new KDBRMetaStoreElement( this, elementType, Long.toString( elementId ), null );
    element.setName( name );

    // Now load the attributes...
    //
    addAttributes( element, new LongObjectId( elementId ), new LongObjectId( 0 ) );

    return element;
  }

  private void addAttributes( IMetaStoreAttribute parentAttribute, ObjectId elementId,
    LongObjectId parentAttributeId ) throws HopException {

    Collection<RowMetaAndData> attributeRows = getElementAttributes( elementId, parentAttributeId );
    for ( RowMetaAndData attributeRow : attributeRows ) {
      KDBRMetaStoreAttribute attribute = parseAttribute( elementId, attributeRow );
      parentAttribute.addChild( attribute );

      // See if this attribute has children...
      addAttributes( attribute, elementId, attribute.getObjectId() );
    }
  }

  private KDBRMetaStoreAttribute parseAttribute( ObjectId elementId, RowMetaAndData attributeRow ) throws HopException {
    try {
      Long attributeId =
        attributeRow.getInteger( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE );
      String key = attributeRow.getString( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_KEY, null );
      String value = attributeRow.getString( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_VALUE, null );

      Object object = parseAttributeValue( value );

      KDBRMetaStoreAttribute attribute = new KDBRMetaStoreAttribute( this, key, object );
      attribute.setObjectId( new LongObjectId( attributeId ) );
      return attribute;
    } catch ( Exception e ) {
      throw new HopException( "Unable to parse attribute from attribute row: " + attributeRow.toString(), e );
    }

  }

  /**
   * supported types:
   * <p/>
   * <pre>
   * S : String (java.lang.String)
   * D : Date (java.util.Date)
   * N : Number (Double)
   * I : Integer (Long)
   * B : Boolean (Boolean)
   *
   * Examples
   * S:Pentaho Data Integration
   * D:2013/08/23 17:25:00
   * N:1039348.9347
   * I:12345678
   * B:true
   * </pre>
   *
   * @param value
   * @return The native value
   * @throws Exception in case of conversion trouble
   */
  public Object parseAttributeValue( String value ) throws Exception {
    if ( Utils.isEmpty( value ) || value.length() < 3 ) {
      return null;
    }
    String valueString = value.substring( 2 );
    char type = value.charAt( 0 );
    switch ( type ) {
      case 'S':
        return valueString;
      case 'T':
        return new SimpleTimestampFormat( ValueMetaBase.DEFAULT_TIMESTAMP_FORMAT_MASK ).parse( valueString );
      case 'D':
        return new SimpleDateFormat( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK ).parse( valueString );
      case 'N':
        return Double.valueOf( valueString );
      case 'I':
        return Long.valueOf( valueString );
      case 'B':
        return "true".equalsIgnoreCase( valueString ) || "y".equalsIgnoreCase( valueString );
      default:
        throw new HopException( "Unknown data type : " + type );
    }
  }

  public String encodeAttributeValue( Object object ) throws Exception {
    if ( object == null ) {
      return "";
    }
    if ( object instanceof String ) {
      return "S:" + object.toString();
    }
    if ( object instanceof Timestamp ) {
      return "T:" + new SimpleTimestampFormat(
        ValueMetaBase.DEFAULT_TIMESTAMP_FORMAT_MASK ).format( (Timestamp) object );
    }
    if ( object instanceof Date ) {
      return "D:" + new SimpleDateFormat( ValueMetaBase.DEFAULT_DATE_FORMAT_MASK ).format( (Date) object );
    }
    if ( object instanceof Double ) {
      return "N:" + Double.toString( (Double) object );
    }
    if ( object instanceof Long ) {
      return "I:" + Long.toString( (Long) object );
    }
    if ( object instanceof Boolean ) {
      return "B:" + ( ( (Boolean) object ) ? "true" : "false" );
    }

    throw new HopException( "Can't encode object of class : " + object.getClass().getName() );
  }

  public ObjectId insertElement( IMetaStoreElementType elementType, IMetaStoreElement element ) throws MetaStoreException {
    try {

      LongObjectId elementId =
        repository.connectionDelegate.getNextID(
          quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT ),
          quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT ) );
      RowMetaAndData table = new RowMetaAndData();

      table.addValue( new ValueMetaInteger(
        HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT ), elementId
        .longValue() );
      table.addValue( new ValueMetaInteger(
        HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT_TYPE ), Long
        .valueOf( elementType.getId() ) );
      table.addValue(
        new ValueMetaString( HopDatabaseRepository.FIELD_ELEMENT_NAME ), element
          .getName() );

      repository.connectionDelegate.getDatabase().prepareInsert(
        table.getRowMeta(), HopDatabaseRepository.TABLE_R_ELEMENT );
      repository.connectionDelegate.getDatabase().setValuesInsert( table );
      repository.connectionDelegate.getDatabase().insertRow();
      repository.connectionDelegate.getDatabase().closeInsert();

      element.setId( elementId.toString() );

      // Now save the attributes
      //
      insertAttributes( element.getChildren(), elementId, new LongObjectId( 0L ) );

      if ( log.isDebug() ) {
        log.logDebug( "Saved element with name [" + element.getName() + "]" );
      }

      return elementId;
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to create new element with name '" + element.getName() + "'", e );
    }
  }

  private void insertAttributes( List<IMetaStoreAttribute> children, LongObjectId elementId,
    LongObjectId parentAttributeId ) throws Exception {
    for ( IMetaStoreAttribute child : children ) {
      LongObjectId attributeId =
        repository.connectionDelegate.getNextID(
          quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_ATTRIBUTE ),
          quote( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE ) );
      RowMetaAndData table = new RowMetaAndData();

      table.addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE ), attributeId.longValue() );
      table.addValue(
        new ValueMetaInteger(
          HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT ),
        elementId.longValue() );
      table.addValue( new ValueMetaInteger(
        HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT_ATTRIBUTE_PARENT ), parentAttributeId.longValue() );
      table.addValue( new ValueMetaString(
        HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_KEY ), child.getId() );
      table.addValue(
        new ValueMetaString( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_VALUE ),
        encodeAttributeValue( child.getValue() ) );

      repository.connectionDelegate.getDatabase().prepareInsert(
        table.getRowMeta(), HopDatabaseRepository.TABLE_R_ELEMENT_ATTRIBUTE );
      repository.connectionDelegate.getDatabase().setValuesInsert( table );
      repository.connectionDelegate.getDatabase().insertRow();
      repository.connectionDelegate.getDatabase().closeInsert();

      child.setId( attributeId.toString() );
    }
  }

  public void deleteElement( ObjectId elementId ) throws HopException {
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT_ATTRIBUTE ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_ELEMENT_ATTRIBUTE_ID_ELEMENT ) + " = ? ", elementId );
    repository.connectionDelegate.performDelete( "DELETE FROM "
      + quoteTable( HopDatabaseRepository.TABLE_R_ELEMENT ) + " WHERE "
      + quote( HopDatabaseRepository.FIELD_ELEMENT_ID_ELEMENT ) + " = ? ", elementId );
  }

}

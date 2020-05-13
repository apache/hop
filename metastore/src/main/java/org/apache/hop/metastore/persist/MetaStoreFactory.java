/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.metastore.persist;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.util.MetaStoreUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetaStoreFactory<T> {

  private enum AttributeType {
    STRING, INTEGER, LONG, DATE, BOOLEAN, LIST, MAP, NAME_REFERENCE, FILENAME_REFERENCE, FACTORY_NAME_REFERENCE, ENUM, POJO;
  }

  private static final String OBJECT_FACTORY_CONTEXT = "_ObjectFactoryContext_";
  private static final String POJO_CHILD = "_POJO_";

  protected IMetaStore metaStore;
  protected final Class<T> clazz;

  protected Map<String, List<?>> nameListMap;
  protected Map<String, MetaStoreFactory<?>> nameFactoryMap;
  protected Map<String, List<?>> filenameListMap;

  protected IMetaStoreObjectFactory objectFactory;

  private volatile SimpleDateFormat DATE_FORMAT = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );

  public MetaStoreFactory( Class<T> clazz, IMetaStore metaStore ) {
    this.metaStore = metaStore;
    this.clazz = clazz;
    nameListMap = new HashMap<>();
    filenameListMap = new HashMap<>();
    nameFactoryMap = new HashMap<>();
  }

  public void addNameList( String nameListKey, List<?> nameList ) {
    nameListMap.put( nameListKey, nameList );
  }

  public void addNameFactory( String nameFactoryKey, MetaStoreFactory<?> factory ) {
    nameFactoryMap.put( nameFactoryKey, factory );
  }

  public void addFilenameList( String filenameListKey, List<?> filenameList ) {
    filenameListMap.put( filenameListKey, filenameList );
  }

  /**
   * Load an element from the metastore, straight into the appropriate class
   */
  public T loadElement( String name ) throws MetaStoreException {

    if ( name == null || name.length() == 0 ) {
      throw new MetaStoreException( "You need to specify the name of an element to load" );
    }

    MetaStoreElementType elementTypeAnnotation = getElementTypeAnnotation();

    IMetaStoreElementType elementType = metaStore.getElementTypeByName( elementTypeAnnotation.name() );
    if ( elementType == null ) {
      return null;
    }

    IMetaStoreElement element = metaStore.getElementByName( elementType, name );
    if ( element == null ) {
      return null;
    }
    return loadElement( element );
  }

  /**
   * See if the element with the given name exists
   *
   * @param name The element to look up
   * @return true if the element exists, false if it doesn't.
   * @throws MetaStoreException TODO: optimize this in IMetaStore itself by adding an elementExists() option there.  This is faster than actually trying to load the whole element.
   */
  public boolean elementExists( String name ) throws MetaStoreException {
    return loadElement( name ) != null;
  }

  /**
   * Load an element from the metastore, straight into the appropriate class
   */
  private T loadElement( IMetaStoreElement element ) throws MetaStoreException {
    T object;

    try {
      object = clazz.newInstance();
    } catch ( Exception e ) {
      throw new MetaStoreException( "Class " + clazz.getName() + " could not be instantiated. Make sure the empty constructor is present", e );
    }

    // Set the name of the object...
    //
    setAttributeValue( clazz, object, "name", "setName", String.class, element.getName() );

    loadAttributes( object, element, clazz );
    return object;
  }

  /**
   * Get all declared fields from the given class, also the ones from all super classes
   *
   * @param parentClass
   * @return A unqiue list of fields.
   */
  private List<Field> findDeclaredFields( Class<?> parentClass ) {
    Set<Field> fields = new HashSet<>();

    for ( Field field : parentClass.getDeclaredFields() ) {
      fields.add( field );
    }
    Class<?> superClass = parentClass.getSuperclass();
    while ( superClass != null ) {
      for ( Field field : superClass.getDeclaredFields() ) {
        fields.add( field );
      }

      superClass = superClass.getSuperclass();
    }

    return new ArrayList<>( fields );
  }

  @SuppressWarnings( { "rawtypes", "unchecked" } )
  private void loadAttributes( Object parentObject, IMetaStoreAttribute parentElement, Class<?> parentClass ) throws MetaStoreException {

    // Which are the attributes to load?
    //
    List<Field> fields = findDeclaredFields( parentClass );
    for ( Field field : fields ) {
      MetaStoreAttribute attributeAnnotation = field.getAnnotation( MetaStoreAttribute.class );
      if ( attributeAnnotation != null ) {
        String key = attributeAnnotation.key();
        if ( key == null || key.length() == 0 ) {
          key = field.getName();
        }

        AttributeType type = determineAttributeType( field, attributeAnnotation );
        IMetaStoreAttribute child = parentElement.getChild( key );
        if ( child == null ) {
          for ( String mappedKey : MetaStoreKeyMap.get( key ) ) {
            child = parentElement.getChild( mappedKey );
            if ( child != null ) {
              break;
            }
          }
        }
        if ( child != null && ( child.getValue() != null || !child.getChildren().isEmpty() ) ) {
          String setterName = getSetterMethodName( field.getName() );
          String childValue = MetaStoreUtil.getAttributeString( child );
          if ( attributeAnnotation.password() ) {
            childValue = metaStore.getTwoWayPasswordEncoder().decode( childValue );
          }
          switch ( type ) {
            case STRING:
              setAttributeValue( parentClass, parentObject, field.getName(), setterName, String.class, childValue );
              break;
            case INTEGER:
              setAttributeValue( parentClass, parentObject, field.getName(), setterName, int.class, Integer.valueOf( childValue ) );
              break;
            case LONG:
              setAttributeValue( parentClass, parentObject, field.getName(), setterName, long.class, Long.valueOf( childValue ) );
              break;
            case BOOLEAN:
              setAttributeValue( parentClass, parentObject, field.getName(), setterName, boolean.class, "Y".equalsIgnoreCase( childValue ) );
              break;
            case ENUM:
              Enum<?> enumValue = null;
              final Class<? extends Enum> enumClass = (Class<? extends Enum>) field.getType();
              if ( childValue != null && childValue.length() > 0 ) {
                enumValue = Enum.valueOf( enumClass, childValue );
              }
              setAttributeValue( parentClass, parentObject, field.getName(), setterName, field.getType(), enumValue );
              break;
            case DATE:
              try {
                synchronized ( DATE_FORMAT ) {
                  Date date = childValue == null ? null : DATE_FORMAT.parse( childValue );
                  setAttributeValue( parentClass, parentObject, field.getName(), setterName, Date.class, date );
                }
              } catch ( Exception e ) {
                throw new MetaStoreException( "Unexpected date parsing problem with value: '" + childValue + "'", e );
              }
              break;
            case LIST:
              loadAttributesList( parentClass, parentObject, field, child );
              break;
            case MAP:
              loadAttributesMap( parentClass, parentObject, field, child );
              break;
            case NAME_REFERENCE:
              loadNameReference( parentClass, parentObject, field, child, attributeAnnotation );
              break;
            case FACTORY_NAME_REFERENCE:
              Object object = loadFactoryNameReference( parentClass, parentObject, field, child, attributeAnnotation );
              setAttributeValue( parentClass, parentObject, field.getName(), getSetterMethodName( field.getName() ), field.getType(), object );
              break;
            case FILENAME_REFERENCE:
              loadFilenameReference( parentClass, parentObject, field, child, attributeAnnotation );
              break;
            case POJO:
              Object pojo = loadPojo( parentClass, parentObject, field, child, attributeAnnotation );
              setAttributeValue( parentClass, parentObject, field.getName(), setterName, field.getType(), pojo );
              break;
            default:
              throw new MetaStoreException( "Only String values are supported at this time" );
          }
        }
      }
    }
  }

  private Object loadPojo( Class<?> parentClass, Object parentObject, Field field, IMetaStoreAttribute child, MetaStoreAttribute attributeAnnotation ) throws MetaStoreException {

    // There are 2 possible attributes in the child attribute: the object factory and/or the pojo top level attributes
    // If there is no pojo attribute it means the value of the object was null when save so we can stop if that's the case.
    //
    String pojoChildClassName;
    IMetaStoreAttribute pojoChild = child.getChild( POJO_CHILD );
    if ( pojoChild == null ) {
      // Support legacy code (backwards compatibility)
      pojoChildClassName = MetaStoreUtil.getAttributeString( child );
      pojoChild = child;
    } else {
      pojoChildClassName = pojoChild.getValue().toString();
    }

    if ( pojoChildClassName == null ) {
      // Nothing to load, move along
      return null;
    }

    try {
      Class<?> pojoClass;
      Object pojoObject;
      if ( objectFactory == null ) {
        pojoClass = clazz.getClassLoader().loadClass( pojoChildClassName );
        pojoObject = pojoClass.newInstance();
      } else {
        Map<String, String> objectFactoryContext = getObjectFactoryContext( child );
        pojoObject = objectFactory.instantiateClass( pojoChildClassName, objectFactoryContext, parentObject );
        pojoClass = pojoObject.getClass();
      }
      loadAttributes( pojoObject, pojoChild, pojoClass );

      return pojoObject;
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to load POJO class " + pojoChildClassName + " in parent class: " + parentClass, e );
    }
  }

  /**
   * There's an attribute in the parentElement called OBJECT_FACTORY_CONTEXT which contains a set of key/value pair attributes which we'll simply read and pass back.
   *
   * @param parentElement the parent element to read the object factory context from
   * @return
   */
  private Map<String, String> getObjectFactoryContext( IMetaStoreAttribute parentElement ) {
    Map<String, String> context = new HashMap<>();

    if ( parentElement != null ) {
      IMetaStoreAttribute contextChild = parentElement.getChild( OBJECT_FACTORY_CONTEXT );
      if ( contextChild != null ) {
        for ( IMetaStoreAttribute child : contextChild.getChildren() ) {
          if ( child.getId() != null && child.getValue() != null ) {
            context.put( child.getId(), child.getValue().toString() );
          }
        }
      }
    }

    return context;
  }

  /**
   * Save contextual information about an object from an object factory
   *
   * @param parentElement
   * @param context
   * @throws MetaStoreException
   */
  private void saveObjectFactoryContext( IMetaStoreAttribute parentElement, Map<String, String> context ) throws MetaStoreException {
    if ( context == null || context.isEmpty() ) {
      return;
    }

    IMetaStoreAttribute contextAttribute = metaStore.newAttribute( OBJECT_FACTORY_CONTEXT, null );
    parentElement.addChild( contextAttribute );

    for ( String key : context.keySet() ) {
      IMetaStoreAttribute attribute = metaStore.newAttribute( key, context.get( key ) );
      contextAttribute.addChild( attribute );
    }
  }

  private void loadAttributesList( Class<?> parentClass, Object parentObject, Field field, IMetaStoreAttribute parentElement ) throws MetaStoreException {
    try {

      if ( parentElement.getValue() == null ) {
        // nothing more to do, no elements saved
        return;
      }

      MetaStoreAttribute metaStoreAttribute = field.getAnnotation( MetaStoreAttribute.class );

      // What is the list object to populate?
      //
      String listGetter = getGetterMethodName( field.getName(), false );
      Method listGetMethod = parentClass.getMethod( listGetter );
      @SuppressWarnings( "unchecked" )
      List<Object> list = (List<Object>) listGetMethod.invoke( parentObject );
      if ( list == null ) {
        throw new MetaStoreException( "List attribute '" + field.getName() + "' in class '" + parentClass.getName() + "' is not pre-initialized. It will not be possible to add values" );
      }
      String childClassName = parentElement.getValue().toString();

      List<IMetaStoreAttribute> children = parentElement.getChildren();
      for ( int i = 0; i < children.size(); i++ ) {
        IMetaStoreAttribute child = parentElement.getChild( Integer.toString( i ) );
        if ( child == null ) {
          continue; // skip, go to the next child
        }
        // Instantiate the class and load the attributes
        //

        if ( metaStoreAttribute != null && metaStoreAttribute.factoryNameReference() ) {
          // Name reference to another factory OR locally embedded POJO
          //
          Object object = loadFactoryNameReference( parentClass, parentObject, field, child, metaStoreAttribute );
          if ( object != null ) {
            list.add( object );
          }
        } else if ( childClassName.equals( String.class.getName() ) ) {
          // String lists are a special case
          //
          String value = (String) child.getValue();
          if ( value != null ) {
            list.add( value );
          }
        } else {
          Class<?> childClass;
          Object childObject;
          if ( objectFactory == null ) {
            childClass = clazz.getClassLoader().loadClass( childClassName );
            childObject = childClass.newInstance();
          } else {
            Map<String, String> context = getObjectFactoryContext( child );
            childObject = objectFactory.instantiateClass( childClassName, context, parentObject );
            childClass = childObject.getClass();
          }

          loadAttributes( childObject, child, childClass );
          list.add( childObject );
        }
      }
    } catch ( Exception e ) {
      e.printStackTrace();
      throw new MetaStoreException( "Unable to load list attribute for field '" + field.getName() + "'", e );
    }

  }

  private void loadAttributesMap( Class<?> parentClass, Object parentObject, Field field, IMetaStoreAttribute parentElement ) throws MetaStoreException {
    try {

      if ( parentElement.getChildren() == null || parentElement.getChildren().isEmpty() ) {
        // nothing more to do, no elements saved
        return;
      }

      MetaStoreAttribute metaStoreAttribute = field.getAnnotation( MetaStoreAttribute.class );

      // What is the Map object to populate?
      //
      String listGetter = getGetterMethodName( field.getName(), false );
      Method listGetMethod = parentClass.getMethod( listGetter );
      @SuppressWarnings( "unchecked" )
      Map<String, String> map = (Map<String, String>) listGetMethod.invoke( parentObject );
      if ( map == null ) {
        throw new MetaStoreException( "Map attribute '" + field.getName() + "' in class '" + parentClass.getName() + "' is not pre-initialized. It will not be possible to add values" );
      }
      List<IMetaStoreAttribute> children = parentElement.getChildren();
      for ( int i = 0; i < children.size(); i++ ) {
        IMetaStoreAttribute child = children.get(i);
        if ( child == null ) {
          continue; // skip, go to the next child
        }
        // Instantiate the class and load the attributes
        //
        Object childValue = child.getValue();

        String key = child.getId();
        String value = childValue==null ? null : childValue.toString();

        map.put(key, value);
      }
    } catch ( Exception e ) {
      e.printStackTrace();
      throw new MetaStoreException( "Unable to load Map<String,String> attribute for field '" + field.getName() + "'", e );
    }

  }

  private void loadNameReference( Class<?> parentClass, Object parentObject, Field field, IMetaStoreAttribute parentElement, MetaStoreAttribute attributeAnnotation ) throws MetaStoreException {
    try {

      if ( parentElement.getValue() == null ) {
        // nothing more to do, no elements saved
        return;
      }

      // What is the name stored?
      //
      String name = parentElement.getValue().toString();
      if ( name.length() == 0 ) {
        // No name, no game
        return;
      }
      // What is the reference list to look up in?
      //
      List<?> list = nameListMap.get( attributeAnnotation.nameListKey() );
      if ( list == null ) {
        // No reference list, developer didn't provide a list!
        //
        throw new MetaStoreException( "Unable to find reference list for named objects with key '" + attributeAnnotation.nameListKey() + "', name reference '" + name + "' can not be looked up" );
      }

      for ( Object object : list ) {
        String verifyName = (String) object.getClass().getMethod( "getName" ).invoke( object );
        if ( verifyName.equals( name ) ) {
          // This is the object we want to set on the parent object...
          // Ex: setDatabaseMeta(), setNameElement()
          //
          String setter = getSetterMethodName( field.getName() );
          Method setterMethod = parentObject.getClass().getMethod( setter, object.getClass() );
          setterMethod.invoke( parentObject, object );
          break;
        }
      }
    } catch ( Exception e ) {
      throw new MetaStoreException( "Error lookup up reference for field '" + field.getName() + "'", e );
    }
  }

  private Object loadFactoryNameReference( Class<?> parentClass, Object parentObject, Field field, IMetaStoreAttribute parentElement, MetaStoreAttribute attributeAnnotation ) throws
    MetaStoreException {
    try {

      if ( parentElement.getValue() == null ) {
        // nothing more to do, no elements saved
        return null;
      }

      // What is the name stored?
      //
      String name = parentElement.getValue().toString();

      // See if the object is optionally shared
      //
      IMetaStoreAttribute pojoChild = parentElement.getChild( POJO_CHILD );
      if ( pojoChild != null ) {
        // Simply load POJO and set the name on the object...
        //
        Object pojo = loadPojo( parentClass, parentObject, field, parentElement, attributeAnnotation );

        // The name is not saved automatically but we have it...
        //
        if ( pojo != null ) {
          setAttributeValue( pojo.getClass(), pojo, "name", "setName", String.class, name );
        }
        return pojo;
      }

      // Simple named reference to a shared element
      //
      if ( name == null || name.length() == 0 ) {
        // No name, no reference to be retrieved.
        return null;
      }

      // What is the reference list to look up in?
      //
      MetaStoreFactory<?> factory = nameFactoryMap.get( attributeAnnotation.factoryNameKey() );
      if ( factory == null ) {
        // No reference list, developer didn't provide a list!
        //
        throw new MetaStoreException( "Unable to find factory to load attribute for factory key '" + attributeAnnotation.factoryNameKey() + "', name reference '" + name + "' can not be looked up" );
      }

      Object object = factory.loadElement( name );

      return object;
    } catch ( Exception e ) {
      throw new MetaStoreException( "Error lookup up reference for field '" + field.getName() + "'", e );
    }
  }

  private void loadFilenameReference( Class<?> parentClass, Object parentObject, Field field, IMetaStoreAttribute parentElement, MetaStoreAttribute attributeAnnotation ) throws MetaStoreException {
    try {

      if ( parentElement.getValue() == null ) {
        // nothing more to do, no elements saved
        return;
      }

      // What is the filename stored?
      //
      String filename = parentElement.getValue().toString();
      if ( filename.length() == 0 ) {
        // No name, no game
        return;
      }
      // What is the reference list to look up in?
      //
      List<?> list = filenameListMap.get( attributeAnnotation.filenameListKey() );
      if ( list == null ) {
        // No reference list, developer didn't provide a list!
        //
        throw new MetaStoreException(
          "Unable to find reference list for named objects with key '" + attributeAnnotation.filenameListKey() + "', name reference '" + filename + "' can not be looked up" );
      }

      for ( Object object : list ) {
        Method getNameMethod = object.getClass().getMethod( "getFilename" );
        String verifyName = (String) getNameMethod.invoke( object );
        if ( verifyName.equals( filename ) ) {
          // This is the object we want to set on the parent object...
          // Ex: setDatabaseMeta(), setNameElement()
          //
          String setter = getSetterMethodName( field.getName() );
          Method setterMethod = parentObject.getClass().getMethod( setter, object.getClass() );
          setterMethod.invoke( parentObject, object );
          break;
        }
      }
    } catch ( Exception e ) {
      throw new MetaStoreException( "Error lookup up reference for field '" + field.getName() + "'", e );
    }
  }

  /**
   * Save the specified class into the metastore.
   * Create the element type if needed...
   *
   * @param t The element to store...
   * @throws MetaStoreException
   */
  public void saveElement( T t ) throws MetaStoreException {

    MetaStoreElementType elementTypeAnnotation = getElementTypeAnnotation();

    // Make sure the element type exists...
    //
    IMetaStoreElementType elementType = metaStore.getElementTypeByName( elementTypeAnnotation.name() );
    if ( elementType == null ) {
      elementType = metaStore.newElementType();
      elementType.setName( elementTypeAnnotation.name() );
      elementType.setDescription( elementTypeAnnotation.description() );
      metaStore.createElementType( elementType );
    }

    // Now store the element itself
    // Verify if this is an update or a create...
    //

    String name = (String) getAttributeValue( clazz, t, "name", "getName" );
    if ( name == null || name.trim().length() == 0 ) {
      throw new MetaStoreException( "Unable to find name of element class object '" + t.toString() + "'" );
    }

    IMetaStoreElement element = metaStore.newElement();
    element.setName( name );
    element.setElementType( elementType );

    // Store the attributes
    //
    saveAttributes( element, clazz, t );

    // Now that we have the element populated, do a quick check to see if we need to update the element
    // or simply create a new element in the metastore.

    IMetaStoreElement existingElement = metaStore.getElementByName( elementType, name );
    if ( existingElement == null ) {
      metaStore.createElement( elementType, element );
    } else {
      metaStore.updateElement( elementType, existingElement.getId(), element );
    }
  }

  private void saveAttributes( IMetaStoreAttribute parentElement, Class<?> parentClass, Object parentObject ) throws MetaStoreException {
    try {
      List<Field> fields = findDeclaredFields( parentClass );
      for ( Field field : fields ) {
        MetaStoreAttribute attributeAnnotation = field.getAnnotation( MetaStoreAttribute.class );
        if ( attributeAnnotation != null ) {
          String key = attributeAnnotation.key();
          if ( key == null || key.length() == 0 ) {
            key = field.getName();
          }

          AttributeType type = determineAttributeType( field, attributeAnnotation );

          IMetaStoreAttribute child;
          switch ( type ) {
            case STRING:
              String value = (String) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
              if ( attributeAnnotation.password() ) {
                value = metaStore.getTwoWayPasswordEncoder().encode( value );
              }
              child = metaStore.newAttribute( key, value );
              parentElement.addChild( child );
              break;
            case INTEGER:
              int intValue = (Integer) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
              child = metaStore.newAttribute( key, Integer.toString( intValue ) );
              parentElement.addChild( child );
              break;
            case LONG:
              long longValue = (Long) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
              child = metaStore.newAttribute( key, Long.toString( longValue ) );
              parentElement.addChild( child );
              break;
            case BOOLEAN:
              boolean boolValue = (Boolean) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), true ) );
              child = metaStore.newAttribute( key, boolValue ? "Y" : "N" );
              parentElement.addChild( child );
              break;
            case ENUM:
              Object enumValue = getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
              String name = null;
              if ( enumValue != null ) {
                name = (String) getAttributeValue( Enum.class, enumValue, field.getName(), "name" );
              }
              child = metaStore.newAttribute( key, name );
              parentElement.addChild( child );
              break;
            case DATE:
              Date dateValue = (Date) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
              child = metaStore.newAttribute( key, dateValue == null ? null : DATE_FORMAT.format( dateValue ) );
              parentElement.addChild( child );
              break;
            case LIST:
              saveListAttribute( parentClass, parentElement, parentObject, field, key );
              break;
            case MAP:
              saveMapAttribute( parentClass, parentElement, parentObject, field, key );
              break;
            case NAME_REFERENCE:
              saveNameReference( parentClass, parentElement, parentObject, field, key );
              break;
            case FACTORY_NAME_REFERENCE:
              saveFactoryNameReference( parentClass, parentElement, parentObject, field, key );
              break;
            case FILENAME_REFERENCE:
              saveFilenameReference( parentClass, parentElement, parentObject, field, key );
              break;
            case POJO:
              // Create a new empty child element in the parent as a placeholder...
              //
              IMetaStoreAttribute pojoChild = metaStore.newAttribute( key, null );
              parentElement.addChild( pojoChild );

              // Save the POJO and the context in this child element
              //
              savePojo( parentClass, pojoChild, parentObject, field );
              break;
            default:
              throw new MetaStoreException( "Only String values are supported at this time" );
          }

          // TODO: support other field data types...

        }
      }
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to save attributes of element id '" + parentElement.getId() + "', class " + parentClass.getName(), e );
    }
  }

  @SuppressWarnings( "unchecked" )
  private void saveListAttribute( Class<?> parentClass, IMetaStoreAttribute parentElement, Object parentObject, Field field, String key ) throws MetaStoreException {
    List<Object> list = (List<Object>) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
    IMetaStoreAttribute topChild = metaStore.newAttribute( key, null );
    parentElement.addChild( topChild );
    MetaStoreAttribute metaStoreAttribute = field.getAnnotation( MetaStoreAttribute.class );

    if ( !list.isEmpty() ) {
      // Save the class name used as well, otherwise we can't re-inflate afterwards...
      //
      Class<?> attributeClass = list.get( 0 ).getClass();
      topChild.setValue( attributeClass.getName() );

      // Add one child to the topChild for each object in the list...

      for ( int i = 0; i < list.size(); i++ ) {
        Object object = list.get( i );

        IMetaStoreAttribute childAttribute = metaStore.newAttribute( Integer.toString( i ), null );
        topChild.addChild( childAttribute );

        if ( metaStoreAttribute != null && metaStoreAttribute.factoryNameReference() ) {
          // Is this a list of factory name references?
          //
          saveFactoryNameReference( parentClass, childAttribute, parentObject, field, object );

        } else if ( object instanceof String ) {
          // STRING
          //
          childAttribute.setValue( object );
        } else {
          // POJO
          //
          // See if we need to store additional information about this class
          //
          if ( objectFactory != null ) {
            Map<String, String> context = objectFactory.getContext( object );
            saveObjectFactoryContext( childAttribute, context );
          }
          saveAttributes( childAttribute, attributeClass, object );
        }
      }
    }
  }


  private void saveMapAttribute( Class<?> parentClass, IMetaStoreAttribute parentElement, Object parentObject, Field field, String key ) throws MetaStoreException {
    Map<String, String> map = (Map<String,String>) getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
    IMetaStoreAttribute topChild = metaStore.newAttribute( key, null );
    parentElement.addChild( topChild );
    MetaStoreAttribute metaStoreAttribute = field.getAnnotation( MetaStoreAttribute.class );

    if ( !map.isEmpty() ) {
      // We always assume it's these are String pairs
      //
      for ( String mapKey : map.keySet() ) {
        String mapValue = map.get(mapKey);
        IMetaStoreAttribute childAttribute = metaStore.newAttribute( mapKey, mapValue );
        topChild.addChild( childAttribute );
      }
    }
  }

  private void saveNameReference( Class<?> parentClass, IMetaStoreAttribute parentElement, Object parentObject, Field field, String key ) throws MetaStoreException {
    // What is the object of which we need to store the name as a reference?
    //
    Object namedObject = getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
    String name = null;
    if ( namedObject != null ) {
      name = (String) getAttributeValue( namedObject.getClass(), namedObject, "name", "getName" );
    }
    IMetaStoreAttribute nameChild = metaStore.newAttribute( key, name );
    parentElement.addChild( nameChild );
  }

  private void saveFactoryNameReference( Class<?> parentClass, IMetaStoreAttribute parentElement, Object parentObject, Field field, String key ) throws MetaStoreException {

    // What is the object of which we need to store the name as a reference?
    //
    Object namedObject = getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
    if ( namedObject == null ) {
      // Nothing to see here, move along.
      return;
    }

    IMetaStoreAttribute refChild = metaStore.newAttribute( key, null );
    parentElement.addChild( refChild );
    saveFactoryNameReference( parentClass, refChild, parentObject, field, namedObject );

  }

  /**
   * Save a name reference and save the referenced object in the specified target element
   *
   * @param parentClass
   * @param targetElement
   * @param parentObject
   * @param field
   * @param namedObject
   * @throws MetaStoreException
   */
  private void saveFactoryNameReference( Class<?> parentClass, IMetaStoreAttribute targetElement, Object parentObject, Field field, Object namedObject ) throws MetaStoreException {
    Class<?> namedObjectClass = namedObject.getClass();

    // What's the name of this named object?
    //
    String name = (String) getAttributeValue( namedObject.getClass(), namedObject, "name", "getName" );
    targetElement.setValue( name );

    // Do we need to store this named object locally or use a factory to store it centrally?
    //
    String indicatorName = field.getAnnotation( MetaStoreAttribute.class ).factorySharedIndicatorName();
    if ( indicatorName != null && indicatorName.length() > 0 ) {
      // True : shared
      // False : local embedding of attributes
      //
      String isSharedMethod = getGetterMethodName( indicatorName, true );
      Boolean shared = (Boolean) getAttributeValue( namedObjectClass, namedObject, indicatorName, isSharedMethod );
      if ( shared == null ) {
        throw new MetaStoreException( "Shared indicator attribute is not available through '" + namedObjectClass.getName() + "." + isSharedMethod + "()'" );
      }
      if ( !shared ) {
        // Save the complete POJO, not just the name reference.
        savePojo( parentClass, targetElement, parentObject, namedObject );
        return;
      }
    }

    String factoryNameKey = field.getAnnotation( MetaStoreAttribute.class ).factoryNameKey();
    MetaStoreFactory<?> factory = nameFactoryMap.get( factoryNameKey );

    try {
      Method method = factory.getClass().getMethod( "saveElement", Object.class );
      method.invoke( factory, namedObject );
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to save attribute element of class " + namedObject.getClass() + " in metastore", e );
    }
  }

  private void savePojo( Class<?> parentClass, IMetaStoreAttribute pojoElement, Object parentObject, Field field ) throws MetaStoreException {
    Object pojo = getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
    savePojo( parentClass, pojoElement, parentObject, pojo );
  }

  private void savePojo( Class<?> parentClass, IMetaStoreAttribute pojoElement, Object parentObject, Object pojo ) throws MetaStoreException {

    if ( pojo == null ) {
      // Nothing to save here, move along.
      return;
    }

    // See if we need to store additional factory information about this object
    //
    if ( objectFactory != null ) {
      Map<String, String> context = objectFactory.getContext( pojo );
      saveObjectFactoryContext( pojoElement, context );
    }

    // Add all the pojo attributes in a special child element...
    //
    IMetaStoreAttribute pojoChild = metaStore.newAttribute( POJO_CHILD, pojo.getClass().getName() );
    pojoElement.addChild( pojoChild );
    saveAttributes( pojoChild, pojo.getClass(), pojo );
  }

  private void saveFilenameReference( Class<?> parentClass, IMetaStoreAttribute parentElement, Object parentObject, Field field, String key ) throws MetaStoreException {
    // What is the object of which we need to store the filename as a reference?
    //
    Object namedObject = getAttributeValue( parentClass, parentObject, field.getName(), getGetterMethodName( field.getName(), false ) );
    String name = null;
    if ( namedObject != null ) {
      name = (String) getAttributeValue( namedObject.getClass(), namedObject, "filename", "getFilename" );
    }
    IMetaStoreAttribute nameChild = metaStore.newAttribute( key, name );
    parentElement.addChild( nameChild );
  }

  /**
   * @return A list of all the de-serialized objects of this class in the metastore
   * @throws MetaStoreException
   */
  public List<T> getElements() throws MetaStoreException {

    MetaStoreElementType elementTypeAnnotation = getElementTypeAnnotation();

    IMetaStoreElementType elementType = metaStore.getElementTypeByName( elementTypeAnnotation.name() );
    if ( elementType == null ) {
      return Collections.emptyList();
    }

    List<IMetaStoreElement> elements = metaStore.getElements( elementType );
    List<T> list = new ArrayList<T>( elements.size() );
    for ( IMetaStoreElement metaStoreElement : elements ) {
      list.add( loadElement( metaStoreElement ) );
    }
    return list;
  }

  /**
   * Remove an element with a specific name from the metastore
   *
   * @param name The name of the element to delete
   * @throws MetaStoreException In case either the element type or the element to delete doesn't exists
   */
  public T deleteElement( String name ) throws MetaStoreException {
    MetaStoreElementType elementTypeAnnotation = getElementTypeAnnotation();

    IMetaStoreElementType elementType = metaStore.getElementTypeByName( elementTypeAnnotation.name() );
    if ( elementType == null ) {
      throw new MetaStoreException( "The element type '" + elementTypeAnnotation.name() + "' does not exist so the element with name '" + name + "' can not be deleted" );
    }

    T element = loadElement( name );
    if ( element == null ) {
      throw new MetaStoreException( "The element with name '" + name + "' does not exists so it can not be deleted" );
    }

    metaStore.deleteElement( elementType, name );

    return element;
  }

  /**
   * @return The list of element names
   * @throws MetaStoreException
   */
  public List<String> getElementNames() throws MetaStoreException {
    List<String> names = new ArrayList<>();

    MetaStoreElementType elementTypeAnnotation = getElementTypeAnnotation();

    IMetaStoreElementType elementType = metaStore.getElementTypeByName( elementTypeAnnotation.name() );
    if ( elementType == null ) {
      return names;
    }

    List<IMetaStoreElement> elements = metaStore.getElements( elementType );
    for ( IMetaStoreElement element : elements ) {
      names.add( element.getName() );
    }

    return names;
  }

  /**
   * @return The {@link IMetaStoreElementType} to reference in the {@link IMetaStore} API.
   * @throws MetaStoreException
   */
  public IMetaStoreElementType getElementType() throws MetaStoreException {
    MetaStoreElementType elementTypeAnnotation = getElementTypeAnnotation();
    return metaStore.getElementTypeByName( elementTypeAnnotation.name() );
  }

  private AttributeType determineAttributeType( Field field, MetaStoreAttribute annotation ) throws MetaStoreException {
    Class<?> fieldClass = field.getType();
    if ( List.class.equals( fieldClass ) ) {
      return AttributeType.LIST;
    }
    if ( Map.class.equals( fieldClass )) {
      return AttributeType.MAP;
    }
    if ( annotation.nameReference() ) {
      return AttributeType.NAME_REFERENCE;
    }
    if ( annotation.filenameReference() ) {
      return AttributeType.FILENAME_REFERENCE;
    }
    if ( annotation.factoryNameReference() ) {
      return AttributeType.FACTORY_NAME_REFERENCE;
    }
    if ( String.class.equals( fieldClass ) ) {
      return AttributeType.STRING;
    }
    if ( int.class.equals( fieldClass ) ) {
      return AttributeType.INTEGER;
    }
    if ( long.class.equals( fieldClass ) ) {
      return AttributeType.LONG;
    }
    if ( Date.class.equals( fieldClass ) ) {
      return AttributeType.DATE;
    }
    if ( boolean.class.equals( fieldClass ) ) {
      return AttributeType.BOOLEAN;
    }
    if ( fieldClass.isEnum() ) {
      return AttributeType.ENUM;
    }
    return AttributeType.POJO;

    // throw new MetaStoreException( "Unable to recognize attribute type for class '" + fieldClass + "'" );
  }

  private MetaStoreElementType getElementTypeAnnotation() throws MetaStoreException {
    MetaStoreElementType elementTypeAnnotation = clazz.getAnnotation( MetaStoreElementType.class );
    if ( elementTypeAnnotation == null ) {
      throw new MetaStoreException( "The class you want to serialize needs to have the @MetaStoreElementType annotation" );
    }
    return elementTypeAnnotation;
  }

  /**
   * Set an attribute value in the specified object
   *
   * @param parentClass The parent object class
   * @param object      The object to modify
   * @param fieldName   The field to modify
   * @param setterName  The setter method name
   * @param valueClass  The class value
   * @param value       The value to set
   * @throws MetaStoreException
   */
  private void setAttributeValue( Class<?> parentClass, Object object, String fieldName, String setterName, Class<?> valueClass, Object value ) throws MetaStoreException {
    Method method;
    try {
      method = parentClass.getMethod( setterName, valueClass );
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to find setter for attribute field : " + fieldName + ". Expected '" + setterName + "'", e );
    }

    try {
      method.invoke( object, value );
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to set value '" + value + "' using method '" + setterName + "'", e );
    }
  }

  private Object getAttributeValue( Class<?> parentClass, Object object, String fieldName, String getterName ) throws MetaStoreException {
    Method method;
    try {
      method = parentClass.getMethod( getterName );
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to find getter for attribute field : " + fieldName + ". Expected '" + getterName + "'", e );
    }

    try {
      Object value = method.invoke( object );
      return value;
    } catch ( Exception e ) {
      throw new MetaStoreException( "Unable to get value using method '" + getterName + "' on class " + parentClass.getName(), e );
    }

  }

  /**
   * myAttribute ==>  setMyAttribute
   */
  private String getSetterMethodName( String name ) {

    StringBuilder setter = new StringBuilder();
    setter.append( "set" );
    setter.append( name.substring( 0, 1 ).toUpperCase() );
    setter.append( name.substring( 1 ) );

    return setter.toString();
  }

  /**
   * myAttribute ==>  getMyAttribute
   */
  private String getGetterMethodName( String name, boolean isBoolean ) {

    StringBuilder setter = new StringBuilder();
    setter.append( isBoolean ? "is" : "get" );
    setter.append( name.substring( 0, 1 ).toUpperCase() );
    setter.append( name.substring( 1 ) );

    return setter.toString();
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public Map<String, List<?>> getNamedListMap() {
    return nameListMap;
  }

  public void setNamedListMap( Map<String, List<?>> namedListMap ) {
    this.nameListMap = namedListMap;
  }

  public Map<String, List<?>> getNameListMap() {
    return nameListMap;
  }

  public void setNameListMap( Map<String, List<?>> nameListMap ) {
    this.nameListMap = nameListMap;
  }

  public Map<String, List<?>> getFilenameListMap() {
    return filenameListMap;
  }

  public void setFilenameListMap( Map<String, List<?>> filenameListMap ) {
    this.filenameListMap = filenameListMap;
  }

  /**
   * @return the objectFactory
   */
  public IMetaStoreObjectFactory getObjectFactory() {
    return objectFactory;
  }

  /**
   * @param objectFactory the objectFactory to set
   */
  public void setObjectFactory( IMetaStoreObjectFactory objectFactory ) {
    this.objectFactory = objectFactory;
  }

}

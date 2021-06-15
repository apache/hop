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

package org.apache.hop.metadata.serializer.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonMetadataParser<T extends IHopMetadata> {

  private Class<T> managedClass;
  private IHopMetadataProvider metadataProvider;

  public JsonMetadataParser( Class<T> managedClass, IHopMetadataProvider metadataProvider ) {
    this.managedClass = managedClass;
    this.metadataProvider = metadataProvider;
  }

  public T loadJsonObject( Class<T> managedClass, JsonParser jsonParser ) throws HopException {
    try {
      // Now we can load the annotated fields, the properties:
      //
      T object = managedClass.newInstance();
      loadProperties( object, jsonParser );
      return object;
    } catch(Exception e) {
      throw new HopException("Unable to load JSON object", e);
    }
  }

  private void loadProperties( Object object, com.fasterxml.jackson.core.JsonParser jsonParser ) throws HopException {
    Class<?> objectClass = object.getClass();
    Map<String, Field> keyFieldMap = new HashMap<>();
    for ( Field field : ReflectionUtil.findAllFields( objectClass ) ) {
      HopMetadataProperty metadataProperty = field.getAnnotation( HopMetadataProperty.class );
      if ( metadataProperty != null ) {
        String key;
        if ( StringUtils.isNotEmpty( metadataProperty.key() ) ) {
          key = metadataProperty.key();
        } else {
          key = field.getName();
        }
        keyFieldMap.put( key, field );
      }
    }

    try {
      while ( jsonParser.nextToken() != JsonToken.END_OBJECT ) {
        String key = jsonParser.getCurrentName();
        Field field = keyFieldMap.get( key );
        if ( field != null ) {
          // This is a recognized piece of data. We can load this...
          //
          loadProperty( object, jsonParser, key, field );
        }

      }
    } catch ( Exception e ) {
      throw new HopException( "Error loading fields for object class " + objectClass.getName(), e );
    }
  }

  private void loadProperty( Object object, com.fasterxml.jackson.core.JsonParser jsonParser, String key, Field field ) throws HopException {
    Class<?> objectClass = object.getClass();
    Class<?> fieldType = field.getType();
    HopMetadataProperty metadataProperty = field.getAnnotation( HopMetadataProperty.class );

    try {
      // Position on the value in the JSON
      //
      jsonParser.nextToken();
      Object fieldValue = null;

      if ("null".equals(jsonParser.getText()) && jsonParser.getValueAsString() == null) {
        // This is the case { "name" : null }
        //
        fieldValue=null;
      } else {
        if ( fieldType.isEnum() ) {
          final Class<? extends Enum> enumerationClass = (Class<? extends Enum>) field.getType();
          String enumerationName = jsonParser.getText();
          if ( StringUtils.isNotEmpty( enumerationName ) ) {
            fieldValue = Enum.valueOf( enumerationClass, enumerationName );
          }
        } else if ( String.class.equals( fieldType ) ) {
          String string = jsonParser.getText();
          if ( metadataProperty.password() ) {
            string = metadataProvider.getTwoWayPasswordEncoder().decode( string, true );
          }
          fieldValue = string;
        } else if ( int.class.equals( fieldType ) || Integer.class.equals( fieldType ) ) {
          fieldValue = jsonParser.getIntValue();
        } else if ( long.class.equals( fieldType ) || Long.class.equals( fieldType ) ) {
          fieldValue = jsonParser.getLongValue();
        } else if ( Boolean.class.equals( fieldType ) || boolean.class.equals( fieldType ) ) {
          fieldValue = jsonParser.getBooleanValue();
        } else if ( Date.class.equals( fieldType ) ) {
          String dateString = jsonParser.getText();
          fieldValue = new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).parse( dateString );
        } else if ( Map.class.equals( fieldType ) ) {
          Map<String, String> map = new HashMap<>();
          while ( jsonParser.nextToken() != JsonToken.END_OBJECT ) {
            String mapKey = jsonParser.getText();
            jsonParser.nextToken();
            String mapValue = jsonParser.getText();
            map.put( mapKey, mapValue );
          }
          fieldValue = map;
        } else if ( List.class.equals( fieldType ) ) {
          ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
          Class<?> listClass = (Class<?>) parameterizedType.getActualTypeArguments()[ 0 ];
          if ( String.class.equals( listClass ) ) {
            List<String> list = new ArrayList<>();
            while ( jsonParser.nextToken() != JsonToken.END_ARRAY ) {
              list.add( jsonParser.getText() );
            }
            fieldValue = list;
          } else {
            // List of POJO
            //
            IHopMetadataSerializer<?> serializer = null;
            if ( metadataProperty.storeWithName() ) {
              if ( !IHopMetadata.class.isAssignableFrom( listClass ) ) {
                throw new HopException( "Error: metadata objects that need to be stored with a name reference need to implement IHopMetadata: " + listClass.getName() );
              }
              serializer = metadataProvider.getSerializer( (Class<? extends IHopMetadata>) listClass );
            }
            List list = new ArrayList<>();
            while ( jsonParser.nextToken() != JsonToken.END_ARRAY ) {
              if ( metadataProperty.storeWithName() ) {
                // Load by name reference
                //
                String name = jsonParser.getText();
                Object listObject = serializer.load( name );
                list.add( listObject );
              } else {
                // Load the object itself
                //
                Object listObject = loadPojoProperties( listClass, jsonParser );
                list.add( listObject );
              }
            }
            fieldValue = list;
          }
        } else {
          // POJO
          //
          if ( metadataProperty.storeWithName() ) {
            // Load using name reference
            //
            if ( !IHopMetadata.class.isAssignableFrom( fieldType ) ) {
              throw new HopException( "Error: metadata objects that need to be stored with a name reference need to implement IHopMetadata: " + fieldType.getName() );
            }
            IHopMetadataSerializer<?> serializer = metadataProvider.getSerializer( (Class<? extends IHopMetadata>) fieldType );
            String name = jsonParser.getText();
            fieldValue = serializer.load( name );
          } else {
            fieldValue = loadPojoProperties( fieldType, jsonParser );
          }
        }
      }

      // Set the value on the object...
      //
      ReflectionUtil.setFieldValue( object, field.getName(), fieldType, fieldValue );

    } catch ( Exception e ) {
      throw new HopException( "Error loading field with key '" + key + "' for field '" + field.getName() + " in class " + objectClass.getName(), e );
    }

  }

  private Object loadPojoProperties( Class<?> fieldType, com.fasterxml.jackson.core.JsonParser jsonParser ) throws HopException {
    try {
      Object fieldValue;
      HopMetadataObject hopMetadataObject = fieldType.getAnnotation( HopMetadataObject.class );
      if ( hopMetadataObject == null ) {
        fieldValue = fieldType.newInstance();
        loadProperties( fieldValue, jsonParser );
      } else {
        jsonParser.nextToken(); // skip {
        String fieldValueId = jsonParser.getText();
        IHopMetadataObjectFactory objectFactory = hopMetadataObject.objectFactory().newInstance();
        fieldValue = objectFactory.createObject( fieldValueId, null ); // No parent object
        loadProperties( fieldValue, jsonParser );
        jsonParser.nextToken(); // skip }
      }
      return fieldValue;
    } catch ( Exception e ) {
      throw new HopException( "Error loading POJO field '" + fieldType.getName() + "'", e );
    }
  }





  public JSONObject getJsonObject( T object ) throws HopException {
    JSONObject jObject = new JSONObject();
    saveProperties( jObject, object, managedClass );
    return jObject;
  }



  /**
   * Go over all the fields in the object class and see if there are with a HopMetadataProperty annotation...
   *
   * @param jObject
   * @param object
   */
  private void saveProperties( JSONObject jObject, Object object, Class<?> objectClass ) throws HopException {
    if ( object == null ) {
      return;
    }
    for ( Field objectField : ReflectionUtil.findAllFields( object.getClass() ) ) {
      HopMetadataProperty metadataProperty = objectField.getAnnotation( HopMetadataProperty.class );
      if ( metadataProperty != null ) {
        // The contents of this field needs to be serialized...
        //
        saveProperty( jObject, object, metadataProperty, objectField );
      }
    }
  }

  private void saveProperty( JSONObject jObject, Object object, HopMetadataProperty metadataProperty, Field objectField ) throws HopException {
    String key = objectField.getName();
    if ( StringUtils.isNotEmpty( metadataProperty.key() ) ) {
      key = metadataProperty.key();
    }
    Class<?> fieldType = objectField.getType();
    boolean isBoolean = Boolean.class.equals(fieldType) || boolean.class.equals( fieldType );

    try {
      Object fieldValue = ReflectionUtil.getFieldValue( object, objectField.getName(), isBoolean );
      if ( fieldValue == null ) {
        jObject.put( key, null );
      } else {
        // Enumeration?
        if ( fieldType.isEnum() ) {
          // Save the enum as its name
          jObject.put( key, ( (Enum) fieldValue ).name() );
        } else if ( String.class.equals( fieldType ) ) {
          String fieldStringValue = (String) fieldValue;
          if ( metadataProperty.password() ) {
            ITwoWayPasswordEncoder passwordEncoder = metadataProvider.getTwoWayPasswordEncoder();
            fieldStringValue = passwordEncoder.encode( fieldStringValue, true );
          }
          jObject.put( key, fieldStringValue );
        } else if ( int.class.equals( fieldType ) || Integer.class.equals( fieldType ) ) {
          jObject.put( key, fieldValue );
        } else if ( long.class.equals( fieldType ) || Long.class.equals( fieldType ) ) {
          jObject.put( key, fieldValue );
        } else if ( isBoolean ) {
          jObject.put( key, fieldValue );
        } else if ( Date.class.equals( fieldType ) ) {
          String dateString = new SimpleDateFormat( "yyyy/MM/dd'T'HH:mm:ss" ).format( (Date) fieldValue );
          jObject.put( key, dateString );
        } else if ( Map.class.equals( fieldType ) ) {
          jObject.put( key, new JSONObject( (Map) fieldValue ) );
        } else if ( List.class.equals( fieldType ) ) {
          JSONArray jListObjects = new JSONArray();
          ParameterizedType parameterizedType = (ParameterizedType) objectField.getGenericType();
          Class<?> listClass = (Class<?>) parameterizedType.getActualTypeArguments()[ 0 ];

          List<?> fieldListObjects = (List<?>) fieldValue;
          for ( Object fieldListObject : fieldListObjects ) {
            if ( String.class.equals( listClass ) ) {
              jListObjects.add( fieldListObject );
            } else if ( metadataProperty.storeWithName() ) {
              String name = ReflectionUtil.getObjectName( fieldListObject );
              jListObjects.add( name );
            } else {
              JSONObject jListObject = savePojoProperty( key, fieldListObject, listClass );
              jListObjects.add( jListObject );
            }
          }
          jObject.put( key, jListObjects );
        } else {
          if ( metadataProperty.storeWithName() ) {
            // Just save the name
            String name = ReflectionUtil.getObjectName( fieldValue );
            jObject.put( key, name );
          } else {
            JSONObject jPojo = savePojoProperty( key, fieldValue, fieldType );
            jObject.put( key, jPojo );
          }
        }
      }
    } catch ( Exception e ) {
      throw new HopException( "Error serializing field '" + objectField.getName() + "' with type '" + fieldType.toString() + "'", e );
    }
  }

  private JSONObject savePojoProperty( String key, Object fieldValue, Class<?> fieldType ) throws HopException {
    try {

      // Check if we can serialize this POJO
      // We're looking for annotation @HopMetadataObject on the POJO class indicating how to instantiate it...
      // If not we assume it's in the current classpath.
      // If we don't find the annotation, we don't serialize
      //
      JSONObject jPojoObject = new JSONObject();

      // We can just serialize this POJO just like any other object with properties...
      //
      saveProperties( jPojoObject, fieldValue, fieldType );

      HopMetadataObject hopMetadataObject = fieldType.getAnnotation( HopMetadataObject.class );
      if ( hopMetadataObject == null ) {
        return jPojoObject;
      } else {
        IHopMetadataObjectFactory objectFactory = hopMetadataObject.objectFactory().newInstance();
        String fieldValueId = objectFactory.getObjectId( fieldValue );

        // We need to store the object ID (plugin ID, class name, ...)
        // To prevent re-ordering by JSON formatters (or humans) we use the ID as the key for a new JSON block
        // We'll wrap the POJO JSON in that block
        //
        JSONObject jPojoBlock = new JSONObject();

        // The POJO JSON goes into the block
        //
        jPojoBlock.put( fieldValueId, jPojoObject );

        return jPojoBlock;
      }
    } catch ( Exception e ) {
      throw new HopException( "Error saving POJO field with key " + key + ", field type '" + fieldType.getName() + "'", e );
    }
  }






  /**
   * Gets provider
   *
   * @return value of provider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The provider to set
   */
  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }


}

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

package org.apache.hop.core.util;

import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

@Deprecated
/**
 * See {@link org.apache.hop.core.util.serialization.BaseSerializingMeta for an alternative. }
 */
public class SerializationHelper {

  private static final String INDENT_STRING = "    ";

  /**
   * This method will perform the work that used to be done by hand in each hop input meta for: readData(Node
   * transformNode). We handle all primitive types, complex user types, arrays, lists and any number of nested object levels,
   * via recursion of this method.
   *
   * @param object The object to be persisted
   * @param node   The node to 'attach' our XML to
   */
  public static void read( Object object, Node node ) {
    // get this classes fields, public, private, protected, package, everything
    Field[] fields = object.getClass().getFields();

    for ( Field field : fields ) {

      // ignore fields which are final, static or transient
      if ( Modifier.isFinal( field.getModifiers() )
        || Modifier.isStatic( field.getModifiers() ) || Modifier.isTransient( field.getModifiers() ) ) {
        continue;
      }

      // if the field is not accessible (private), we'll open it up so we can operate on it
      if ( !field.isAccessible() ) {
        field.setAccessible( true );
      }

      // check if we're going to try to read an array
      if ( field.getType().isArray() ) {
        try {
          // get the node (if available) for the field
          Node fieldNode = XmlHandler.getSubNode( node, field.getName() );
          if ( fieldNode == null ) {
            // doesn't exist (this is possible if fields were empty/null when persisted)
            continue;
          }
          // get the Java classname for the array elements
          String fieldClassName = XmlHandler.getTagAttribute( fieldNode, "class" );
          Class<?> clazz = null;
          // primitive types require special handling
          if ( fieldClassName.equals( "boolean" ) ) {
            clazz = boolean.class;
          } else if ( fieldClassName.equals( "int" ) ) {
            clazz = int.class;
          } else if ( fieldClassName.equals( "float" ) ) {
            clazz = float.class;
          } else if ( fieldClassName.equals( "double" ) ) {
            clazz = double.class;
          } else if ( fieldClassName.equals( "long" ) ) {
            clazz = long.class;
          } else {
            // normal, non primitive array class
            clazz = Class.forName( fieldClassName );
          }
          // get the child nodes for the field
          NodeList childrenNodes = fieldNode.getChildNodes();

          // create a new, appropriately sized array
          int arrayLength = 0;
          for ( int i = 0; i < childrenNodes.getLength(); i++ ) {
            Node child = childrenNodes.item( i );
            // ignore TEXT_NODE, they'll cause us to have a larger count than reality, even if they are empty
            if ( child.getNodeType() != Node.TEXT_NODE ) {
              arrayLength++;
            }
          }
          // create a new instance of our array
          Object array = Array.newInstance( clazz, arrayLength );
          // set the new array on the field (on object, passed in)
          field.set( object, array );

          int arrayIndex = 0;
          for ( int i = 0; i < childrenNodes.getLength(); i++ ) {
            Node child = childrenNodes.item( i );
            if ( child.getNodeType() == Node.TEXT_NODE ) {
              continue;
            }

            // roll through all of our array elements setting them as encountered
            if ( String.class.isAssignableFrom( clazz ) || Number.class.isAssignableFrom( clazz ) ) {
              Constructor<?> constructor = clazz.getConstructor( String.class );
              Object instance = constructor.newInstance( XmlHandler.getTagAttribute( child, "value" ) );
              Array.set( array, arrayIndex++, instance );
            } else if ( Boolean.class.isAssignableFrom( clazz ) || boolean.class.isAssignableFrom( clazz ) ) {
              Object value = Boolean.valueOf( XmlHandler.getTagAttribute( child, "value" ) );
              Array.set( array, arrayIndex++, value );
            } else if ( Integer.class.isAssignableFrom( clazz ) || int.class.isAssignableFrom( clazz ) ) {
              Object value = Integer.valueOf( XmlHandler.getTagAttribute( child, "value" ) );
              Array.set( array, arrayIndex++, value );
            } else if ( Float.class.isAssignableFrom( clazz ) || float.class.isAssignableFrom( clazz ) ) {
              Object value = Float.valueOf( XmlHandler.getTagAttribute( child, "value" ) );
              Array.set( array, arrayIndex++, value );
            } else if ( Double.class.isAssignableFrom( clazz ) || double.class.isAssignableFrom( clazz ) ) {
              Object value = Double.valueOf( XmlHandler.getTagAttribute( child, "value" ) );
              Array.set( array, arrayIndex++, value );
            } else if ( Long.class.isAssignableFrom( clazz ) || long.class.isAssignableFrom( clazz ) ) {
              Object value = Long.valueOf( XmlHandler.getTagAttribute( child, "value" ) );
              Array.set( array, arrayIndex++, value );
            } else {
              // create an instance of 'fieldClassName'
              Object instance = clazz.newInstance();
              // add the instance to the array
              Array.set( array, arrayIndex++, instance );
              // read child, the same way as the parent
              read( instance, child );
            }
          }
        } catch ( Throwable t ) {
          t.printStackTrace();
          // TODO: log this
        }
      } else if ( List.class.isAssignableFrom( field.getType() ) ) {
        // handle lists
        try {
          // get the node (if available) for the field
          Node fieldNode = XmlHandler.getSubNode( node, field.getName() );
          if ( fieldNode == null ) {
            // doesn't exist (this is possible if fields were empty/null when persisted)
            continue;
          }
          // get the Java classname for the array elements
          String fieldClassName = XmlHandler.getTagAttribute( fieldNode, "class" );
          Class<?> clazz = Class.forName( fieldClassName );

          // create a new, appropriately sized array
          List<Object> list = new ArrayList<>();
          field.set( object, list );

          // iterate over all of the array elements and add them one by one as encountered
          NodeList childrenNodes = fieldNode.getChildNodes();
          for ( int i = 0; i < childrenNodes.getLength(); i++ ) {
            Node child = childrenNodes.item( i );
            if ( child.getNodeType() == Node.TEXT_NODE ) {
              continue;
            }

            // create an instance of 'fieldClassName'
            if ( String.class.isAssignableFrom( clazz )
              || Number.class.isAssignableFrom( clazz ) || Boolean.class.isAssignableFrom( clazz ) ) {
              Constructor<?> constructor = clazz.getConstructor( String.class );
              Object instance = constructor.newInstance( XmlHandler.getTagAttribute( child, "value" ) );
              list.add( instance );
            } else {
              // read child, the same way as the parent
              Object instance = clazz.newInstance();
              // add the instance to the array
              list.add( instance );
              read( instance, child );
            }
          }
        } catch ( Throwable t ) {
          t.printStackTrace();
          // TODO: log this
        }
      } else {
        // we're handling a regular field (not an array or list)
        try {
          Object value = XmlHandler.getTagValue( node, field.getName() );
          if ( value == null ) {
            continue;
          }
          if ( !( field.getType().isPrimitive() && "".equals( value ) ) ) {
            // skip setting of primitives if we see null
            if ( "".equals( value ) ) {
              field.set( object, value );
            } else if ( field.getType().isPrimitive() ) {
              // special primitive handling
              if ( double.class.isAssignableFrom( field.getType() ) ) {
                field.set( object, Double.parseDouble( value.toString() ) );
              } else if ( float.class.isAssignableFrom( field.getType() ) ) {
                field.set( object, Float.parseFloat( value.toString() ) );
              } else if ( long.class.isAssignableFrom( field.getType() ) ) {
                field.set( object, Long.parseLong( value.toString() ) );
              } else if ( int.class.isAssignableFrom( field.getType() ) ) {
                field.set( object, Integer.parseInt( value.toString() ) );
              } else if ( byte.class.isAssignableFrom( field.getType() ) ) {
                field.set( object, value.toString().getBytes() );
              } else if ( boolean.class.isAssignableFrom( field.getType() ) ) {
                field.set( object, "true".equalsIgnoreCase( value.toString() ) );
              }
            } else if ( String.class.isAssignableFrom( field.getType() )
              || Number.class.isAssignableFrom( field.getType() ) ) {
              Constructor<?> constructor = field.getType().getConstructor( String.class );
              Object instance = constructor.newInstance( value );
              field.set( object, instance );
            } else {
              // we don't know what we're handling, but we'll give it a shot
              Node fieldNode = XmlHandler.getSubNode( node, field.getName() );
              if ( fieldNode == null ) {
                // doesn't exist (this is possible if fields were empty/null when persisted)
                continue;
              }
              // get the Java classname for the array elements
              String fieldClassName = XmlHandler.getTagAttribute( fieldNode, "class" );
              Class<?> clazz = Class.forName( fieldClassName );
              Object instance = clazz.newInstance();
              field.set( object, instance );
              read( instance, fieldNode );
            }
          }
        } catch ( Throwable t ) {
          // TODO: log this
          t.printStackTrace();
        }
      }
    }
  }

  /**
   * This method will perform the work that used to be done by hand in each hop input meta for: getXml(). We handle
   * all primitive types, complex user types, arrays, lists and any number of nested object levels, via recursion of
   * this method.
   *
   * @param object
   * @param buffer
   */
  @SuppressWarnings( "unchecked" )
  public static void write( Object object, int indentLevel, StringBuilder buffer ) {

    // don't even attempt to persist
    if ( object == null ) {
      return;
    }

    // get this classes fields, public, private, protected, package, everything
    Field[] fields = object.getClass().getFields();
    for ( Field field : fields ) {

      // ignore fields which are final, static or transient
      if ( Modifier.isFinal( field.getModifiers() )
        || Modifier.isStatic( field.getModifiers() ) || Modifier.isTransient( field.getModifiers() ) ) {
        continue;
      }

      // if the field is not accessible (private), we'll open it up so we can operate on it
      if ( !field.isAccessible() ) {
        field.setAccessible( true );
      }

      try {
        Object fieldValue = field.get( object );
        // no value? null? skip it!
        if ( fieldValue == null || "".equals( fieldValue ) ) {

          continue;
        }
        if ( field.getType().isPrimitive()
          || String.class.isAssignableFrom( field.getType() ) || Number.class.isAssignableFrom( field.getType() ) ) {
          indent( buffer, indentLevel );
          buffer.append( XmlHandler.addTagValue( field.getName(), fieldValue.toString() ) );
        } else if ( field.getType().isArray() ) {
          // write array values
          int length = Array.getLength( fieldValue );

          // open node (add class name attribute)
          indent( buffer, indentLevel );
          buffer
            .append(
              "<" + field.getName() + " class=\"" + fieldValue.getClass().getComponentType().getName() + "\">" )
            .append( Const.CR );

          for ( int i = 0; i < length; i++ ) {
            Object childObject = Array.get( fieldValue, i );
            // handle all strings/numbers
            if ( String.class.isAssignableFrom( childObject.getClass() )
              || Number.class.isAssignableFrom( childObject.getClass() ) ) {
              indent( buffer, indentLevel + 1 );
              buffer.append( "<" ).append( fieldValue.getClass().getComponentType().getSimpleName() );
              buffer.append( " value=\"" + childObject.toString() + "\"/>" ).append( Const.CR );
            } else if ( Boolean.class.isAssignableFrom( childObject.getClass() )
              || boolean.class.isAssignableFrom( childObject.getClass() ) ) {
              // handle booleans (special case)
              indent( buffer, indentLevel + 1 );
              buffer.append( "<" ).append( fieldValue.getClass().getComponentType().getSimpleName() );
              buffer.append( " value=\"" + childObject.toString() + "\"/>" ).append( Const.CR );
            } else {
              // array element is a user defined/complex type, recurse into it
              indent( buffer, indentLevel + 1 );
              buffer.append( "<" + fieldValue.getClass().getComponentType().getSimpleName() + ">" ).append(
                Const.CR );
              write( childObject, indentLevel + 1, buffer );
              indent( buffer, indentLevel + 1 );
              buffer.append( "</" + fieldValue.getClass().getComponentType().getSimpleName() + ">" ).append(
                Const.CR );
            }
          }
          // close node
          buffer.append( "    </" + field.getName() + ">" ).append( Const.CR );
        } else if ( List.class.isAssignableFrom( field.getType() ) ) {
          // write list values
          List<Object> list = (List<Object>) fieldValue;
          if ( list.size() == 0 ) {
            continue;
          }
          Class<?> listClass = list.get( 0 ).getClass();

          // open node (add class name attribute)
          indent( buffer, indentLevel );
          buffer.append( "<" + field.getName() + " class=\"" + listClass.getName() + "\">" ).append( Const.CR );

          for ( Object childObject : list ) {
            // handle all strings/numbers
            if ( String.class.isAssignableFrom( childObject.getClass() )
              || Number.class.isAssignableFrom( childObject.getClass() ) ) {
              indent( buffer, indentLevel + 1 );
              buffer.append( "<" ).append( listClass.getSimpleName() );
              buffer.append( " value=\"" + childObject.toString() + "\"/>" ).append( Const.CR );
            } else if ( Boolean.class.isAssignableFrom( childObject.getClass() )
              || boolean.class.isAssignableFrom( childObject.getClass() ) ) {
              // handle booleans (special case)
              indent( buffer, indentLevel + 1 );
              buffer.append( "<" ).append( listClass.getSimpleName() );
              buffer.append( " value=\"" + childObject.toString() + "\"/>" ).append( Const.CR );
            } else {
              // array element is a user defined/complex type, recurse into it
              indent( buffer, indentLevel + 1 );
              buffer.append( "<" + listClass.getSimpleName() + ">" ).append( Const.CR );
              write( childObject, indentLevel + 1, buffer );
              indent( buffer, indentLevel + 1 );
              buffer.append( "</" + listClass.getSimpleName() + ">" ).append( Const.CR );
            }
          }
          // close node
          indent( buffer, indentLevel );
          buffer.append( "</" + field.getName() + ">" ).append( Const.CR );
        } else {
          // if we don't now what it is, let's treat it like a first class citizen and try to write it out
          // open node (add class name attribute)
          indent( buffer, indentLevel );
          buffer.append( "<" + field.getName() + " class=\"" + fieldValue.getClass().getName() + "\">" ).append(
            Const.CR );
          write( fieldValue, indentLevel + 1, buffer );
          // close node
          indent( buffer, indentLevel );
          buffer.append( "</" + field.getName() + ">" ).append( Const.CR );
        }
      } catch ( Throwable t ) {
        t.printStackTrace();
        // TODO: log this
      }
    }

  }

  private static void indent( StringBuilder sb, int indentLevel ) {
    for ( int i = 0; i < indentLevel; i++ ) {
      sb.append( INDENT_STRING );
    }
  }

}

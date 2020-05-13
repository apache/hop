package org.apache.hop.core.metastore;

import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.memory.MemoryMetaStore;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;

/**
 * This metastore implementation is an in-memory metastore which serializes using JSON.
 * In other words, JSON is read into memory as a MetaStore and then you can ask to serialize that information to and from JSON.
 */
public class SerializableMetaStore extends MemoryMetaStore implements IMetaStore {
  public SerializableMetaStore() {
    super();
  }

  /**
   * Create a copy of all elements in an existing metastore.
   *
   * @param source the source store to copy over
   */
  public SerializableMetaStore( IMetaStore source) throws MetaStoreException {
      List<IMetaStoreElementType> srcElementTypes = source.getElementTypes( );
      for (IMetaStoreElementType srcElementType : srcElementTypes) {

        IMetaStoreElementType elementType = newElementType();
        elementType.setName( srcElementType.getName() );
        elementType.setDescription( srcElementType.getDescription() );
        createElementType(elementType);

        List<IMetaStoreElement> srcElements = source.getElements( elementType );
        for (IMetaStoreElement srcElement : srcElements) {
          IMetaStoreElement element = newElement();
          element.setName( srcElement.getName() );
          element.setValue( srcElement.getValue() );

          copyChildren(srcElement, element);

          createElement(elementType, element);
        }
      }
  }

  private void copyChildren( IMetaStoreAttribute srcAttribute, IMetaStoreAttribute attribute ) throws MetaStoreException {
    List<IMetaStoreAttribute> srcChildren = srcAttribute.getChildren();
    for (IMetaStoreAttribute srcChild : srcChildren) {
      IMetaStoreAttribute child = newAttribute( srcChild.getId(), srcChild.getValue() );
      copyChildren(srcChild, child);
      attribute.addChild( child );
    }
  }

  public String toJson() throws MetaStoreException {

    JSONObject jStore = new JSONObject();

    // Metastore name and description
    //
    jStore.put("name", getName());
    jStore.put("description", getDescription());

    // The namespaces
    //
    JSONArray jTypes = new JSONArray ();
    jStore.put("types", jTypes);

    for ( IMetaStoreElementType elementType : getElementTypes()) {
      addElementType( jTypes, elementType);
    }

    return jStore.toJSONString();
  }

  public SerializableMetaStore(String storeJson) throws ParseException, MetaStoreException {
    this();
    JSONParser parser = new JSONParser();
    JSONObject jStore = (JSONObject) parser.parse( storeJson );

    setName((String)jStore.get( "name" ));
    setDescription((String)jStore.get( "description" ));

    JSONArray jTypes = (JSONArray) jStore.get("types");
    for (int t=0;t<jTypes.size();t++) {
      JSONObject jType = (JSONObject) jTypes.get( t );
      readElementType( jType );
    }
  }
  
  private void addElementType( JSONArray jElementTypes, IMetaStoreElementType elementType ) throws MetaStoreException {
    JSONObject jElementType = new JSONObject();
    jElementTypes.add(jElementType);

    jElementType.put("name", elementType.getName());
    jElementType.put("description", elementType.getDescription());

    JSONArray jElements = new JSONArray ();
    jElementType.put("elements", jElements);

    List<IMetaStoreElement> elements = getElements(elementType );
    for (IMetaStoreElement element : elements) {
      addElement(jElements,elementType, element);
    }
  }

  private void readElementType( JSONObject jType ) throws MetaStoreException {
    String name = (String) jType.get("name");
    String description = (String) jType.get("description");

    IMetaStoreElementType elementType = newElementType();
    elementType.setName( name );
    elementType.setDescription( description );
    createElementType(elementType );

    JSONArray jElements = (JSONArray) jType.get( "elements" );
    for (int e=0;e<jElements.size();e++) {
      JSONObject jElement = (JSONObject) jElements.get(e);
      readElement(elementType, jElement);
    }

  }

  private void addElement( JSONArray jElements, IMetaStoreElementType elementType, IMetaStoreElement element ) {
    JSONObject jElement = new JSONObject();
    jElements.add(jElement);

    jElement.put("id", element.getId());
    jElement.put("name", element.getName());
    jElement.put("value", element.getValue());

    JSONArray jChildren = new JSONArray();
    jElement.put("children", jChildren);
    List<IMetaStoreAttribute> children = element.getChildren();
    for (IMetaStoreAttribute child : children) {
      addChild(jChildren, child);
    }
  }

  private void readElement( IMetaStoreElementType elementType, JSONObject jElement ) throws MetaStoreException {

    IMetaStoreElement element = newElement();
    element.setName( (String) jElement.get("name") );

    readChild(element, jElement);

    createElement(elementType, element );
  }

  private void addChild( JSONArray jChildren, IMetaStoreAttribute child ) {
    JSONObject jChild = new JSONObject();
    jChildren.add(jChild);

    jChild.put("id", child.getId());
    jChild.put("value", child.getValue());
    JSONArray jSubChildren = new JSONArray();
    jChild.put("children", jSubChildren);
    List<IMetaStoreAttribute> subChildren = child.getChildren();
    for (IMetaStoreAttribute subChild : subChildren) {
      addChild(jSubChildren, subChild);
    }
  }

  private void readChild( IMetaStoreAttribute parent, JSONObject jChild ) throws MetaStoreException {
    parent.setId((String) jChild.get("id"));
    parent.setValue( jChild.get("value") );

    JSONArray jSubChildren = (JSONArray) jChild.get("children");
    for (int c=0;c<jSubChildren.size();c++) {
      JSONObject jSubChild = (JSONObject) jSubChildren.get( c );
      IMetaStoreAttribute subAttribute = newAttribute(null, null);
      readChild( subAttribute, jSubChild );
      parent.addChild( subAttribute );
    }
  }
}

package org.apache.hop.metadata.api;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

public class HopMetadataDefaultObjectFactory implements IHopMetadataObjectFactory {

  /**
   * By default we use the classname as the id.
   *
   * @param id The ID to use to create the object
   * @return
   * @throws HopException
   */
  @Override public Object createObject( String id, Object parentObject ) throws HopException {
    try {
      Object object = Class.forName( id );

      // By default, inherit variables from a parent object.
      //
      if (parentObject!=null) {
        if (parentObject instanceof IVariables ) {
          if (object instanceof IVariables) {
            ((IVariables)object).initializeVariablesFrom( (IVariables) parentObject );
          }
        }
      }

      return object;
    } catch(Exception e) {
      throw new HopException("Unable to create object for id : "+id, e);
    }
  }

  @Override public String getObjectId( Object object ) {
    return object.getClass().getName();
  }
}

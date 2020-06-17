package org.apache.hop.metadata.serializer.json.person.interest;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;

public class InterestObjectFactory implements IHopMetadataObjectFactory {

  @Override public Object createObject( String id, Object parentObject ) throws HopException {
    if ("cooking".equals( id )) {
      return new Cooking();
    }
    if ("running".equals( id )) {
      return new Running();
    }
    if ("music".equals( id )) {
      return new Music();
    }
    throw new HopException("Unable to recognize object ID "+id);
  }

  @Override public String getObjectId( Object object ) throws HopException {
    if (object instanceof Cooking) {
      return "cooking";
    }
    if (object instanceof Running) {
      return "running";
    }
    if (object instanceof Music) {
      return "music";
    }
    throw new HopException("Class "+object.getClass()+" could not be recognized");
  }
}

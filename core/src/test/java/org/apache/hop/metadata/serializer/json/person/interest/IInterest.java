package org.apache.hop.metadata.serializer.json.person.interest;

import org.apache.hop.metadata.api.HopMetadataObject;

@HopMetadataObject(
  objectFactory = InterestObjectFactory.class
)
public interface IInterest {
  String getName();
  String getDescription();
}

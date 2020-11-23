package org.apache.hop.pipeline.transforms.mongodbinput;

import org.apache.hop.mongo.wrapper.field.MongoField;

import java.util.List;

/** Created by brendan on 12/30/14. */
public interface DiscoverFieldsCallback {
  public void notifyFields(List<MongoField> fields);

  public void notifyException(Exception exception);
}

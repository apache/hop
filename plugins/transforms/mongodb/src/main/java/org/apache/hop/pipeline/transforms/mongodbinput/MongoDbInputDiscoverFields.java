package org.apache.hop.pipeline.transforms.mongodbinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.wrapper.field.MongoField;

import java.util.List;

/** Created by brendan on 11/4/14. */
public interface MongoDbInputDiscoverFields {
  public List<MongoField> discoverFields(
      MongoProperties.Builder properties,
      String db,
      String collection,
      String query,
      String fields,
      boolean isPipeline,
      int docsToSample,
      MongoDbInputMeta transform)
      throws HopException;

  public void discoverFields(
      MongoProperties.Builder properties,
      String db,
      String collection,
      String query,
      String fields,
      boolean isPipeline,
      int docsToSample,
      MongoDbInputMeta transform,
      DiscoverFieldsCallback discoverFieldsCallback)
      throws HopException;
}

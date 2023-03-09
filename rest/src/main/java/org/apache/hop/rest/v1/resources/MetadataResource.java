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
 *
 */

package org.apache.hop.rest.v1.resources;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.json.simple.JSONObject;

@Path("/metadata")
public class MetadataResource extends BaseResource {

  /**
   * List all the type keys
   *
   * @return A list with all the type keys in the metadata
   */
  @GET
  @Path("/types")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTypes() {
    List<String> types = new ArrayList<>();
    IHopMetadataProvider provider = hop.getMetadataProvider();
    List<Class<IHopMetadata>> metadataClasses = provider.getMetadataClasses();
    for (Class<IHopMetadata> metadataClass : metadataClasses) {
      HopMetadata metadata = metadataClass.getAnnotation(HopMetadata.class);
      types.add(metadata.key());
    }
    return Response.ok(types).build();
  }

  /**
   * List all the element names for a given type
   *
   * @param key the metadata key to use
   * @return A list with all the metadata element names
   * @throws HopException
   */
  @GET
  @Path("/list/{key}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listNames(@PathParam("key") String key) throws HopException {
    IHopMetadataProvider provider = hop.getMetadataProvider();
    Class<IHopMetadata> metadataClass = provider.getMetadataClassForKey(key);
    IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
    return Response.ok(serializer.listObjectNames()).build();
  }

  /**
   * Get a metadata element with a given type and name
   *
   * @param key The key of the metadata type
   * @param name The name to look up
   * @return The metadata element
   * @throws HopException
   */
  @GET
  @Path("/{key}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getElement(@PathParam("key") String key, @PathParam("name") String name)
      throws HopException {
    IHopMetadataProvider provider = hop.getMetadataProvider();
    Class<IHopMetadata> metadataClass = provider.getMetadataClassForKey(key);
    IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
    IHopMetadata metadata = serializer.load(name);
    // We want to serialize this exactly like we do on the filesystem to avoid confusion.
    //
    JsonMetadataParser<IHopMetadata> parser = new JsonMetadataParser<>(metadataClass, provider);
    JSONObject jsonObject = parser.getJsonObject(metadata);

    return Response.ok().entity(jsonObject.toJSONString()).build();
  }

  /**
   * Save a metadata element
   *
   * @param key
   * @param metadataJson
   * @return
   * @throws HopException
   */
  @POST
  @Path("/{key}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveElement(@PathParam("key") String key, String metadataJson)
      throws HopException {
    try {
      IHopMetadataProvider provider = hop.getMetadataProvider();
      Class<IHopMetadata> metadataClass = provider.getMetadataClassForKey(key);
      IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
      JsonMetadataParser<IHopMetadata> parser = new JsonMetadataParser<>(metadataClass, provider);
      JsonFactory jsonFactory = new JsonFactory();
      JsonParser jsonParser = jsonFactory.createParser(metadataJson);
      IHopMetadata metadata = parser.loadJsonObject(metadataClass, jsonParser);
      serializer.save(metadata);
      return Response.ok().entity(metadata.getName()).build();
    } catch (Exception e) {
      return getServerError("Error saving element of type " + key, e, true);
    }
  }

  /**
   * Save a metadata element
   *
   * @param key The key of the type of metadata to delete
   * @param elementName The name of the element to delete
   * @return
   * @throws HopException
   */
  @DELETE
  @Path("/{key}/{elementName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteElement(
      @PathParam("key") String key, @PathParam("elementName") String elementName)
      throws HopException {
    try {
      IHopMetadataProvider provider = hop.getMetadataProvider();
      Class<IHopMetadata> metadataClass = provider.getMetadataClassForKey(key);
      IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
      serializer.delete(elementName);
      return Response.ok().entity(elementName).build();
    } catch (Exception e) {
      return getServerError(
          "Error deleting element of type " + key + " with name " + elementName, e, true);
    }
  }
}

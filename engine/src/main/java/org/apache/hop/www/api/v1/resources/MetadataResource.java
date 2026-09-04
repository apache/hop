/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www.api.v1.resources;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import jakarta.ws.rs.Consumes;
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
import org.apache.hop.www.api.HopApiNotFoundException;
import org.json.simple.JSONObject;

/** Reads and writes the metadata of the Hop server this API runs on. */
@Path("/metadata")
public class MetadataResource extends BaseApiResource {

  /**
   * List all the metadata type keys.
   *
   * @return a list with all the type keys in the metadata
   */
  @GET
  @Path("/types")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTypes() {
    List<String> types = new ArrayList<>();
    IHopMetadataProvider provider = context.getMetadataProvider();
    for (Class<IHopMetadata> metadataClass : provider.getMetadataClasses()) {
      HopMetadata metadata = metadataClass.getAnnotation(HopMetadata.class);
      types.add(metadata.key());
    }
    return Response.ok(types).build();
  }

  /**
   * List all the element names for a given type.
   *
   * @param key the metadata key to use
   * @return a list with all the metadata element names
   */
  @GET
  @Path("/list/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listNames(@PathParam("key") String key) throws HopException {
    IHopMetadataProvider provider = context.getMetadataProvider();
    IHopMetadataSerializer<IHopMetadata> serializer = getSerializer(provider, key);
    return Response.ok(serializer.listObjectNames()).build();
  }

  /**
   * Get a metadata element with a given type and name.
   *
   * @param key the key of the metadata type
   * @param name the name to look up
   * @return the metadata element
   */
  @GET
  @Path("/{key}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getElement(@PathParam("key") String key, @PathParam("name") String name)
      throws HopException {
    IHopMetadataProvider provider = context.getMetadataProvider();
    Class<IHopMetadata> metadataClass = getMetadataClass(provider, key);
    IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
    IHopMetadata metadata = serializer.load(name);
    if (metadata == null) {
      throw new HopApiNotFoundException(
          "Unable to find metadata element '" + name + "' of type '" + key + "'");
    }

    // We want to serialize this exactly like we do on the filesystem to avoid confusion.
    //
    JsonMetadataParser<IHopMetadata> parser = new JsonMetadataParser<>(metadataClass, provider);
    JSONObject jsonObject = parser.getJsonObject(metadata);

    return Response.ok().entity(jsonObject.toJSONString()).build();
  }

  /**
   * Save a metadata element.
   *
   * @param key the key of the metadata type to save
   * @param metadataJson the element to save, serialized as JSON
   * @return the name of the element that was saved
   */
  @POST
  @Path("/{key}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response saveElement(@PathParam("key") String key, String metadataJson)
      throws HopException {
    try {
      IHopMetadataProvider provider = context.getMetadataProvider();
      Class<IHopMetadata> metadataClass = getMetadataClass(provider, key);
      IHopMetadataSerializer<IHopMetadata> serializer = provider.getSerializer(metadataClass);
      JsonMetadataParser<IHopMetadata> parser = new JsonMetadataParser<>(metadataClass, provider);
      JsonParser jsonParser = new JsonFactory().createParser(metadataJson);
      IHopMetadata metadata = parser.loadJsonObject(metadataClass, jsonParser);
      serializer.save(metadata);
      return Response.ok().entity(metadata.getName()).build();
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Error saving element of type " + key, e);
    }
  }

  /**
   * Delete a metadata element.
   *
   * @param key the key of the type of metadata to delete
   * @param name the name of the element to delete
   * @return the name of the element that was deleted
   */
  @DELETE
  @Path("/{key}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteElement(@PathParam("key") String key, @PathParam("name") String name)
      throws HopException {
    IHopMetadataProvider provider = context.getMetadataProvider();
    IHopMetadataSerializer<IHopMetadata> serializer = getSerializer(provider, key);
    serializer.delete(name);
    return Response.ok().entity(name).build();
  }

  private Class<IHopMetadata> getMetadataClass(IHopMetadataProvider provider, String key)
      throws HopException {
    Class<IHopMetadata> metadataClass;
    try {
      metadataClass = provider.getMetadataClassForKey(key);
    } catch (HopException e) {
      // An unknown key is a missing resource, not a server failure.
      throw new HopApiNotFoundException("Unable to find metadata type '" + key + "'");
    }
    if (metadataClass == null) {
      throw new HopApiNotFoundException("Unable to find metadata type '" + key + "'");
    }
    return metadataClass;
  }

  private IHopMetadataSerializer<IHopMetadata> getSerializer(
      IHopMetadataProvider provider, String key) throws HopException {
    return provider.getSerializer(getMetadataClass(provider, key));
  }
}

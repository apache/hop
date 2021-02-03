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
 *
 */

package org.apache.hop.metadata.serializer.multi;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public class MultiMetadataSerializer<T extends IHopMetadata> implements IHopMetadataSerializer<T> {

  protected MultiMetadataProvider multiProvider;
  protected Class<T> managedClass;
  protected IVariables variables;
  protected String description;

  public MultiMetadataSerializer(
      MultiMetadataProvider multiProvider,
      Class<T> managedClass,
      IVariables variables,
      String description) {
    this.multiProvider = multiProvider;
    this.managedClass = managedClass;
    this.variables = variables;
    this.description = description;
  }

  /**
   * Check the providers one at a time in reverse order. The first that has the object is the one
   * that will be taken. This gives the possibility to override existing objects in parent projects
   * with local versions.
   *
   * @param objectName The name of the object to load
   * @return The object or null if it couldn't be found
   * @throws HopException
   */
  @Override
  public T load(String objectName) throws HopException {

    List<IHopMetadataProvider> providers = multiProvider.getProviders();
    ListIterator<IHopMetadataProvider> providerIterator = providers.listIterator(providers.size());
    while (providerIterator.hasPrevious()) {
      IHopMetadataProvider provider = providerIterator.previous();
      IHopMetadataSerializer<T> serializer = provider.getSerializer(managedClass);
      if (serializer.exists(objectName)) {
        return serializer.load(objectName);
      }
    }

    // Not found, sorry!
    //
    return null;
  }

  /**
   * We're going to save the object in the source specified in the object or if no source is
   * specified in the last provider.
   *
   * @param t
   * @throws HopException
   */
  @Override
  public void save(T t) throws HopException {
    IHopMetadataProvider provider;
    if (StringUtils.isEmpty(t.getMetadataProviderName())) {
      List<IHopMetadataProvider> providers = multiProvider.getProviders();
      if (providers.isEmpty()) {
        throw new HopException("No providers are listed to save object '" + t.getName() + "' to");
      }
      provider = providers.get(providers.size() - 1);
    } else {
      // Find the provider listed in the object...
      // We use the description
      //
      String sourceDescription = t.getMetadataProviderName();
      provider = multiProvider.findProvider(sourceDescription);
      if (provider == null) {
        throw new HopException(
            "Hop metadata provider '"
                + sourceDescription
                + " could not be found to save object '"
                + t.getName()
                + "'");
      }
    }
    IHopMetadataSerializer<T> serializer = provider.getSerializer(managedClass);
    serializer.save(t);
  }

  /**
   * Delete the first object that matches the name in the providers.
   *
   * @param name The name of the object to delete
   * @return The deleted object...
   * @throws HopException
   */
  @Override
  public T delete(String name) throws HopException {
    List<IHopMetadataProvider> providers = multiProvider.getProviders();
    ListIterator<IHopMetadataProvider> providerIterator = providers.listIterator(providers.size());
    while (providerIterator.hasPrevious()) {
      IHopMetadataProvider provider = providerIterator.previous();
      IHopMetadataSerializer<T> serializer = provider.getSerializer(managedClass);
      if (serializer.exists(name)) {
        return serializer.delete(name);
      }
    }
    throw new HopException(
        "Object '" + name + "' was not found nor deleted from any of the metadata providers");
  }

  @Override
  public List<String> listObjectNames() throws HopException {
    Set<String> set = new HashSet<>();
    for (IHopMetadataProvider provider : multiProvider.getProviders()) {
      set.addAll(provider.getSerializer(managedClass).listObjectNames());
    }
    return new ArrayList<>(set);
  }

  @Override
  public boolean exists(String name) throws HopException {
    for (IHopMetadataProvider provider : multiProvider.getProviders()) {
      if (provider.getSerializer(managedClass).exists(name)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<T> loadAll() throws HopException {
    Set<T> set = new HashSet<>();
    for (IHopMetadataProvider provider : multiProvider.getProviders()) {
      set.addAll(provider.getSerializer(managedClass).loadAll());
    }
    return new ArrayList<>(set);
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Class<T> getManagedClass() {
    return managedClass;
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return multiProvider;
  }
}

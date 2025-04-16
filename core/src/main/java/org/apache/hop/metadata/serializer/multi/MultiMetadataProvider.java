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

package org.apache.hop.metadata.serializer.multi;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.BaseMetadataProvider;

/**
 * This metadata provider delegates for a standard provider but also reads information from others
 */
@Getter
public class MultiMetadataProvider implements IHopMetadataProvider {
  @Setter private ITwoWayPasswordEncoder twoWayPasswordEncoder;
  private final IVariables variables;
  @Setter private List<IHopMetadataProvider> providers;

  /**
   * @param twoWayPasswordEncoder The password encoder to use
   * @param providers The list of providers to use. If no source is specified when serializing the
   *     first is addressed.
   * @param variables The variables to resolve variable expressions with.
   */
  public MultiMetadataProvider(
      ITwoWayPasswordEncoder twoWayPasswordEncoder,
      List<IHopMetadataProvider> providers,
      IVariables variables) {
    this.twoWayPasswordEncoder = twoWayPasswordEncoder;
    this.providers = providers;
    this.variables = variables;
  }

  public MultiMetadataProvider(IVariables variables, IHopMetadataProvider... metadataProviders) {
    this.providers = new ArrayList<>();
    this.variables = variables;
    for (IHopMetadataProvider metadataProvider : metadataProviders) {
      providers.add(metadataProvider);
      if (metadataProvider.getTwoWayPasswordEncoder() != null) {
        this.twoWayPasswordEncoder = metadataProvider.getTwoWayPasswordEncoder();
      }
    }
    if (this.twoWayPasswordEncoder == null) {
      this.twoWayPasswordEncoder = Encr.getEncoder();
    }
  }

  private String calculateDescription() {
    StringBuilder descr = new StringBuilder();

    descr.append("Multi Metadata Provider with " + providers.size() + " providers");
    for (int i = 0; i < providers.size(); i++) {
      IHopMetadataProvider provider = providers.get(i);
      if (i == 0) {
        descr.append(": ");
      } else {
        descr.append(", ");
      }
      descr.append(provider.getDescription());
    }
    return descr.toString();
  }

  @Override
  public <T extends IHopMetadata> IHopMetadataSerializer<T> getSerializer(Class<T> managedClass)
      throws HopException {
    if (managedClass == null) {
      throw new HopException("You need to specify the class to serialize");
    }

    // Is this a metadata class?
    //
    HopMetadata hopMetadata = managedClass.getAnnotation(HopMetadata.class);
    if (hopMetadata == null) {
      throw new HopException(
          "To serialize class "
              + managedClass.getClass().getName()
              + " it needs to have annotation "
              + HopMetadata.class.getName());
    }

    String description = TranslateUtil.translate(hopMetadata.name(), managedClass);

    // Return the serializer for all providers
    // This makes sure we can list all objects across the list and so on...
    //
    return new MultiMetadataSerializer<>(this, managedClass, variables, description);
  }

  @Override
  public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
    Set<Class<T>> set = new HashSet<>();
    for (IHopMetadataProvider provider : providers) {
      set.addAll(provider.getMetadataClasses());
    }
    return new ArrayList<>(set);
  }

  @Override
  public <T extends IHopMetadata> Class<T> getMetadataClassForKey(String key) throws HopException {
    // This is from the base metadata provider: simply scan the registry and give back the class..
    // So we can take the first in the providers list and we'll be fine or just ask the base
    // provider.
    //
    if (providers.isEmpty()) {
      return new BaseMetadataProvider(variables, null).getMetadataClassForKey(key);
    } else {
      return providers.get(0).getMetadataClassForKey(key);
    }
  }

  /**
   * Find the provider with the given description
   *
   * @param providerDescription The description of the provider to look for
   * @return The provider with the given description or null if nothing could be found
   */
  public IHopMetadataProvider findProvider(String providerDescription) {
    ListIterator<IHopMetadataProvider> listIterator = providers.listIterator(providers.size());
    while (listIterator.hasPrevious()) {
      IHopMetadataProvider provider = listIterator.previous();
      if (provider.getDescription().equals(providerDescription)) {
        return provider;
      }
    }
    return null;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  @Override
  public String getDescription() {
    return calculateDescription();
  }

  /**
   * @param description The description to set
   */
  public void setDescription(String description) {
    throw new RuntimeException(
        "The description of the multi metadata provider can't be changed. "
            + "It's derived from the list of providers it contains.");
  }
}

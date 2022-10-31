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
 */

package org.apache.hop.mongo;

import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.TaggableReadPreference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public enum NamedReadPreference {
  PRIMARY(ReadPreference.primary()),
  PRIMARY_PREFERRED(ReadPreference.primaryPreferred()),
  SECONDARY(ReadPreference.secondary()),
  SECONDARY_PREFERRED(ReadPreference.secondaryPreferred()),
  NEAREST(ReadPreference.nearest());

  private ReadPreference pref = null;

  NamedReadPreference(ReadPreference pref) {
    this.pref = pref;
  }

  public String getName() {
    return pref.getName();
  }

  public ReadPreference getPreference() {
    return pref;
  }

  public static Collection<String> getPreferenceNames() {
    ArrayList<String> prefs = new ArrayList<>();

    for (NamedReadPreference preference : NamedReadPreference.values()) {
      prefs.add(preference.getName());
    }

    return prefs;
  }

  public ReadPreference getTaggableReadPreference(
      DBObject firstTagSet, DBObject... remainingTagSets) {

    switch (this) {
      case PRIMARY_PREFERRED:
        return ReadPreference.primaryPreferred(toTagsList(firstTagSet, remainingTagSets));
      case SECONDARY:
        return ReadPreference.secondary(toTagsList(firstTagSet, remainingTagSets));
      case SECONDARY_PREFERRED:
        return ReadPreference.secondaryPreferred(toTagsList(firstTagSet, remainingTagSets));
      case NEAREST:
        return ReadPreference.nearest(toTagsList(firstTagSet, remainingTagSets));
      default:
        return (pref instanceof TaggableReadPreference) ? pref : null;
    }
  }

  private static List<TagSet> toTagsList(DBObject firstTagSet, DBObject[] remainingTagSets) {
    List tagsList = new ArrayList(remainingTagSets.length + 1);
    tagsList.add(toTags(firstTagSet));
    for (DBObject cur : remainingTagSets) {
      tagsList.add(toTags(cur));
    }

    return tagsList;
  }

  private static TagSet toTags(DBObject tagsDocument) {
    List tagList = new ArrayList();
    for (String key : tagsDocument.keySet()) {
      tagList.add(new Tag(key, tagsDocument.get(key).toString()));
    }
    return new TagSet(tagList);
  }

  public static NamedReadPreference byName(String preferenceName) {
    NamedReadPreference foundPreference = null;

    for (NamedReadPreference preference : NamedReadPreference.values()) {
      if (preference.getName().equalsIgnoreCase(preferenceName)) {
        foundPreference = preference;
        break;
      }
    }
    return foundPreference;
  }
}

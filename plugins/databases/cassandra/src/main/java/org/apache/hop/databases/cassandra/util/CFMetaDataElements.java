/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.databases.cassandra.util;

/** Created by mracine on 2/6/2018. */
public enum CFMetaDataElements {
  KEYSPACE_NAME("keyspace_name"),
  TABLE_NAME("table_name"),
  BLOOM_FILTER_FP_CHANCE("bloom_filter_fp_chance"),
  CACHING("caching"),
  CDC("cdc"),
  CLUSTERING_ORDER("clustering_order"),
  COMMENT("comment"),
  COMPACTION("compaction"),
  COMPRESSION("compression"),
  COLUMN_NAME("column_name"),
  COLUMN_NAME_BYTES("column_name_bytes"),
  CRC_CHECK_CHANCE("crc_check_chance"),
  DC_LOCAL_READ_REPAIR_CHANCE("dc_local_read_repair_chance"),
  DEFAULT_TIME_TO_LIVE("default_time_to_live"),
  EXTENSIONS("extensions"),
  FLAGS("flags"),
  GC_GRACE_SECONDS("gc_grace_seconds"),
  ID("id"),
  KIND("kind"),
  MAX_INDEX_INTERVAL("max_index_interval"),
  MEMTABLE_FLUSH_PERIOD_IN_MS("memtable_flush_period_in_ms"),
  MIN_INDEX_INTERVAL("min_index_interval"),
  POSITION("position"),
  READ_REPAIR_CHANCE("read_repair_chance"),
  SPECULATIVE_RETRY("speculative_retry"),
  TYPE("type");

  private final String m_name;

  CFMetaDataElements(String name) {
    m_name = name;
  }

  @Override
  public String toString() {
    return m_name;
  }
}

#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SEARCH_DIR="assemblies/client/target/hop"

# Check if the search directory exists
if [ ! -d "$SEARCH_DIR" ]; then
    echo "Error: Directory '$SEARCH_DIR' does not exist" >&2
    exit 1
fi

find "$SEARCH_DIR" -type f -name "*.jar" | awk -F/ '
    function get_size(path,   cmd, s) {
      cmd = "stat -f%z \"" path "\""
      cmd | getline s
      close(cmd)
      return s+0
    }

    function human(n) {
      if (n < 1024) return n " B"
      else if (n < 1048576) return sprintf("%.1f KB", n/1024)
      else if (n < 1073741824) return sprintf("%.1f MB", n/1048576)
      else return sprintf("%.1f GB", n/1073741824)
    }

    function repeat(c, n,   s,i) {
      s=""
      for(i=1;i<=n;i++) s=s c
      return s
    }

    {
      full = $0
      file = $NF
      base = file
      sub(/-[0-9][0-9A-Za-z._-]*\.jar$/, "", base)

      size = get_size(full)

      count[base]++
      total_size[base] += size
      if (size > max_size[base]) max_size[base] = size
      if (!(base in example_file)) {
        example_file[base] = file
        example_size[base] = size
      }
    }

    END {
      # compute max widths
      max_count = 5
      max_name = length("NAME")
      max_example = length("EXAMPLE")
      max_size_w = length("SIZE")
      max_total = length("TOTAL")
      max_saved = length("SAVED")

      for (b in count) {
        if (count[b] > 1) {
          ex = example_file[b]
          max_name = (length(b) > max_name ? length(b) : max_name)
          max_example = (length(ex) > max_example ? length(ex) : max_example)
          s_ex = human(example_size[b])
          s_tot = human(total_size[b])
          s_saved = human(total_size[b] - max_size[b])
          max_size_w = (length(s_ex) > max_size_w ? length(s_ex) : max_size_w)
          max_total = (length(s_tot) > max_total ? length(s_tot) : max_total)
          max_saved = (length(s_saved) > max_saved ? length(s_saved) : max_saved)
          saved_map[b] = total_size[b] - max_size[b]
        }
      }

      # sort by saved descending (bubble sort for macOS awk)
      n = 0
      for (b in saved_map) {
        n++
        sorted[n] = b
      }
      for(i=1;i<=n;i++)
        for(j=i+1;j<=n;j++)
          if(saved_map[sorted[i]] < saved_map[sorted[j]]) {
            t=sorted[i]; sorted[i]=sorted[j]; sorted[j]=t
          }

      # prepare format string
      fmt = "%" max_count "s %-" max_name "s %-" max_example "s %" max_size_w "s %" max_total "s %" max_saved "s\n"

      # print header
      printf fmt, "COUNT", "NAME", "EXAMPLE", "SIZE", "TOTAL", "SAVED"

      # separator
      total_width = max_count + 1 + max_name + 1 + max_example + 1 + max_size_w + 1 + max_total + 1 + max_saved
      printf "%s\n", repeat("-", total_width)

      # print data rows
      sum_saved=0
      for(i=1;i<=n;i++){
        b = sorted[i]
        cnt = count[b]
        ex_file = example_file[b]
        ex_size = human(example_size[b])
        tot = human(total_size[b])
        saved = human(saved_map[b])
        sum_saved += saved_map[b]

        printf fmt, cnt, b, ex_file, ex_size, tot, saved
      }

      # footer
      printf "%s\n", repeat("-", total_width)
      printf "Total potential savings: %s\n", human(sum_saved)
    }
  '


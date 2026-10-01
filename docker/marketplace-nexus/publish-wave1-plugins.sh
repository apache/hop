#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Deprecated name: use publish-marketplace-plugins.sh
# Forwards all arguments and environment to the registry-driven publisher.
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "NOTE: publish-wave1-plugins.sh is deprecated; use publish-marketplace-plugins.sh" >&2
exec "${SCRIPT_DIR}/publish-marketplace-plugins.sh" "$@"

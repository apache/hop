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
"""DAGs that the Trigger Airflow DAG run integration tests start from Hop."""

import time
from datetime import datetime

from airflow.sdk import dag, get_current_context, task

START = datetime(2026, 1, 1)


@dag(dag_id="hop_it_success", schedule=None, start_date=START, catchup=False)
def hop_it_success():
    @task
    def ok():
        return "ok"

    ok()


@dag(dag_id="hop_it_failure", schedule=None, start_date=START, catchup=False)
def hop_it_failure():
    @task(retries=0)
    def fail():
        raise RuntimeError("This task fails on purpose")

    fail()


@dag(dag_id="hop_it_conf", schedule=None, start_date=START, catchup=False)
def hop_it_conf():
    """Fails unless Hop passed greeting = 'hello from hop' and count = 3 in the run conf."""

    @task(retries=0)
    def check_conf():
        conf = get_current_context()["dag_run"].conf or {}
        if conf.get("greeting") != "hello from hop" or int(conf.get("count", 0)) != 3:
            raise ValueError(f"Unexpected run conf: {conf}")
        return conf

    check_conf()


@dag(dag_id="hop_it_slow", schedule=None, start_date=START, catchup=False)
def hop_it_slow():
    @task
    def slow():
        time.sleep(120)

    slow()


hop_it_success()
hop_it_failure()
hop_it_conf()
hop_it_slow()

# Workflow AI Help — plugin notes

A workflow run configuration is metadata, and it is distinct from a pipeline run configuration.

A Pipeline action starts a pipeline. Its pipeline run configuration field must be the exact name of an existing Pipeline Run Configuration (case-sensitive).
If the workflow XML or check results say `local`, do not suggest `Local`.

Action hops are unconditional, success, or failure. Do not add a hop type the workflow does not use.

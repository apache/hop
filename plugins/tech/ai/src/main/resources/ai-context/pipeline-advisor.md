# Pipeline AI Help — plugin notes

A pipeline run configuration is metadata.
The run configuration name on a pipeline must match an existing Pipeline Run Configuration object exactly (case-sensitive).

Hops connect transforms by name from the structure JSON.
Copies and distribute vs copy-rows change parallelism; do not assume a transform is single-copy unless the structure JSON says so.

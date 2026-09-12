# Hop AI Assistant — plugin notes

Apache Hop metadata names are case-sensitive.
A run configuration, relational connection, or other metadata object named `local` is not `Local`.
Use the exact name from the project metadata. Do not guess a different capitalisation.

Do not invent metadata, transform plugins, or action plugins that are not listed in the prompt context.

Variable expressions use `${NAME}`. Prefer variables or a variable resolver for secrets; never ask the user to paste a live API key.

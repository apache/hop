# Hop AI Assistant — plugin notes

    Apache Hop metadata names are case-sensitive.
    Use the exact name from the project metadata. Do not guess a different capitalization.
    
    Pipeline run configurations and workflow run configurations are different metadata types.
    A Pipeline action that runs a pipeline must name a Pipeline Run Configuration, not a workflow one.
    
    Do not invent metadata, transform plugins, or action plugins that are not listed in the prompt context.
    
    Variable expressions use `${NAME}`. Prefer variables or a resolver for secrets; never ask the user to paste a live API key.
    
    Hop terminology: pipeline (.hpl) and transform; workflow (.hwf) and action.
    Project files and folders are usually VFS paths under `${PROJECT_HOME}`.

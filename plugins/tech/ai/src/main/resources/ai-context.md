# Hop AI Assistant — plugin notes

    Apache Hop metadata names are case-sensitive.
    Use the exact name from the project metadata. Do not guess a different capitalization.
    
    Pipeline run configurations and workflow run configurations are different metadata types.
    A Pipeline action that runs a pipeline must name a Pipeline Run Configuration, not a workflow one.
    
    Do not guess names of existing metadata, transforms, or actions that are not listed in the prompt context.
    New objects the user asked to create should use the names they gave. Plugin ids must come from the catalog JSON when it is present.
    
    Variable expressions use `${NAME}`. Prefer variables or a resolver for secrets; never ask the user to paste a live API key.
    
    Hop terminology: pipeline (.hpl) and transform; workflow (.hwf) and action.
    Project files and folders are usually VFS paths under `${PROJECT_HOME}`.

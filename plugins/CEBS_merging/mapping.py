def merged_edges_mapping(cls):
    default_text = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
    edges_props = {
        "agent_type": default_text,
        "anatomical_context_qualifier": default_text,
        "knowledge_level": default_text,
        "object": default_text,
        "original_object": default_text,
        "original_subject": default_text,
        "population_context_qualifier": default_text,
        "predicate": default_text,
        "primary_knowledge_source": default_text,
        "publications": default_text,
        "sex_qualifier": default_text,
        "species_context_qualifier": default_text,
        "subject": default_text,
    }

    nodes_props = {
        "type": "object",
        "properties": {
            "id": default_text,
            "name": default_text,
            "category": default_text,
            "equivalent_identifiers": default_text,
            "information_content": {
                "type": "float"
            }
        }
    }


    return {
            **edges_props,
            "subject": nodes_props,
            "object": nodes_props,
        }
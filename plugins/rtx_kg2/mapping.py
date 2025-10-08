
def merged_edges_mapping(cls):
    default_text = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}

    default_keyword ={"type": "keyword"}
    edges_props = {
            "agent_type": default_text,
            "domain_range_exclusion": {"type": "boolean", "index": False},
            "id": {"type": "long", "index": False},
            "kg2_ids": {"type": "text", "index": False},
            "knowledge_level": default_text,
            "predicate": default_text,
            "all_predicates": default_text,
            "primary_knowledge_source": default_text,
            "publications": default_text,
            "publications_info": {
                "type": "object",
                "enabled": False,
            },
            "qualified_object_aspect": {
                "type": "text",
                "index": False,
            },
            "qualified_object_direction": {
                "type": "text",
                "index": False,
            },
            "qualified_predicate": {
                "type": "text",
                "index": False,
            },
            }

    nodes_props = {
        "all_categories": default_text,
        "all_names": default_text,
        "category": default_text,
        "description": {"type": "text", "index": False},
        "equivalent_curies": default_text,
        "id": default_text,
        "iri": {"type": "text", "index": False},
        "name": default_text,
        "publications": default_text,
    }


    return {
            **edges_props,
            "subject": nodes_props,
            "object": nodes_props,
    }


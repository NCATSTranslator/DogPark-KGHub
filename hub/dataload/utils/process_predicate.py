from hub.dataload.utils.postprocessing import get_ancestors, remove_biolink_prefix


def process_predicate(edge, predicate_cache: dict):
    predicate = edge.get("predicate")

    if predicate:
        ancestors = get_ancestors(predicate, predicate_cache)
        if ancestors:
            edge["all_predicates"] = ancestors

        edge["predicate"] = remove_biolink_prefix(predicate)

import bmt

biolink = bmt.Toolkit()

def remove_single_biolink_prefix(text:str):
    if text.startswith("biolink:"):
        return text[8:]
    return text

def remove_biolink_prefix(target: str | list):
    if type(target) is str:
        return remove_single_biolink_prefix(target)

    return [remove_single_biolink_prefix(item) for item in target]

def get_ancestors(phrase: str, cache: dict):
    if phrase in cache:
        return cache[phrase]

    ancestors = biolink.get_ancestors(phrase, formatted=True)

    if ancestors:
        # ancestors = ['_'.join(item.split(' ')) for item in ancestors]
        ancestors = [remove_biolink_prefix(item) for item in ancestors]

    cache[phrase] = ancestors

    return ancestors
import pathlib
import jsonlines
from typing import Union

NODE_BUFFER_SIZE = 4096
EDGE_BUFFER_SIZE = 2048

def read_jsonl(input_file: Union[str, pathlib.Path]):
    """ Common reader to load data from jsonl files """
    with jsonlines.open(input_file) as source:
        buffer = []
        index = 0
        for doc in source:
            buffer.append(doc)

            doc["_id"] = doc["id"] if "id" in doc else str(index)

            if len(buffer) == NODE_BUFFER_SIZE:
                yield from buffer
                buffer = []

            index += 1

        if len(buffer) > 0:
            yield from buffer


def load_edges(data_folder: Union[str, pathlib.Path]):
    """ Stream edge data from given JSONL file """
    data_folder = pathlib.Path(data_folder).resolve().absolute()
    edge_file = data_folder.joinpath("edges.jsonl")
    yield from read_jsonl(edge_file)


def load_nodes(data_folder: Union[str, pathlib.Path]):
    """ Stream node data from given JSONL file """
    data_folder = pathlib.Path(data_folder).resolve().absolute()
    edge_file = data_folder.joinpath("nodes.jsonl")
    yield from read_jsonl(edge_file)


def load_merged_edges(data_folder: Union[str, pathlib.Path]):
    """ Generate merged edge data"""

    # use loaded node info as reference dict
    nodes = {node['id']: node for node in load_nodes(data_folder)}

    buffer = []
    for edge in load_edges(data_folder):
        subject_id = edge["subject"]
        object_id = edge["object"]

        edge["subject"] = nodes[subject_id]
        edge["object"] = nodes[object_id]

        buffer.append(edge)

        if len(buffer) == EDGE_BUFFER_SIZE:
            yield from buffer
            buffer = []


    if len(buffer) > 0:
        yield from buffer








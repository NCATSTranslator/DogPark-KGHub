import asyncio
import math
import os
import pickle
from functools import partial

from biothings import config as btconfig
from biothings.utils import mongo
from biothings.utils.backend import DocMongoBackend
from biothings.utils.common import iter_n
from biothings.utils.loggers import get_logger
from biothings.utils.mongo import doc_feeder, id_feeder

from biothings.hub.databuild.backend import LinkTargetDocMongoBackend
from biothings.hub.databuild.builder import DataBuilder

logging = btconfig.logger


class NodeEdgeBuilder(DataBuilder):
    """
    NodeEdgeBuilder is a dual source builder with the two sources representing the
    nodes and edges of some knowledge-graph like datasource. Rather than have two separate
    elasticsearch indices, we want to combine the node information into the edges (or vice-versa in
    some cases) so that we have one index containing the information stored from the two mongoDB
    collections
    """

    def __init__(self, build_name, source_backend, target_backend, *args, **kwargs):
        super().__init__(build_name, source_backend, target_backend=partial(LinkTargetDocMongoBackend), *args, **kwargs)
        conf = self.source_backend.get_build_configuration(self.build_name)
        assert hasattr(self.target_backend, "datasource_name")
        self.target_backend.datasource_name = conf["sources"][0]
        self.target_backend.source_db = self.source_backend

    async def merge_source(self, src_name, batch_size=100000, ids=None, job_manager=None):
        # it's actually not optional
        assert job_manager
        _query = self.generate_document_query(src_name)
        # Note: no need to check if there's an existing document with _id (we want to merge only with an existing document)
        # if the document doesn't exist then the update() call will silently fail.
        # That being said... if no root documents, then there won't be any previously inserted
        # documents, and this update() would just do nothing. So if no root docs, then upsert
        # (update or insert, but do something)
        defined_root_sources = self.get_root_document_sources()
        upsert = not defined_root_sources or src_name in defined_root_sources
        if not upsert:
            self.logger.debug(
                "Documents from source '%s' will be stored only if a previous document exists with same _id", src_name
            )
        jobs = []
        total = self.source_backend[src_name].count()
        btotal = math.ceil(total / batch_size)
        bnum = 1
        cnt = 0
        got_error = False
        # grab ids only, so we can get more, let's say 10 times more
        id_batch_size = batch_size * 10

        # FIXME id_provider initialized below will be overwritten by `if _query and ids is None:` code block
        if ids:
            self.logger.info(
                "Merging '%s' specific list of _ids, create merger job with batch_size=%d", src_name, batch_size
            )
            id_provider = [ids]
        else:
            self.logger.info(
                "Fetch _ids from '%s' with batch_size=%d, and create merger job with batch_size=%d",
                src_name,
                id_batch_size,
                batch_size,
            )
            id_provider = id_feeder(self.source_backend[src_name], batch_size=id_batch_size, logger=self.logger)

        if _query and ids is not None:
            self.logger.info("Query/filter involved, but also specific list of _ids. Ignoring query and use _ids")

        if _query and ids is None:
            self.logger.info("Query/filter involved, can't use cache to fetch _ids")
            # use doc_feeder but post-process doc to keep only the _id
            id_provider = map(
                lambda docs: [d["_id"] for d in docs],
                doc_feeder(
                    self.source_backend[src_name],
                    query=_query,
                    step=batch_size,
                    inbatch=True,
                    fields={"_id": 1},
                    logger=self.logger,
                ),
            )
        else:
            # when passing a list of _ids, IDs will be sent to the query, so we need to reduce the batch size
            id_provider = (
                ids
                and iter_n(ids, int(batch_size / 100))
                or id_feeder(self.source_backend[src_name], batch_size=id_batch_size, logger=self.logger)
            )

        src_master = self.source_backend.master
        meta = src_master.find_one({"_id": src_name}) or {}
        merger = meta.get("merger", "upsert")
        self.logger.info("Documents from source '%s' will be merged using %s", src_name, merger)
        merger_kwargs = meta.get("merger_kwargs")
        if merger_kwargs:
            self.logger.info(
                "Documents from source '%s' will be using these extra parameters during the merge %s",
                src_name,
                merger_kwargs,
            )

        doc_cleaner = self.document_cleaner(src_name)
        for big_doc_ids in id_provider:
            for doc_ids in iter_n(big_doc_ids, batch_size):
                # try to put some async here to give control back
                # (but everybody knows it's a blocking call: doc_feeder)
                await asyncio.sleep(0.1)
                cnt += len(doc_ids)
                pinfo = self.get_pinfo()
                pinfo["step"] = src_name
                pinfo["description"] = "#%d/%d (%.1f%%)" % (bnum, btotal, (cnt / total * 100))
                self.logger.info(
                    "Creating merger job #%d/%d, to process '%s' %d/%d (%.1f%%)",
                    bnum,
                    btotal,
                    src_name,
                    cnt,
                    total,
                    (cnt / total * 100.0),
                )
                job = await job_manager.defer_to_process(
                    pinfo,
                    partial(
                        node_edge_merger_worker,
                        self.source_backend[src_name].name,
                        self.target_backend.target_name,
                        doc_ids,
                        self.get_mapper_for_source(src_name, init=False),
                        doc_cleaner,
                        upsert,
                        merger,
                        bnum,
                        merger_kwargs,
                    ),
                )

                def batch_merged(f, batch_num):
                    nonlocal got_error
                    if type(f.result()) != int:
                        got_error = Exception(
                            "Batch #%s failed while merging source '%s' [%s]" % (batch_num, src_name, f.result())
                        )

                job.add_done_callback(partial(batch_merged, batch_num=bnum))
                jobs.append(job)
                bnum += 1
                # raise error as soon as we know
                if got_error:
                    raise got_error
        self.logger.info("%d jobs created for merging step", len(jobs))
        tasks = asyncio.gather(*jobs)

        def done(f):
            nonlocal got_error
            if None in f.result():
                got_error = Exception("Some batches failed")
                return
            # compute overall inserted/updated records (consume result() and check summable)
            _ = sum(f.result())

        tasks.add_done_callback(done)
        await tasks
        if got_error:
            raise got_error
        else:
            return {"%s" % src_name: cnt}


def node_edge_merger_worker(col_name, dest_name, ids, mapper, cleaner, upsert):
    """
    So for nodes and edges, in this case we treat the main collection as the edges. So we wish to
    aggregate the data from the nodes and enrich the edges with that information
    """
    try:
        source_database = get_src_db()
        target_database = mongo.get_target_db()
        source_column_name = source_database[col_name]
        target_column_name = target_database[dest_name]
        destination_backend = DocMongoBackend(target_database, target_column_name)
        cur = doc_feeder(source_column_name, step=len(ids), inbatch=False, query={"_id": {"$in": ids}})

        if cleaner:
            cur = map(cleaner, cur)
        mapper.load()

        docs = list(mapper.process(cur))
        stored_docs = destination_backend.mget_from_ids([d["_id"] for d in docs])
        ddocs = {d["_id"]: d for d in docs}

        # Merge the old document in mongodb into the new document
        for d in stored_docs:
            ddocs[d["_id"]] = node_edge_merge_struct(ddocs[d["_id"]], d)
        docs = list(ddocs.values())

        cnt = destination_backend.update(docs, upsert=upsert)
        return cnt
    except Exception as e:
        logger_name = "build_%s_%s_batch_%s" % (dest_name, col_name, batch_num)
        logger, _ = get_logger(logger_name, btconfig.LOG_FOLDER)
        logger.exception(e)
        logger.error(
            "col_name: %s, dest_name: %s, ids: see pickle, " % (col_name, dest_name)
            + "mapper: %s, cleaner: %s, upsert: %s, " % (mapper, cleaner, upsert)
            + "merger: %s, batch_num: %s" % (merger, batch_num)
        )
        exc_fn = os.path.join(btconfig.LOG_FOLDER, "%s.exc.pick" % logger_name)
        pickle.dump(e, open(exc_fn, "wb"))
        logger.info("Exception was dumped in pickle file '%s'", exc_fn)
        ids_fn = os.path.join(btconfig.LOG_FOLDER, "%s.ids.pick" % logger_name)
        pickle.dump(ids, open(ids_fn, "wb"))
        logger.info("IDs dumped in pickle file '%s'", ids_fn)
        dat_fn = os.path.join(btconfig.LOG_FOLDER, "%s.docs.pick" % logger_name)
        pickle.dump(docs, open(dat_fn, "wb"))
        logger.info("Data (batch of docs) dumped in pickle file '%s'", dat_fn)
        raise


def node_edge_merge_struct(edge_document: dict, node_document: dict) -> dict:
    edge_document["subject"] = node_document
    return edge_document

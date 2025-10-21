"""metadata matcher"""

# pylint: disable = invalid-name, invalid-unary-operand-type, no-member
from datetime import datetime
from typing import List, Dict, Any


# pylint: disable=import-error
import pandas as pd
from opensearchpy import OpenSearch
from opensearch_dsl import Search, Q
from orion.logger import SingletonLogger


class Matcher:
    """
    A class used to match or interact with an Elasticsearch index for performance scale testing.

    Attributes:
        index (str): Name of the Elasticsearch index to interact with.
        es_url (str): Elasticsearch endpoint
        verify_certs (bool): Whether to verify SSL certificates when connecting to Elasticsearch.
        uuid_field (str): Name of the field containing the UUID.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        index: str = "ospst-perf-scale-ci",
        es_server: str = "https://localhost:9200",
        verify_certs: bool = True,
        uuid_field: str = "uuid"
    ):
        self.index = index
        self.search_size = 10000
        self.logger = SingletonLogger.get_logger("Orion")
        self.es = OpenSearch(es_server,
                             timeout=30,
                             verify_certs=verify_certs,
                             http_compress=True,
                             max_retries=3,
                             retry_on_timeout=True)
        self.uuid_field = uuid_field

    def query_index(self, search: Search, return_all: bool = False):
        """Query index using search_after

        Args:
            search (Search): Search object with query
            return_all (bool): Returns full list of documents (optional)
        """
        self.logger.info("Executing query against index: %s", self.index)
        self.logger.debug("Executing query \r\n%s", search.to_dict())

        all_hits = []
        search_after = None
        while True:
            if search_after:
                search = search.extra(search_after=search_after)

            response = search.execute()
            hits = response.hits.hits

            if not hits:
                break

            if not return_all:
                return response

            all_hits.extend(hits)
            search_after = response.hits[-1].meta.sort
        return all_hits

    # pylint: disable=too-many-locals
    def get_uuid_by_metadata(
        self,
        metadata: Dict[str, Any],
        lookback_date: datetime = None,
        lookback_size: int = 10000,
        timestamp_field: str = "timestamp",
        additional_fields: List[str] = []
    ) -> List[Dict[str, str]]:
        """gets uuid by metadata

        Args:
            metadata (Dict[str, Any]): metadata of the runs
            lookback_date (datetime, optional):
            The cutoff date to get the uuids from. Defaults to None.
            lookback_size (int, optional):
            Maximum number of runs to get, gets the latest. Defaults to 10000.

            lookback_size and lookback_date get the data on the
            precedency of whichever cutoff comes first.
            Similar to a car manufacturer's warranty limits.
            timestamp_field (str): timestamp field in data

        Returns:
            List[Dict[str, str]]: List of dictionaries with uuid, buildURL and ocpVersion as keys
        """
        must_clause = []
        must_not_clause = []
        filter_clause = []
        for not_field, not_value in metadata.pop("not", {}).items():
            must_not_clause.append(Q("match", **{not_field: str(not_value)}))
        for wildcard_field, wildcard_value in metadata.pop("wildcard", {}).items():
            filter_clause.append(Q("wildcard", **{wildcard_field: str(wildcard_value)}))
        for regexp_field, regexp_value in metadata.pop("regexp", {}).items():
            filter_clause.append(Q("regexp", **{regexp_field: str(regexp_value)}))
        for field, value in metadata.items():
            must_clause.append(Q("match", **{field: str(value)}))

        if isinstance(lookback_date, datetime):
            lookback_date = lookback_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        if lookback_date:
            filter_clause.append(Q("range", **{timestamp_field: {"gt": lookback_date}}))
        query = Q(
            "bool",
            must=must_clause,
            must_not=must_not_clause,
            filter=filter_clause,
        )
        s = (
            Search(using=self.es, index=self.index)
            .query(query)
            .sort({timestamp_field: {"order": "desc"}})
            .extra(size=lookback_size)
        )
        all_hits = self.query_index(s, return_all=True)
        uuids_docs = []
        for hit in all_hits:
            source_data = hit.to_dict()["_source"]

            # Base document with required fields
            doc = source_data

            # Add additional fields if requested
            for field in additional_fields:
                doc[field] = source_data.get(field, "N/A")

            uuids_docs.append(doc)
        return uuids_docs

    def get_results(
        self, uuid: str,
        uuids: List[str],
        metrics: Dict[str, Any],
        timestamp_field: str = "timestamp"
    ) -> List[Dict[str, Any]]:
        """
        Get results of elasticsearch data query based on uuid(s) and defined metrics

        Args:
            uuid (str): uuid of the run
            uuids (list): list of uuids to get results for
            metrics (dict): metrics to get results for
            timestamp_field (str): timestamp field in data

        Returns:
            list: List of result dictionaries
        """
        if len(uuids) > 1 and uuid in uuids:
            uuids.remove(uuid)
        metric_queries = []
        not_queries = [
            ~Q("match", **{not_item_key: not_item_value})
            for not_item_key, not_item_value in metrics.get("not", {}).items()
        ]
        metric_queries = [
            Q("match", **{metric_key: metric_value})
            for metric_key, metric_value in metrics.items()
            if metric_key not in ["name", "metric_of_interest", "not"]
        ]
        metric_query = Q("bool", must=metric_queries + not_queries)
        query = Q(
            "bool",
            must=[
                Q("terms", **{self.uuid_field+".keyword": uuids}),
                metric_query
            ],
        )
        search = (
            Search(using=self.es, index=self.index)
            .query(query)
            .extra(size=self.search_size)
            .sort({timestamp_field: {"order": "desc"}})
        )
        all_hits = self.query_index(search, return_all=True)
        runs = [hit.to_dict()["_source"] for hit in all_hits]
        return runs

    def get_agg_metric_query(
        self, uuids: List[str],
        metrics: Dict[str, Any],
        timestamp_field: str = "timestamp"
    ):
        """burner_metric_query will query for specific metrics data.

        Args:
            uuids (list): List of uuids
            metrics (dict): metrics defined in es index metrics
            timestamp_field (str): timestamp field in data

        """
        metric_queries = []
        not_queries = [
            ~Q("match", **{not_item_key: not_item_value})
            for not_item_key, not_item_value in metrics.get("not", {}).items()
        ]
        metric_queries = [
            Q("match", **{metric_key: metric_value})
            for metric_key, metric_value in metrics.items()
            if metric_key not in ["name", "metric_of_interest", "not", "agg"]
        ]
        metric_query = Q("bool", must=metric_queries + not_queries)
        query = Q(
            "bool",
            must=[
                Q("terms", **{self.uuid_field + ".keyword": uuids}),
                metric_query,
            ],
        )
        search = (
            Search(using=self.es, index=self.index)
            .query(query)
            .extra(size=self.search_size)
            .sort({timestamp_field: {"order": "desc"}})
        )
        agg_value = metrics["agg"]["value"]
        agg_type = metrics["agg"]["agg_type"]
        search.aggs.bucket(
            "time", "terms", field=self.uuid_field+".keyword", size=self.search_size
        ).metric("time", "avg", field=timestamp_field)
        search.aggs.bucket(
            "uuid", "terms", field=self.uuid_field+".keyword", size=self.search_size
        ).metric(agg_value, agg_type, field=metrics["metric_of_interest"])
        result = self.query_index(search)
        data = self.parse_agg_results(result, agg_value, agg_type, timestamp_field)
        return data

    def parse_agg_results(
        self, data: Dict[Any, Any],
        agg_value: str,
        agg_type: str,
        timestamp_field: str = "timestamp"
    ) -> List[Dict[Any, Any]]:
        """parse out CPU data from kube-burner query
        Args:
            data (dict): Aggregated data from Elasticsearch DSL query
            agg_value (str): Aggregation value field name
            agg_type (str): Aggregation type (e.g., 'avg', 'sum', etc.)
        Returns:
            list: List of parsed results
        """
        res = []
        if "aggregations" not in data:
            return res

        stamps = data.aggregations.time.buckets
        agg_buckets = data.aggregations.uuid.buckets

        for stamp in stamps:
            dat = {}
            dat[self.uuid_field] = stamp.key
            dat[timestamp_field] = stamp.time.value_as_string
            agg_values = next(
                (item for item in agg_buckets if item.key == stamp.key), None
            )
            if agg_values:
                dat[agg_value + "_" + agg_type] = agg_values[agg_value].value
            else:
                dat[agg_value + "_" + agg_type] = None
            res.append(dat)
        return res

    def convert_to_df(
        self, data: Dict[Any, Any],
        columns: List[str] = None,
        timestamp_field: str = "timestamp"
    ) -> pd.DataFrame:
        """convert to a dataframe
        Args:
            data (_type_): _description_
            columns (_type_, optional): _description_. Defaults to None.
        Returns:
            _type_: _description_
        """
        odf = pd.json_normalize(data)
        odf = odf.sort_values(by=[timestamp_field])
        if columns is not None:
            odf = pd.DataFrame(odf, columns=columns)
        return odf

    def save_results(
        self,
        df: pd.DataFrame,
        csv_file_path: str = "output.csv",
        columns: List[str] = None,
    ) -> None:
        """write results to CSV
        Args:
            df (_type_): _description_
            csv_file_path (str, optional): _description_. Defaults to "output.csv".
            columns (_type_, optional): _description_. Defaults to None.
        """
        if columns is not None:
            df = pd.DataFrame(df, columns=columns)
        df.to_csv(csv_file_path)

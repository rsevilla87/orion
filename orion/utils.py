# pylint: disable=cyclic-import
# pylint: disable = line-too-long, too-many-arguments, consider-using-enumerate, broad-exception-caught
"""
module for all utility functions orion uses
"""
# pylint: disable = import-error

import json
import re
import urllib.parse
import xml.etree.ElementTree as ET
import xml.dom.minidom
from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import List, Any, Dict, Tuple
import pandas as pd
import pyshorteners
import requests
from tabulate import tabulate

import orion.constants as cnsts
from orion.matcher import Matcher
from orion.logger import SingletonLogger

class NoDataFound(Exception):
    pass

class Utils:
    """
    Helper utils class
    """

    def __init__(self, uuid_field: str ="uuid",  version_field: str ="ocpVersion"):
        """Instanciates utils class with uuid field

        Args:
            uuid_field (str): key to find the uuid
            version_field (str): key to find the version
        """
        self.uuid_field = uuid_field
        self.version_field = version_field
        self.logger = SingletonLogger.get_logger("Orion")

    # pylint: disable=too-many-locals
    def get_metric_data(
        self, uuids: List[str], metrics: Dict[str, Any], match: Matcher, test_threshold: int, timestamp_field: str="timestamp"
    ) -> List[pd.DataFrame]:
        """Gets details metrics based on metric yaml list

        Args:
            ids (list): list of all uuids
            metrics (dict): metrics to gather data on
            match (Matcher): current matcher instance
            logger (logger): log data to one output

        Returns:
            dataframe_list: dataframe of the all metrics
        """
        dataframe_list = []
        metrics_config = {}

        for metric in metrics:
            metric_name = metric["name"]
            metric_value_field = metric["metric_of_interest"]

            labels = metric.pop("labels", None)
            direction = int(metric.pop("direction", 0))
            threshold = abs(int(metric.pop("threshold", test_threshold)))
            timestamp_field = metric.pop("timestamp", timestamp_field)
            correlation = metric.pop("correlation", "")
            context = metric.pop("context", 5)
            self.logger.info("Collecting %s", metric_name)
            try:
                if "agg" in metric:
                    metric_df, metric_dataframe_name = self.process_aggregation_metric(
                        uuids, metric, match, timestamp_field
                    )
                else:
                    metric_df, metric_dataframe_name = self.process_standard_metric(
                        uuids, metric, match, metric_value_field, timestamp_field
                    )
                metric["labels"] = labels
                metric["direction"] = direction
                metric["threshold"] = threshold
                metric["correlation"] = correlation
                metric["context"] = context
                metrics_config[metric_dataframe_name] = metric
                dataframe_list.append(metric_df)
                self.logger.debug(metric_df)
            except Exception as e:
                self.logger.error(
                    "Couldn't get metrics %s, exception %s",
                    metric_name,
                    e,
                )
        return dataframe_list, metrics_config


    def process_aggregation_metric(
        self, uuids: List[str],  metric: Dict[str, Any], match: Matcher, timestamp_field: str="timestamp"
    ) -> pd.DataFrame:
        """Method to get aggregated dataframe

        Args:
            uuids (List[str]): _description_
            metric (Dict[str, Any]): _description_
            match (Matcher): _description_

        Returns:
            pd.DataFrame: _description_
        """
        aggregated_metric_data = match.get_agg_metric_query(uuids, metric, timestamp_field)
        aggregation_value = metric["agg"]["value"]
        aggregation_type = metric["agg"]["agg_type"]
        aggregation_name = f"{aggregation_value}_{aggregation_type}"
        if len(aggregated_metric_data) == 0:
            aggregated_df = pd.DataFrame(columns=[self.uuid_field, timestamp_field, aggregation_name])
        else:
            aggregated_df = match.convert_to_df(
                aggregated_metric_data, columns=[self.uuid_field, timestamp_field, aggregation_name],
                timestamp_field=timestamp_field
            )
            aggregated_df[timestamp_field] = aggregated_df[timestamp_field].apply(self.standardize_timestamp)

        aggregated_df = aggregated_df.drop_duplicates(subset=[self.uuid_field], keep="first")
        aggregated_metric_name = f"{metric['name']}_{aggregation_type}"
        aggregated_df = aggregated_df.rename(
            columns={aggregation_name: aggregated_metric_name}
        )
        if timestamp_field != "timestamp":
            aggregated_df = aggregated_df.rename(
                columns={timestamp_field: "timestamp"}
            )
        return aggregated_df, aggregated_metric_name

    def standardize_timestamp(self, timestamp: Any) -> str:
        """Method to standardize timestamp formats

        Args:
            timestamp Any: timestamp object with various formats 

        Returns:
            str: standard timestamp in format %Y-%m-%dT%H:%M:%S
        """
        if timestamp is None:
            return timestamp
        if timestamp.isnumeric():
            dt = pd.to_datetime(timestamp, unit='s', utc=True)
        else:
            dt = pd.to_datetime(timestamp, utc=True)
        return dt.replace(tzinfo=None).isoformat(timespec="seconds")

    def process_standard_metric(
        self,
        uuids: List[str],
        metric: Dict[str, Any],
        match: Matcher,
        metric_value_field: str,
        timestamp_field: str="timestamp"
    ) -> pd.DataFrame:
        """Method to get dataframe of standard metric

        Args:
            uuids (List[str]): _description_
            metric (Dict[str, Any]): _description_
            match (Matcher): _description_
            metric_value_field (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        standard_metric_data = match.get_results("", uuids, metric, timestamp_field=timestamp_field)
        if len(standard_metric_data) == 0:
            standard_metric_df = pd.DataFrame(columns=[self.uuid_field, timestamp_field, metric_value_field])
        else:
            standard_metric_df = match.convert_to_df(
                standard_metric_data, columns=[self.uuid_field, timestamp_field, metric_value_field],
                timestamp_field=timestamp_field
            )
            standard_metric_df[timestamp_field] = standard_metric_df[timestamp_field].apply(self.standardize_timestamp)
        standard_metric_name = f"{metric['name']}_{metric_value_field}"
        standard_metric_df = standard_metric_df.rename(
            columns={metric_value_field: standard_metric_name}
        )
        if timestamp_field != "timestamp":
            standard_metric_df = standard_metric_df.rename(
                columns={timestamp_field: "timestamp"}
            )

        standard_metric_df = standard_metric_df.drop_duplicates()
        return standard_metric_df, standard_metric_name


    def extract_metadata_from_test(self, test: Dict[str, Any]) -> Dict[Any, Any]:
        """Gets metadata of the run from each test

        Args:
            test (dict): test dictionary

        Returns:
            dict: dictionary of the metadata
        """
        metadata = test["metadata"]
        metadata[self.version_field] = str(metadata[self.version_field])
        self.logger.debug("metadata" + str(metadata))
        return metadata


    def get_version(self, uuids: List[str], match: Matcher, timestamp_field: str) -> dict:
        """Gets the version of the run from each test

        Args:
            uuids (List[str]): list of uuids to find version of
            match (Matcher): the fmatch instance
            timestamp_field (str): timestamp field in data
        """
        test = match.get_results("", uuids, {}, timestamp_field=timestamp_field)
        return {run[self.uuid_field]: run[self.version_field] for run in test}

    def get_build_urls(self, uuids: List[str], match: Matcher, timestamp_field: str):
        """Gets metadata of the run from each test
            to get the build url

        Args:
            uuids (list): str list of uuid to find build urls of
            match: the fmatch instance
            timestamp_field (str): timestamp field in data

        Returns:
            dict: dictionary of the metadata
        """

        test = match.get_results("", uuids, {}, timestamp_field=timestamp_field)
        buildUrls = {run[self.uuid_field]: run["buildUrl"] for run in test}
        return buildUrls


    def process_test(
        self,
        test: Dict[str, Any],
        match: Matcher,
        options: Dict[str, Any],
        start_timestamp: datetime
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process a test and get the data for the test

        Args:
            test (dict): test configuration
            match (Matcher): the matcher object
            options (dict): options for the run
            start_timestamp (datetime): start time for the run

        Returns:
            tuple: A tuple of a dataframe and a dictionary of metrics
        """
        self.logger.info("The test %s has started", test["name"])
        prs = {}

        test_threshold = test.get("threshold", 0)
        timestamp_field = test.get("timestamp", "timestamp")

        # get uuids, buildUrls matching with the metadata
        additional_fields = options.get("display")
        additional_fields.append(options.get("version_field"))
        runs = match.get_uuid_by_metadata(
            test["metadata"],
            lookback_date=start_timestamp,
            lookback_size=options["lookback_size"],
            timestamp_field=timestamp_field,
            additional_fields=additional_fields
        )
        uuids = [run[self.uuid_field] for run in runs]
        # get uuids if there is a uuid
        if options.get("uuid"):
            uuids.append(options["uuid"])
            prs[options["uuid"]] = self.sippy_pr_search(options["uuid"])
        elif not uuids:
            raise NoDataFound("No UUID present for the given metadata or uuid flag")
        for run in runs:
            if self.version_field in run:
                prs[run[self.uuid_field]] = self.sippy_pr_search(run[self.version_field])
            else:
                self.logger.error("Sippy version field '%s' not found in run: %s", self.version_field, run)
        match.index = options["benchmark_index"]
        # get metrics data and dataframe
        metrics = test["metrics"]
        dataframe_list, metrics_config = self.get_metric_data(
            uuids, metrics, match, test_threshold, timestamp_field
        )
        if not dataframe_list:
            raise NoDataFound(f"Data not found for the uuids: {uuids}")

        uuid_timestamp_map = pd.DataFrame()
        for df in dataframe_list:
            if "timestamp" in df.columns:
                uuid_timestamp_map = pd.concat(
                    [uuid_timestamp_map, df[[self.uuid_field, "timestamp"]].drop_duplicates()]
                )
        uuid_timestamp_map = uuid_timestamp_map.drop_duplicates(subset=[self.uuid_field])

        for i, df in enumerate(dataframe_list):
            dataframe_list[i] = df.drop(columns=["timestamp"], errors="ignore")

        merged_df = reduce(
            lambda left, right: pd.merge(left, right, on=self.uuid_field, how="outer"),
            dataframe_list,
        )

        merged_df = merged_df.merge(uuid_timestamp_map, on=self.uuid_field, how="left")
        merged_df = merged_df.sort_values(by="timestamp")
        merged_df["prs"] = merged_df[self.uuid_field].apply(lambda uuid: prs[uuid])

        # Add display field data if requested
        if options.get("display"):
            display_data = {run[self.uuid_field]: {field: run.get(field) for field in options["display"]} for run in runs}
            for field in options["display"]:
                merged_df[field] = merged_df[self.uuid_field].apply(
                    lambda uuid: display_data.get(uuid, {}).get(field)
                )
        if options["convert_tinyurl"]:
            shortener = pyshorteners.Shortener(timeout=10)
            shorten_url_field = test.get("shorten_url_field", "buildUrl")
            merged_df[shorten_url_field] = merged_df[shorten_url_field].apply(lambda url: shortener.tinyurl.short(url))
        merged_df = merged_df.reset_index(drop=True)
        # save the dataframe
        output_file_path = f"{options['save_data_path'].split('.')[0]}-{test['name']}.csv"
        match.save_results(merged_df, csv_file_path=output_file_path)
        return merged_df, metrics_config


    def sippy_pr_diff(self, base_version: str, new_version: str) -> List[str]:
        """Get diff between two versions in sippy
        Args:
            base_version (str): base version
            new_version (str): diff version
        Returns:
            list: list of PRs
        """
        base_url = "https://sippy.dptools.openshift.org/api/payloads/"
        filter_url = f"diff?fromPayload={base_version}&toPayload={new_version}"
        url = base_url + filter_url
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            self.logger.debug("Failed to get diff between %s and %s in sippy", base_version, new_version)
            return []
        return self.process_sippy_pr_list(response.json())

    def process_sippy_pr_list(self, pr_list: List[Dict[Any, Any]]) -> List[str]:
        """Process the list of PRs
        Args:
            pr_list (List[Dict[Any, Any]]): list of PRs
        Returns:
            List[str]: list of PR URLs
        """
        prs = []
        for pr in pr_list:
            prs.append(pr['url'])
        return prs

    def sippy_pr_search(self, version: str) -> List[str]:
        """Search for PRs in sippy

        Args:
            version (str): version to search for
        Returns:
            List[str]: list of PRs
        """
        base_url = "https://sippy.dptools.openshift.org/api/releases/pull_requests"
        filter_dict = {
            "items": [
                {
                    "columnField": "release_tag",
                    "operatorValue": "equals",
                    "value": version
                }
            ]
        }
        params = {
            "filter": json.dumps(filter_dict),
            "sortField": "pull_request_id",
            "sort": "asc"
        }
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            self.logger.debug("Failed to search for PRs in sippy for version %s", version)
            return []
        return self.process_sippy_pr_list(response.json())

# pylint: disable=too-many-locals
def json_to_junit(
    test_name: str, data_json: Dict[Any, Any], metrics_config: Dict[Any, Any], uuid_field: str, display_field: str = None
) -> str:
    """Convert json to junit format

    Args:
        test_name (_type_): _description_
        data_json (_type_): _description_

    Returns:
        _type_: _description_
    """
    testsuites = ET.Element("testsuites")
    testsuite = ET.SubElement(
        testsuites, "testsuite", name=f"{test_name} nightly compare"
    )
    failures_count = 0
    test_count = 0
    for metric, value in metrics_config.items():
        test_count += 1
        labels = value["labels"]
        label_string = " ".join(labels) if labels else ""
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            name=f"{label_string} {metric} regression detection",
            timestamp=str(int(datetime.now().timestamp())),
        )
        if [
            run
            for run in data_json
            if not run["metrics"][metric]["percentage_change"] == 0
        ]:
            failures_count += 1
            failure = ET.SubElement(testcase, "failure")
            failure.text = (
                "\n" + generate_tabular_output(data_json, metric_name=metric, uuid_field=uuid_field, display_field=display_field) + "\n"
            )

    testsuite.set("failures", str(failures_count))
    testsuite.set("tests", str(test_count))
    xml_str = ET.tostring(testsuites, encoding="utf8", method="xml").decode()
    dom = xml.dom.minidom.parseString(xml_str)
    pretty_xml_as_string = dom.toprettyxml()
    return pretty_xml_as_string


def generate_tabular_output(data: list, metric_name: str, uuid_field: str = "uuid", display_field: str = None) -> str:
    """converts json to tabular format

    Args:
        data (list):data in json format
        metric_name (str): metric name
        uuid_field (str): field name for UUID
        display_field (str): optional metadata field to display as a column
    Returns:
        str: tabular form of data
    """
    records = []
    def create_record(record):
        base_record = {
            uuid_field: record[uuid_field],
            "timestamp": datetime.fromtimestamp(record["timestamp"], timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "buildUrl": record["buildUrl"],
            metric_name: record["metrics"][metric_name]["value"],
            "is_changepoint": bool(record["metrics"][metric_name]["percentage_change"]),
            "percentage_change": record["metrics"][metric_name]["percentage_change"],
        }
        # Add metadata field if it exists in the record
        if display_field and display_field in record:
            base_record[display_field] = record[display_field]
        return base_record
    for i in range(0, len(data)):
        records.append(create_record(data[i]))

    df = pd.DataFrame(records).drop_duplicates().reset_index(drop=True)
    table = tabulate(df, headers="keys", tablefmt="psql")
    lines = table.split("\n")
    highlighted_lines = []
    if lines:
        highlighted_lines += lines[0:3]
    for i, line in enumerate(lines[3:-1]):
        if df["percentage_change"][
            i
        ]:  # Offset by 3 to account for header and separator
            highlighted_line = f"{lines[i+3]} -- changepoint"
            highlighted_lines.append(highlighted_line)
        else:
            highlighted_lines.append(line)
    highlighted_lines.append(lines[-1])

    # Join the lines back into a single string
    highlighted_table = "\n".join(highlighted_lines)

    return highlighted_table


def get_subtracted_timestamp(time_duration: str) -> datetime:
    """Get subtracted datetime from now

    Args:
        time_duration (str): time_gap in XdYh format

    Returns:
        datetime: return datetime of given timegap from now
    """
    logger = SingletonLogger.get_logger("Orion")
    reg_ex = re.match(r"^(?:(\d+)d)?(?:(\d+)h)?$", time_duration)
    if not reg_ex:
        logger.error("Wrong format for time duration, please provide in XdYh")
    days = int(reg_ex.group(1)) if reg_ex.group(1) else 0
    hours = int(reg_ex.group(2)) if reg_ex.group(2) else 0
    duration_to_subtract = timedelta(days=days, hours=hours)
    current_time = datetime.now(timezone.utc)
    timestamp_before = current_time - duration_to_subtract
    return timestamp_before


def get_output_extension(output_format: str) -> str:
    """ Get file extension for a given output format

    Args:
        output_format (str): output format in junit, json, text

    Returns:
        str: one amoung xml, json, txt
    """
    if output_format == cnsts.JSON:
        return "json"
    if output_format == cnsts.JUNIT:
        return "xml"
    return "txt"

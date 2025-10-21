"""
run test
"""
import os
import sys
import json
from typing import Any, Dict, Tuple
from orion.matcher import Matcher
from orion.logger import SingletonLogger
from orion.algorithms import AlgorithmFactory
import orion.constants as cnsts
from orion.utils import Utils, get_subtracted_timestamp, NoDataFound

def get_algorithm_type(kwargs):
    """Switch Case of getting algorithm name

    Args:
        kwargs (dict): passed command line arguments

    Returns:
        str: algorithm name
    """
    if kwargs["hunter_analyze"]:
        algorithm_name = cnsts.EDIVISIVE
    elif kwargs["anomaly_detection"]:
        algorithm_name = cnsts.ISOLATION_FOREST
    elif kwargs['cmr']:
        algorithm_name = cnsts.CMR
    else:
        algorithm_name = None
    return algorithm_name

# pylint: disable=too-many-locals
def run(**kwargs: dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """run method to start the tests

    Args:
      **kwargs: keyword arguments
        config (str): file path to config file
        es_server (str): elasticsearch endpoint
        output_path (str): output path to save the data
        hunter_analyze (bool): changepoint detection through hunter. defaults to True
        output_format (str): output to be table or json
        lookback (str): lookback in days

    Returns:
        tuple:
            - Test output (dict): Test JSON output
            - regression flag (bool): Test result
    """
    logger = SingletonLogger.get_logger("Orion")
    config = kwargs["config"]
    sippy_pr_search = kwargs["sippy_pr_search"]
    result_output = {}
    regression_flag = False
    for test in config["tests"]:

        uuid_field = test.get("uuid_field", "uuid")

        utils = Utils(uuid_field)
        # Create fingerprint Matcher
        matcher = Matcher(
            index=kwargs["metadata_index"],
            es_server=kwargs["es_server"],
            verify_certs=False,
            uuid_field=uuid_field
        )

        start_timestamp = get_start_timestamp(kwargs)
        try:
            fingerprint_matched_df, metrics_config = utils.process_test(
                test,
                matcher,
                kwargs,
                start_timestamp
            )
        except NoDataFound as err:
            logger.error(err)
            sys.exit(3)
        if fingerprint_matched_df.empty:
            logger.error("Empty dataframe returned for test %s", test["name"])
            sys.exit(3)

        algorithm_name = get_algorithm_type(kwargs)
        if algorithm_name is None:
            logger.error("No algorithm configured")
            return None, None, None
        logger.info("Comparison algorithm: %s", algorithm_name)

        algorithmFactory = AlgorithmFactory()
        algorithm = algorithmFactory.instantiate_algorithm(
                algorithm_name,
                fingerprint_matched_df,
                test,
                kwargs,
                metrics_config,
                uuid_field
            )
        # This is env is only present in prow ci
        prow_job_id = os.getenv("PROW_JOB_ID")
        if kwargs["output_format"] != cnsts.JSON and prow_job_id and prow_job_id.strip():
            testname, result_data, _ = algorithm.output(cnsts.JSON)
            output_file_name = f"{kwargs['save_output_path'].split('.')[0]}_{testname}.json"
            with open(output_file_name, 'w', encoding="utf-8") as file:
                file.write(str(result_data))

        testname, result_data, test_flag = algorithm.output(kwargs["output_format"])
        result_output[testname] = result_data
        # Query with JSON
        regression_data = []
        if test_flag:
            testname, result_data, test_flag = algorithm.output(cnsts.JSON)
            prev_ver = None
            bad_ver = None
            if kwargs["version_field"]:
                version_field = kwargs.get("version_field")
                for result in json.loads(result_data):
                    if result["is_changepoint"]:
                        bad_ver = result[version_field]
                    else:
                        prev_ver = result[version_field]
                    if prev_ver is not None and bad_ver is not None:
                        if sippy_pr_search:
                            prs = Utils().sippy_pr_diff(prev_ver, bad_ver)
                            doc = {"prev_ver": prev_ver,
                                    "bad_ver": bad_ver}
                            # We have seen where sippy_pr_diff returns an empty list of PRs
                            # since there is a change the payload tests have not completed.
                            if prs:
                                doc["prs"] = prs
                            regression_data.append(doc)
                        else:
                            regression_data.append({
                                "prev_ver": prev_ver,
                                "bad_ver": bad_ver,
                                "prs": []
                            })
                        prev_ver = None
                        bad_ver = None

        regression_flag = regression_flag or test_flag
    return result_output, regression_flag, regression_data


def get_start_timestamp(kwargs: Dict[str, Any]) -> str:
    """Get the start timestamp if lookback is provided."""
    return (
        get_subtracted_timestamp(kwargs["lookback"]) if kwargs.get("lookback") else ""
    )

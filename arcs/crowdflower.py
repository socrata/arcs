import logging
import os
import re
import requests
import simplejson
import time
import zipfile
import StringIO
from dateutil.parser import parse as dtparse
from frozendict import frozendict
from experiment import Job


FXF_RE = re.compile(r'[a-z0-9]{4}-[a-z0-9]{4}$')


def job_from_dict(job_data):
    """
    Helper function to create an Arcs Job from CrowdFlower job data.

    Args:
        job_data (dict): A dictionary of CrowdFlower job data

    Returns:
        An Arcs Job
    """
    metadata = dict(job_data)

    data = {
        "external_id": job_data["id"],
        "platform": "crowdflower",
        "metadata": metadata
    }

    return Job(**data)

headers = frozendict({'content-type': 'application/json', 'accept': 'application/json'})


def create_job_from_copy(api_key=None, job_id=None):
    """
    Create a new CrowdFlower job from a previous job, copying existing test questions.

    Args:
        api_key (str): Optional CrowdFlower API key; if not specified, we look in env.
        job_id (int): The unique job identifier of the job to copy

    Returns:
        A new Arcs Job
    """
    job_id = job_id or 788107
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    url = "https://api.crowdflower.com/v1/jobs/{}/copy.json?key={}&gold=true".format(job_id, api_key)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return job_from_dict(r.json())


def add_data_to_job(job_id, csv_file, api_key=None):
    """
    Upload a CSV file as the data for a job.

    Args:
        job_id (int): The unique job identifier for the job
        csv_file (str): The full path to CSV file to upload
        api_key (str): API token (use "CROWDFLOWER_API_KEY" env variable if not specified)
    """
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    url = "https://api.crowdflower.com/v1/jobs/{}/upload.json?key={}&force=true".format(job_id, api_key)
    with open(csv_file) as f:
        r = requests.put(url, data=f, headers=headers.copy(**{"content-type": "text/csv"}))
    r.raise_for_status()


def delete_job(job_id, api_key=None):
    """
    Delete a CrowdFlower job by ID.

    Args:
        job_id (int): The unique job identifier for the job
        api_key (str): API token (use "CROWDFLOWER_API_KEY" env variable if not specified)
    """
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    url = "https://api.crowdflower.com/v1/jobs/{}?key={}".format(job_id, api_key)
    r = requests.delete(url, headers=headers)
    r.raise_for_status()


def get_job(job_id, api_key=None):
    """
    Get a CrowdFlower job by ID.

    Args:
        job_id (int): The unique job identifier for the job
        api_key (str): API token (use "CROWDFLOWER_API_KEY" env variable if not specified)

    Returns:
        A CrowdFlower job as a dictionary.
    """
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    url = "https://api.crowdflower.com/v1/jobs/{}?key={}".format(job_id, api_key)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_job_metadata(job_id, api_key=None):
    """
    Get job metadata from CrowdFlower.

    Args:
        external_job_id (str or int): CrowdFlower job ID
        api_key (str): CrowdFlower API key (see README)

    Returns:
        metadata (dict): all available metadata for this job
        created_at (datetime.datetime): time of job creation
        completed_at (datetime.datetime or None): time of job completion,
            if completed, else None
    """
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    metadata = get_job(job_id)
    created_at = dtparse(metadata.get('created_at'))
    completed_at = metadata.get('completed_at')
    completed_at = dtparse(completed_at) if completed_at else None

    return metadata, created_at, completed_at


def get_jobs(api_key=None):
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]
    url = "https://api.crowdflower.com/v1/jobs.json?key={}".format(api_key)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def extract_json_from_csv(zip_data):
    """
    Helper function to grab the individual lines of JSON from the csv that CrowdFlower returns.

    There is lots of additional data in the return payload that we are ignoring for now. But this
    can change easily. We're turning the iterable into a dictionary/JSON, so that we can stash it
    in a JSON-type field into the DB.

    Args:
        zip_data (zipfile.ZipExtFile): opened zipfile or
             other iterable full of JSONifyable strings

    Returns:
        data (list): list of dicts of (query, result_fxf, judgment)
        full_json (dict): the full return content from
            crowdflower, keyed by line number
    """
    judged_data = []
    full_json = {}

    for i, line in enumerate(zip_data):
        j = simplejson.loads(line)
        full_json[i] = j
        data = j.get('data')
        query = data.get('query')
        # we should always include result_fxf in the data we hand off
        # to CrowdFlower, so that we don't have to parse it out
        # of the URL (but we can do that if necessary)
        result_fxf = data.get('result_fxf') or FXF_RE.search(data.get('link')).group()
        _golden = data.get('_golden', full_json[i]["state"].lower() in ("golden", "hidden_gold"))
        # TO DO: the following doesn't seem quite, right
        judgment = j['results']['relevance'].get('avg')

        judged_data.append(
            {"query": query, "result_fxf": result_fxf, "judgment": judgment, "_golden": _golden})

    return judged_data, full_json


def get_job_results(job_id, api_key=None):
    api_key = api_key or os.environ["CROWDFLOWER_API_KEY"]

    # ensure that results have been prepared for job
    post_url = "https://api.crowdflower.com/v1/jobs/{job_id}/regenerate" \
               "?type=json&key={api_key}"
    filled_post_url = post_url.format(job_id=job_id, api_key=api_key)
    r = requests.post(filled_post_url)
    r.raise_for_status()

    logging.info("Waiting 5 seconds because CrowdFlower doesn't like getting "
                 "too many requests at once...")
    time.sleep(5)

    # fetch results as JSON
    get_url = 'https://api.crowdflower.com/v1/jobs/{job_id}.csv' \
              '?type=json&key={api_key}'
    filled_get_url = get_url.format(job_id=job_id, api_key=api_key)

    for _ in range(5):
        try:
            ret = requests.get(filled_get_url)
            # we are returning a bytestring that would be a zipfile, were it a file
            # containing a single file where each line is a json blob
            zc = zipfile.ZipFile(StringIO.StringIO(ret.content))
            zip_data = zc.open(zc.namelist()[0])
            if zip_data:
                break
        except zipfile.BadZipfile:
            logging.error('Waiting 5 seconds, trying to grab zipfile again...')
            time.sleep(5)

    return extract_json_from_csv(zip_data)

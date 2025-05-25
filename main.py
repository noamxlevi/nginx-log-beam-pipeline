import time
import json
import logging
import argparse
from apache_beam.runners import DataflowRunner, DirectRunner
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions, SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from pipeline import pipeline

def main():
    # Command line arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--project',required=True, help='Google Cloud project')
    parser.add_argument('--region', required=True, help='Google Cloud region')
    parser.add_argument('--runner', required=True, help='Apache Beam Runner')
    parser.add_argument('--temp_location', required=True, help='Cloud Storage bucket for temp')
    parser.add_argument('--staging_location', required=True, help='Cloud Storage bucket for staging')
    parser.add_argument('--source', required=True, help='Source file')
    parser.add_argument('--bucket', required=True, help='Cloud Storage bucket of source file')
    parser.add_argument('--dataset', required=True, help='Big query dataset name for output')
    parser.add_argument('--table', required=True, help='Big query table name for output')
    parser.add_argument('--date', required=True, help='Date to be inserted to big query - YYYY/MM')
    parser.add_argument('--setup', required=True, help='')
    
    opts, beam_args = parser.parse_known_args()

    # Beam pipeline options
    options = PipelineOptions(beam_args)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(StandardOptions).runner = opts.runner
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('log-pipeline-',time.time_ns())
    options.view_as(SetupOptions).setup_file = opts.setup

    #input and output
    input = 'gs://{0}/{1}'.format(opts.bucket, opts.source)
    output = '{0}.{1}.{2}'.format(opts.project, opts.dataset, opts.table)

    # big query table schema
    with open('schema.json') as schema_file:
        schema = json.load(schema_file)
 
    # call pipeline
    pipeline(options, schema, input, output, opts.date)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()

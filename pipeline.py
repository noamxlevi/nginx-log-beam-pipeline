import utils
import apache_beam as beam
from functools import partial

def pipeline(options, schema, input, output, date):
    with beam.Pipeline(options=options) as p:
        # Read and parse log lines
        log_line = (p
            | 'Read CSV' >> beam.io.ReadFromText(input)
            | 'Parse line' >> beam.Map(utils.parse_log)
            | 'Filter out-of-format log lines' >> beam.Filter(lambda x: x is not None)
        )

        # Total entries
        total_entries = (log_line
            | 'Total amount of entries' >> beam.combiners.Count.Globally()
            | 'Total requests to dict' >> beam.Map(lambda total: {'total_requests': total})
        )

        # IP
        ip_unique = (log_line
            | 'Extract IP' >> beam.Map(lambda log_dict: log_dict['ip'])
            | 'Keep only unique IP' >> beam.Distinct()
            | 'Count IP' >> beam.combiners.Count.Globally()
            | 'Unique IP to dict' >> beam.Map(lambda ip_unique: {'ip_unique': ip_unique})
        )

        # Path
        path_unique = (log_line
            | 'Extract Path' >> beam.Map(lambda log_dict: log_dict['path'])
            | 'Create Path key-Value pair' >> beam.Map(lambda path: (path, 1))
            | 'Group and count Path by key' >> beam.combiners.Count.PerKey() 
        )
        path_max = (path_unique
            | 'Extract Path count' >> beam.Map(lambda path: path[1])
            | 'Get Path max' >> beam.CombineGlobally(max)
        )
        path_result = (path_unique
            | 'Find path max' >> beam.ParDo(utils.find_path_max(), beam.pvalue.AsSingleton(path_max))
            | 'Group Path(s) and max number' >> beam.GroupByKey()
            | 'Path results To Dict' >> beam.combiners.ToDict()
            | beam.Map(lambda x: {'count': int(next(iter(x))),'path': x[next(iter(x))]})
            | 'Frequent path to dict' >> beam.Map(lambda frequent_path: {'frequent_path': frequent_path})
        )

        # Status
        status = (log_line
            | 'Extract Status' >> beam.Map(lambda log_dict: log_dict['status'])
            | 'Create Status key-Value pair' >> beam.Map(lambda status: (status, 1))
            | 'Group and count Status by key' >> beam.combiners.Count.PerKey()
            | 'status ParDo' >> beam.ParDo(utils.status_analysis())
            | 'Status to list' >> beam.combiners.ToList()
            | 'Format Status' >> beam.Map(utils.status_to_dict)
            | 'Status to dict' >> beam.Map(lambda status: {'status': status})
        )

        # Referrer
        referrer_unique = (log_line
            | 'Extract referrer' >> beam.Map(lambda log_dict: log_dict['referrer'])
            | beam.Map(utils.drop_referrers)
            | 'Filter None' >> beam.Filter(lambda x: x is not None)
            | 'Create referrer key-Value pair' >> beam.Map(lambda referrer: (referrer, 1))
            | 'Group referrer by key' >> beam.combiners.Count.PerKey()
        )
        referrer_max = (referrer_unique
            | 'Extract referrer count' >> beam.Map(lambda x: x[1])
            | 'Get referrer max' >> beam.CombineGlobally(max)
        )
        referrer_result = (referrer_unique
            | beam.ParDo(utils.find_referrer_max(), beam.pvalue.AsSingleton(referrer_max))
            | 'Group referrer(s) and max number' >> beam.GroupByKey()
            | ' Referrer results ro Dict' >>beam.combiners.ToDict()
            | beam.Map(lambda x: {'count': int(next(iter(x))),'referrer': x[next(iter(x))] if x else None})
            | 'Frequent referrer to dict' >> beam.Map(lambda frequent_referrer: {'frequent_referrer': frequent_referrer})
        )

        # Results
        combined_results = ( 
            [total_entries, ip_unique, path_result, status, referrer_result]
            | 'Flatten Results' >> beam.Flatten() 
            | 'Collect into List' >> beam.combiners.ToList() 
            | 'Format results' >> beam.Map(partial(utils.format_results, date=date))
        )

        wrtie_results = (combined_results
            | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                table=output,
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

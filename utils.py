import re
import apache_beam as beam

def parse_log(line):
    line = line.strip()
    pattern = r'^((?P<ip>(?:\d{1,3}\.){3}\d{1,3}|[a-f0-9:]+)) - - \[(?P<timestamp>.*?)\] "(?P<action>GET|PUT|PATCH|HEAD|DELETE|POST)\s(?P<path>.*?)\s(?P<protocol>.*?)" (?P<status>\d{3}) (?P<size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'
    match = re.match(pattern, line)
    if match:
        log_dict = match.groupdict()
        print(log_dict)
        return log_dict
    else:
        return None
    
class find_path_max(beam.DoFn):
    def process(self, path, max_value):
        if path[1] == max_value:
            yield (max_value, path[0])

class status_analysis(beam.DoFn):
    def process(self, elem):
        status = [int(elem[0]), int(elem[1])]
        if 100 <= status[0] <= 199:
            yield ('informational_100_199', status[1])
        elif 200 <= status[0] <= 299:
            yield ('successful_200_299', status[1])
        elif 300 <= status[0] <= 399:
            yield ('redirection_300_399', status[1])
        elif 400 <= status[0] <= 499:
            yield ('client_400_499', status[1]) 
        elif 500 <= status[0] <= 599:
            yield ('server_500_599', status[1]) 
        else:
            return

def status_to_dict(status_list):
    status_dict = {'informational_100_199': 0, 'successful_200_299': 0,'redirection_300_399': 0, 'client_400_499': 0, 'server_500_599': 0}
    for status in status_list:
        if status[0] in status_dict:
            status_dict[status[0]] = status_dict[status[0]] + status[1]
    return status_dict 

class find_referrer_max(beam.DoFn):
    def process(self, referrer, max_value):
        referrer_value = referrer[0]
        referrer_max_value = referrer[1] 
        if referrer_max_value == max_value:
            yield (referrer_max_value, referrer_value)
        else:
            return

def drop_referrers(referrer):
    if referrer and referrer != '-':
        return (referrer)
    return None

class average_time(beam.DoFn):
    def process(self, time_sum, total_entries):
        average_time = time_sum / total_entries['total_requests']
        yield average_time

def format_results(dicts, date):
    results = {}
    results['date'] = date
    for dict in dicts:
        for key, value in dict.items():
            results[key] = value
    return results

#!/usr/bin/env python
import argparse
import sys

import redis


nagios_output_state = {
    'OK': 0,
    'WARNING': 1,
    'CRITICAL': 2,
    'UNKNOWN': 3,
}

class Check(object):
    def __init__(self, key, minimum, maximum, ascending=True):
        self.key = key
        self.value = None
        self.minimum = minimum
        self.maximum = maximum
        self.ascending = ascending

    def __str__(self):
        return "CHECK %s, %s, %s, %s" % (self.value, self.minimum, self.maximum, self.ascending)

    def __repr__(self):
        return str(self)


class NagiosReporter(object):
    OK = 0
    WARN = 1
    ERROR = 2
    UNKNOWN = 3
    STATUS = {
        OK: 'OK',
        WARN: 'WARNING',
        ERROR: 'CRITICAL',
        UNKNOWN: 'UNKNOWN',
    }

    def __init__(self):
        self.check_list = check_list
        self.result = -1

    def process(self, check_list):
        status =  -1
        for check in check_list:
            if status < self.ERROR and check.is_error():
                status = self.ERROR
            elif status < self.WARN and check.is_warning():
                status = self.WARN
            elif check.is_ok():
                status = self.OK

        output = "{status}\n{perf}".format(
            status=self.STATUS[status % len(self.STATUS)]
        )
        
            
            
        return result


class Redis(object):
    def __init__(self, args, host, port, username, password):
        self.host = args.host or 'localhost'
        self.port = args.port or 6379
        self.info = self._get_info(args)

        self.enable_performance_data = args.enable_performance_data


    def _get_info(self, args):
        try:
            return redis.Redis(
                host=self.host,
                port=self.port,
                password=args.password
            ).info()

        except Exception as err:
            print("Can't connect to %s:%s" % (self.host, self.port))
            print(err)
            sys.exit(nagios_output_state['UNKNOWN'])

    def _set_performance_data(self, command, value, warning, critical):
        # label=value;warn;crit;min;max
        if warning:
            warning = warning.replace('<', '').replace('>', '')
        if critical:
            critical = critical.replace('<', '').replace('>', '')

        return '%s=%s;%s;%s;;' % (
            command,
            value,
            warning or '',
            critical or ''
        )

    def _check_limits(self, value, warning, critical):
        status = 'OK'
        if warning:
            if warning[0] == '<':
                if value < float(warning.replace('<', '')):
                    status = 'WARNING'
            elif value > float(warning.replace('>', '')):
                status = 'WARNING'

        if critical:
            if critical[0] == '<':
                if value < float(critical.replace('<', '')):
                    status = 'CRITICAL'
            elif value > float(critical.replace('>', '')):
                status = 'CRITICAL'

        return status


    def _exit_with_nagios_format(self, data):
        output_state = 'OK'
        output = ''
        output_perf_data = ''

        kvpairs = []
        for command in self.command_list:
            command_data = data[command]
            if (
                nagios_output_state[command_data['check_state']] > 
                nagios_output_state[output_state]
            ):
                output_state = command_data['check_state']
            value = command_data['value']
            kvpairs.append((command, 'U' if value is None else value))

        output = output_state + '\n' + ', '.join(("%s: %s" % (k, v) for k, v in kvpairs))
        if self.enable_performance_data:
            output += '|' + output_perf_data

        print(output)
        sys.exit(nagios_output_state[output_state])

    def _hit_ratio(self):
        try:
            return (
                float(self.info['keyspace_hits']) / (
                    float(self.info['keyspace_hits']) + 
                    float(self.info['keyspace_misses'])  * 1.0
                )
            )
        except ZeroDivisionError:
            return None

    def _total_keys(self):
        if 'db0' in self.info:
            return self.info['db0']['keys']
        return None

    def get_value(self, check_name):
        if check_name in self.info:
            return self.info[check_name]
        custom_info = dict(
            hit_ratio=self._hit_ratio,
            total_keys=self._total_keys,
            #latency=self._latency,
        )
        if check_name in custom_info:
            return custom_info[check_name]()

    def check(self, check_list):
        self.check_list = check_list
        for check in check_list:
            check.value = self.get_value(check.key)

    def print_as_nagios(self):
        for check in self.check_list:
            
            pass 

    def deprecated(self):
        output_state = 'OK'
        output = ''
        output_perf_data = ''

        kvpairs = []
        for command in self.command_list:
            command_data = data[command]
            if (
                nagios_output_state[command_data['check_state']] > 
                nagios_output_state[output_state]
            ):
                output_state = command_data['check_state']
            value = command_data['value']
            kvpairs.append((command, 'U' if value is None else value))

        output = output_state + '\n' + ', '.join(("%s: %s" % (k, v) for k, v in kvpairs))
        if self.enable_performance_data:
            output += '|' + output_perf_data

        print(output)
        sys.exit(nagios_output_state[output_state])


def main():
    parser = argparse.ArgumentParser(
        description='Return result of a check to redis with nagios format')

    parser.add_argument(
        '--host',
        help='ip or cname of redis endpoint',
        type=str,
        required=True
    )

    parser.add_argument('--port', type=int)

    parser.add_argument('--username', type=str)

    parser.add_argument('--password', type=str)

    # Added more commands from https://blog.serverdensity.com/monitor-redis/
    all_command_choices = [
        'uptime_in_seconds', 'connected_clients', 'connected_slaves',
        'used_memory', 'mem_fragmentation_ratio', 'instantaneous_ops_per_sec',
        'rejected_connections', 'keyspace_hits', 'keyspace_misses',
        'used_memory_peak', 'rdb_last_save_time', 'rdb_changes_since_last_save',
        'evicted_keys', 'blocked_clients',
        'rejected_connections', 'hit_ratio', 'total_keys'
    ]
    parser.add_argument(
        '-k', '--check',
        nargs='+',
        help='list of checks to perform.',
    )

    parser.add_argument(
        '--enable-performance-data',
        help='enable output performance data',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '--command',
        help='Deprecated. Use --check instead',
        type=str,
    )
    parser.add_argument(
        '-w', '--warning',
        help='Deprecated. Use --check instead',
        type=str,
        default=None
    )
    parser.add_argument(
        '-c', '--critical',
        help='Deprecated. Use --check instead',
        type=str,
        default=None
    )

    args = parser.parse_args()

    checks = []
    for c in args.check:
        data = c.split(',')
        key = data[0]
        minimum = data[1] if len(data) >= 2 else None
        maximum = data[2] if len(data) >= 3 else None
        ascending = False if len(data) >= 4 and data[3] == 'd' else True
        checks.append(Check(key, minimum, maximum, ascending))

    if False:
        if ('all' in args.command):
            args.command = all_command_choices
        else:
            args.command = args.command.split(',')

    r = Redis(args, args.host, args.port, args.username, args.password)
    r.check(checks)
    r.print_as_nagios()


if __name__ == "__main__":
    main()

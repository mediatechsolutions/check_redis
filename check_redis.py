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


class Redis(object):

    def __init__(self, args):
        self.host = args.host or 'localhost'
        self.port = args.port or 6379
        self.info = self._get_info(args)

        self.enable_performance_data = args.enable_performance_data

        self.command_list = args.command
        self.critical_list = args.critical
        if self.critical_list:
            self.critical_list = self.critical_list.split(',')
        self.warning_list = args.warning
        if self.warning_list:
            self.warning_list = self.warning_list.split(',')


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

        for i in range(len(self.command_list)):
            command = self.command_list[i]
            command_data = data[command]
            if (
                nagios_output_state[command_data['check_state']] > 
                nagios_output_state[output_state]
            ):
                output_state = command_data['check_state']

            output += '%s: %s, ' % (command, command_data['value'])
            output_perf_data += command_data['perf_data'] + ' '
        
        output = output_state + '\n' +output
        if self.enable_performance_data:
            output += '|' + output_perf_data

        print(output)
        sys.exit(nagios_output_state[output_state])

    def _hit_ratio(self):
        return (
            float(self.info['keyspace_hits']) / (
                float(self.info['keyspace_hits']) + 
                float(self.info['keyspace_misses'])  * 1.0
            )
        )

    def _total_keys(self):
        return self.info['db0']['keys']

    def perform_check(self):
        data = dict()
        for i in range(len(self.command_list)):
            command = self.command_list[i]
            data[command] = dict()
            data[command]['value'] = 'UNKNOWN'
            data[command]['perf_data'] = ''
            data[command]['check_state'] = 'OK'

            try:
                warning = self.warning_list[i]
            except:
                warning = None
            try:
                critical = self.critical_list[i]
            except:
                critical = None
            if command in ['hit_ratio', 'total_keys', 'latency']:
                data[command]['value'] = eval('self._%s' % command)()
            else:
                data[command]['value'] = self.info[command]
            data[command]['perf_data'] = self._set_performance_data(
                command,
                data[command]['value'],
                warning,
                critical
            )
            data[command]['check_state'] = self._check_limits(
                data[command]['value'],
                warning,
                critical
            )



        self._exit_with_nagios_format(data)


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
        '--command',
        help='operation mode. Limits check will not be performed if there are multiple commands (separated by commas) or if "all" is selected. Possible values: %s or "all"' % all_command_choices,
        type=str,
        required=True
    )

    parser.add_argument(
        '--enable-performance-data',
        help='enable output performance data',
        action='store_true',
        default=False
    )
    parser.add_argument(
        '-w', '--warning',
        help='number of entries neededed to throw a warning. If command is "all" or is a list of commands (separated by commas), warning need to be a list and limites will be checked on same order that command list. You can specify operation to check warning (default is >). Example --warning ">42,<42"',
        type=str,
        default=None
    )
    parser.add_argument(
        '-c', '--critical',
        help='number of entries neededed to throw a critical If command is "all" or is a list of commands, critical need to be a list and limites will be checked on same order that command list. You can specify operation to check warning (default is >). Example --critical ">42,<42"',
        type=str,
        default=None
    )

    args = parser.parse_args()

    if ('all' in args.command):
        args.command = all_command_choices
    else:
        args.command = args.command.split(',')

    result = Redis(args).perform_check()

if __name__ == "__main__":
    main()

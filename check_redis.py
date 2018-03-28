#!/usr/bin/env python
import argparse
import sys
import itertools

import redis


nagios_output_state = {
    'OK': 0,
    'WARNING': 1,
    'CRITICAL': 2,
    'UNKNOWN': 3,
}

class Check(object):
    def __init__(self, key, warning_limit, error_limit, ascending=True):
        self.key = key
        self.value = None
        self.warning_limit = float(warning_limit) if warning_limit else None
        self.error_limit = float(error_limit) if error_limit else None
        self.ascending = ascending

    def __str__(self):
        return "CHECK %s, %s, %s, %s" % (self.value, self.minimum, self.maximum, self.ascending)

    def __repr__(self):
        return str(self)

    def _evaluate(self, value, limit):
        if value is None or limit is None:
            return False
        result = float(value) - limit

        return result > 0 if self.ascending else result < 0

    def is_warning(self):
        return self._evaluate(self.value, self.warning_limit)
        
    def is_error(self):
        return self._evaluate(self.value, self.error_limit)


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
        self.result = -1

    def process(self, check_list):
        status =  -1
        feedback = ''
        performance = []
        for check in check_list:
            op = '>' if check.ascending else '<'
            if status < self.ERROR and check.is_error():
                status = self.ERROR
                feedback += "ERROR for %s: %s %s %s\n" % (check.key, check.value, op, check.error_limit)
            elif status < self.WARN and check.is_warning():
                status = self.WARN
                feedback += "WARNING for %s: %s %s %s\n" % (check.key, check.value, op, check.warning_limit)
            elif check.value is None:
                feedback += "UNKNOWN: %s could not be processed" % check.key
            else:
                status = self.OK

            performance.append(
                "{label}={value};{warn};{crit};;".format(
                    label=check.key,
                    value='U' if check.value is None else check.value,
                    warn='' if check.warning_limit is None else check.warning_limit,
                    crit='' if check.error_limit is None else check.error_limit,
                )
            )

        print("{feedback}\n|{perf}".format(feedback=feedback, perf='\n'.join(performance)))
            
        return status % len(self.STATUS)


class Redis(object):
    def __init__(self, host, port, password):
        self.info = self._get_info(host, port, password)

    def _get_info(self, host, port, password):
        try:
            return redis.Redis(
                host=host,
                port=port,
                password=password,
            ).info()
        except Exception as err:
            print("Can't connect to %s:%s" % (host, port))
            print(err)
            sys.exit(NagiosReporter.UNKNOWN)

    def _hit_ratio(self):
        try:
            return (
                float(self.info['keyspace_hits']) / (
                    float(self.info['keyspace_hits']) + 
                    float(self.info['keyspace_misses'])
                )
            )
        except ZeroDivisionError:
            return None

    def _total_keys(self, db=None):
        if db is None:
            result = 0
            for k in self.info.keys():
                if not k.startswith('db'):
                    continue
                v = self.info[k]
                if not isinstance(v, dict):
                    continue
                result += int(v.get('keys', 0))
                return result
        if db in self.info:
            return int(self.info[db]['keys'])
        return None

    def get_value(self, check_name):
        if check_name in self.info:
            return self.info[check_name]
        if 'hit_ratio' == check_name:
            return self._hit_ratio()
        if check_name.startswith('total_keys_'):
            return self._total_keys(check_name[len('total_keys_'):])
        if check_name == 'total_keys':
            return self._total_keys()
        return None

    def check(self, check_list):
        self.check_list = check_list
        for check in check_list:
            value = self.get_value(check.key)
            try:
                check.value = int(value)
            except TypeError:
                try:
                    check.value = float(value)
                except TypeError:
                    check.value = value 

    def list_checks(self):
        keys = self.info.keys()
        keys.append("hit_ratio")
        for x in sorted(self.info.keys()):
            print(x)

def main():
    parser = argparse.ArgumentParser(
        description='Return result of a check to redis with nagios format')

    parser.add_argument(
        '--host',
        help='ip or cname of redis endpoint',
        type=str,
        required=True
    )

    parser.add_argument('--port', type=int, default=6379)

    parser.add_argument('--password', type=str)

    parser.add_argument('action', choices="check list".split(), default='check', nargs='?')

    parser.add_argument(
        '-k', '--check',
        nargs='+',
        default=[],
        help='list of checks to perform.',
    )

    parser.add_argument(
        '--enable-performance-data',
        help='Deprecated. It is always shown',
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

    commands = (args.command or "").split(',')
    warnings = (args.warning or "").split(',')
    criticals = (args.critical or "").split(',')
    for comm, w, c in itertools.izip_longest(commands, warnings, criticals):
        checks.append(Check(comm, w, c))
        

    r = Redis(args.host, args.port, args.password)
    if args.action == 'list':
        r.list_checks()
    else:
        r.check(checks)
        sys.exit(NagiosReporter().process(checks))


if __name__ == "__main__":
    main()

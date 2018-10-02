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
    def __init__(self, key, forced=False):
        self.key = key
        self.value = None
        self.warning_limit = None
        self.error_limit = None
        self.ascending = True
        self.minimum = None 
        self.maximum = None
        self.forced = forced

    def __str__(self):
        return "CHECK %s=%s, %s, %s, %s" % (self.key, self.value, self.warning_limit, self.error_limit, self.ascending)

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
        for key in sorted(check_list.keys()):
            check = check_list[key]
            if isinstance(check.value, str) and not check.forced:
                # avoid string outputs unless forced
                continue
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
                "{label}={value};{warn};{crit};{m};{M}".format(
                    label=check.key,
                    value='U' if check.value is None else check.value,
                    warn='' if check.warning_limit is None else check.warning_limit,
                    crit='' if check.error_limit is None else check.error_limit,
                    m='' if check.minimum is None else check.minimum,
                    M='' if check.maximum is None else check.maximum,
                )
            )

        print("{feedback}\n|{perf}".format(feedback=feedback, perf='\n'.join(performance)))
        if status == -1:
            status = self.UNKNOWN
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
        for check in check_list.values():
            value = self.get_value(check.key)
            if value is None:
                continue
            try:
                check.value = int(value)
            except ValueError:
                try:
                    check.value = float(value)
                except ValueError:
                    check.value = value 

    def list_checks(self):
        keys = self.info.keys()
        keys.append("hit_ratio")
        return keys


def parse_args():
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
        '-i', '--include',
        nargs='+',
        default=[],
        help='list of checks to perform.',
    )
    parser.add_argument(
        '-e', '--exclude',
        nargs='+',
        default=[],
        help='list of checks to perform.',
    )
    parser.add_argument(
        '-c', '--check-config',
        nargs='+',
        default=[],
        help='Check configuration, with format: key,warning,critical,([a]|d),min,max',
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

    return parser.parse_args()


def main():
    args = parse_args()

    r = Redis(args.host, args.port, args.password)
    if args.action == 'list':
        for x in sorted(r.list_checks()):
            print(x)
        return

    checks = {}

    if args.include:
        for key in args.include:
            checks[key] = Check(key, forced=True)
    else:
        for key in r.list_checks():
            checks[key] = Check(key)
        
    for key in args.exclude:
        if key in checks:
            checks.pop(key)

    for c in args.check_config:
        data = c.split(',')
        key = data[0]
        if key not in checks:
            continue
        check = checks[key]
        check.warning_limit = float(data[1]) if len(data) >= 2 else None
        check.error_limit = float(data[2]) if len(data) >= 3 else None
        check.minimum = float(data[2]) if len(data) >= 3 else None
        check.maximum = float(data[3]) if len(data) >= 4 else None
        check.ascending = False if len(data) >= 6 and data[5] == 'd' else True
        
    r.check(checks)
    rc = NagiosReporter().process(checks)
    sys.exit(rc)


if __name__ == "__main__":
    main()

#!/usr/bin/env python

node_desc = {
  "architecture": {
    "platform_type": "x86_64",
    "smp_size": 2,
    "smt_size": 48
  },
  "bios": {
    "release_date": "03/09/2015",
    "vendor": "Dell Inc.",
    "version": 1.2
  },
  "chassis": {
    "manufacturer": "Dell Inc.",
    "name": "PowerEdge R630",
    "serial": "4Q28C42"
  },
  "type": "node"
}



def flatten(d, key=""):
    results = []
    if type(d) is dict:
        for k in d:
            value = d[k]
            flatten_value = flatten(value, key=k)
            v_with_prefix = map(lambda x: "%s.%s" % (k, x), flatten_value)
            results += v_with_prefix
    else:
        results += ["%s=%s" % (key, d)]
    return results

print(flatten(node_desc))

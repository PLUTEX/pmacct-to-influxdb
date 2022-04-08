#!/bin/sh

curl -s https://www.cidr-report.org/as2.0/autnums.html |
    sed -n 's#^<a .*>AS\([0-9]*\) *</a> *\([^ ,]*\).* \([A-Z][A-Z]\)$#\1\t\3\t\2#p' |
    sponge asn_to_names.txt

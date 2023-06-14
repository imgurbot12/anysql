#!/bin/sh
sqlmap --url 'http://localhost:8000/login?username=u&password=p' --dump-all --ignore-code 401 --level 5 --risk 3

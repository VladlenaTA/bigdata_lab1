#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi
# write some really existing User Agents and one malformed
USER_AGENTS=("Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko" 
			"Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0;  rv:11.0) like Gecko" 
			"Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0" 
			"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0"
			"Opera/9.80 (X11; Linux i686; U; es-ES) Presto/2.8.131 Version/11.11"
			"Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/5.0 Opera 11.11"
			"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-gb) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27"
			"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; sv-se) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27"
			"Mozilliuz/5.0 (Hackintosh; U; Intel Macosev OS X 10_6_6; en-gb) bubu baba")

# prefix doesn't mean something - it can be repetable
PREFIX="ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \""

rm -rf input
mkdir input

for i in {1..200}
	do 
		RESULT="$PREFIX${USER_AGENTS[$((RANDOM % ${#USER_AGENTS[*]}))]}\""
		echo $RESULT >> input/$1.1
	done

for i in {1..50}
	do
		RESULT="$PREFIX${USER_AGENTS[$((RANDOM % ${#USER_AGENTS[*]}))]}\""
		echo $RESULT >> input/$1.2
	done

for i in {1..50}
	do
		RESULT="$PREFIX${USER_AGENTS[$((RANDOM % ${#USER_AGENTS[*]}))]}\""
		echo $RESULT >> input/$1.3
	done

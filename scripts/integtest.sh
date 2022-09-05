#!/bin/bash
set -e
function usage() {
    echo ""
    echo "This script is used to run integration tests for plugin installed on a remote OpenSearch/Dashboards cluster."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo "None"
    echo ""
    echo "Optional arguments:"
    echo -e "-b BIND_ADDRESS\t, defaults to localhost | 127.0.0.1, can be changed to any IP or domain name for the cluster location."
    echo -e "-p BIND_PORT\t, defaults to 9200, can be changed to any port for the cluster location."
    echo -e "-t TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo -e "-a Leader BIND_ADDRESS\t, defaults to localhost | 127.0.0.1, can be changed to any IP or domain name for the cluster location."
    echo -e "-b Leader BIND_PORT\t, defaults to 9200, can be changed to any port for the cluster location."
    echo -e "-c Leader TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo -e "-x Follower BIND_ADDRESS\t, defaults to localhost | 127.0.0.1, can be changed to any IP or domain name for the cluster location."
    echo -e "-y Follower BIND_PORT\t, defaults to 9200, can be changed to any port for the cluster location."
    echo -e "-z Follower TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo -e "-s SECURITY_ENABLED\t(true | false), defaults to true. Specify the OpenSearch/Dashboards have security enabled or not."
    echo -e "-c CREDENTIAL\t(usename:password), no defaults, effective when SECURITY_ENABLED=true."
    echo -e "-h\tPrint this message."
    echo -e "-v OPENSEARCH_VERSION\t, no defaults"
    echo -e "-n SNAPSHOT\t, defaults to false"
    echo "--------------------------------------------------------------------------"
}

while getopts ":h:b:p:s:c:v:n:t:" arg; do
while getopts ":h:a:b:c:x:y:z:s:c:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        a)
            LBIND_ADDRESS=$OPTARG
            ;;
        b)
            BIND_ADDRESS=$OPTARG
            LBIND_PORT=$OPTARG
            ;;
        c)
            LTRANSPORT_PORT=$OPTARG
            ;;
        x)
             FBIND_ADDRESS=$OPTARG
            ;;
        p)
            BIND_PORT=$OPTARG
        y)
            FBIND_PORT=$OPTARG
            ;;
        t)
            TRANSPORT_PORT=$OPTARG
        z)
            FTRANSPORT_PORT=$OPTARG
            ;;
        s)
            SECURITY_ENABLED=$OPTARG
            ;;
        c)
            CREDENTIAL=$OPTARG
            ;;
        v)
            # Do nothing as we're not consuming this param.
            ;;
        n)
            # Do nothing as we're not consuming this param.
            ;;
        :)
            echo "-${OPTARG} requires an argument"
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${OPTARG}"
            exit 1
            ;;
    esac
done


if [ -z "$BIND_ADDRESS" ]
if [ -z "$LBIND_ADDRESS" ]
then
  BIND_ADDRESS="localhost"
  echo "requires an argument -lb <Leader bind address>"
  usage
  exit 1
fi

if [ -z "$BIND_PORT" ]
if [ -z "$LBIND_PORT" ]
then
  BIND_PORT="9200"
  echo "requires an argument -lp <Leader bind port>"
  usage
  exit 1
fi

if [ -z "$TRANSPORT_PORT" ]
if [ -z "$LTRANSPORT_PORT" ]
then
  TRANSPORT_PORT="9300"
  echo "requires an argument -lt <Leader transport port>"
  usage
  exit 1
fi

if [ -z "$FBIND_ADDRESS" ]
then
  echo "requires an argument -fb <Follower bind address>"
  usage
  exit 1
fi

if [ -z "$FBIND_PORT" ]
then
  echo "requires an argument -fp <Follower bind port>"
  usage
  exit 1
fi

if [ -z "$FTRANSPORT_PORT" ]
then
  echo "requires an argument -ft <Follower transport port>"
  usage
  exit 1
fi

if [ -z "$SECURITY_ENABLED" ]
then
  SECURITY_ENABLED="true"
fi
if [ -z "$CREDENTIAL" ]
then
  CREDENTIAL="admin:admin"
fi
USERNAME=`echo $CREDENTIAL | awk -F ':' '{print $1}'`
PASSWORD=`echo $CREDENTIAL | awk -F ':' '{print $2}'`

./gradlew integTestRemote -Dfollower.http_host="$BIND_ADDRESS:$BIND_PORT" -Dfollower.transport_host="$BIND_ADDRESS:$TRANSPORT_PORT" -Dsecurity_enabled=$SECURITY_ENABLED -Duser=$USERNAME -Dpassword=$PASSWORD --console=plain
eval "./gradlew integTestRemote -Dleader.http_host=\"$LBIND_ADDRESS:$LBIND_PORT\" -Dleader.transport_host=\"$LBIND_ADDRESS:$LTRANSPORT_PORT\" -Dfollower.http_host=\"$FBIND_ADDRESS:$FBIND_PORT\" -Dfollower.transport_host=\"$FBIND_ADDRESS:$FTRANSPORT_PORT\" -Dsecurity_enabled=\"$SECURITY_ENABLED\" -Duser=\"$USERNAME\" -Dpassword=\"$PASSWORD\" --console=plain "

#!/bin/bash

set -e

function usage() {
    echo ""
    echo "This script is used to run integration tests for plugin installed on a remote OpenSearch/Dashboards cluster."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Optional arguments:"
    echo -e "-s SECURITY_ENABLED\t(true | false), defaults to true. Specify the OpenSearch/Dashboards have security enabled or not."
    echo -e "-c CREDENTIAL\t(usename:password), no defaults, effective when SECURITY_ENABLED=true."
    echo -e "-h Print this message."
    echo -e "-v OPENSEARCH_VERSION\t, no defaults"
    echo -e "-n SNAPSHOT\t\t, defaults to false"
    echo "Required arguments:"
    echo "Single cluster test:"
    echo -e "-b BIND_ADDRESS\t\t, IP or domain name for the cluster location."
    echo -e "-p BIND_PORT\t\t, port for the cluster location."
    echo -e "-t TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo "--------------------------------------------------------------------------"
    echo "Multi cluster test:"
    echo -e "-m Leader BIND_ADDRESS\t\t, defaults to localhost | 127.0.0.1, can be changed to any IP or domain name for the cluster location."
    echo -e "-n Leader BIND_PORT\t\t, defaults to 9200, can be changed to any port for the cluster location."
    echo -e "-o Leader TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo -e "-x Follower BIND_ADDRESS\t, defaults to localhost | 127.0.0.1, can be changed to any IP or domain name for the cluster location."
    echo -e "-y Follower BIND_PORT\t\t, defaults to 9200, can be changed to any port for the cluster location."
    echo -e "-z Follower TRANSPORT_PORT\t, defaults to 9300, can be changed to any port for the cluster location."
    echo "--------------------------------------------------------------------------"
}

while getopts ":h:b:p:t:m:n:o:x:y:z:s:c:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
         b)
            BIND_ADDRESS=$OPTARG
            ;;
         p)
            BIND_PORT=$OPTARG
            ;;
         t)
            TRANSPORT_PORT=$OPTARG
            ;;
        m)
            LBIND_ADDRESS=$OPTARG
            ;;
        n)
            LBIND_PORT=$OPTARG
            ;;
        o)
            LTRANSPORT_PORT=$OPTARG
            ;;
        x)
             FBIND_ADDRESS=$OPTARG
            ;;
        y)
            FBIND_PORT=$OPTARG
            ;;
        z)
            FTRANSPORT_PORT=$OPTARG
            ;;
        s)
            SECURITY_ENABLED=$OPTARG
            ;;
        c)
            CREDENTIAL=$OPTARG
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

# Common starts
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
# Common ends


# Check if test is run on multiple cluster

if [ -z "$BIND_ADDRESS" ] || [ -z "$BIND_PORT" ]
then
  #Proceeding with multi cluster test
  if [ -z "$LBIND_ADDRESS" ]
  then
    echo "requires an argument -m <Leader bind address>"
    usage
    exit 1
  fi

  if [ -z "$LBIND_PORT" ]
  then
    echo "requires an argument -n <Leader bind port>"
    usage
    exit 1
  fi

  if [ -z "$LTRANSPORT_PORT" ]
  then
    echo "requires an argument -o <Leader transport port>"
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
  eval "./gradlew integTestRemote -Dleader.http_host=\"$LBIND_ADDRESS:$LBIND_PORT\" -Dleader.transport_host=\"$LBIND_ADDRESS:$LTRANSPORT_PORT\" -Dfollower.http_host=\"$FBIND_ADDRESS:$FBIND_PORT\" -Dfollower.transport_host=\"$FBIND_ADDRESS:$FTRANSPORT_PORT\" -Dsecurity_enabled=\"$SECURITY_ENABLED\" -Duser=\"$USERNAME\" -Dpassword=\"$PASSWORD\" --console=plain "

else
  # Single cluster
  if [ -z "$TRANSPORT_PORT" ]
  then
    TRANSPORT_PORT="9300"
  fi
  ./gradlew singleClusterSanity -Dfollower.http_host="$BIND_ADDRESS:$BIND_PORT" -Dfollower.transport_host="$BIND_ADDRESS:$TRANSPORT_PORT" -Dsecurity_enabled=$SECURITY_ENABLED -Duser=$USERNAME -Dpassword=$PASSWORD --console=plain
fi
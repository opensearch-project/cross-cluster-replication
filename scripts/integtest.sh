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
    echo -e "-e Comma seperated endpoint:port, ex: localhost:9200:9300,localhost:9201:9301... ."
    echo "--------------------------------------------------------------------------"
}

while getopts ":h:b:p:t:e:s:c:v:" arg; do
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
        e)
            ENDPOINT_LIST=$OPTARG
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

IFS='.' read -ra version_array <<< "$OPENSEARCH_VERSION"

if [ -z "$CREDENTIAL" ]
then
  CREDENTIAL="admin:admin"
  # Starting in 2.12.0, security demo configuration script requires an initial admin password
  # That same password must be supplied here, else 401 Unauthorized may be seen when running this task
  if (( ${version_array[0]} > 2 || (${version_array[0]} == 2 && ${version_array[1]} >= 12) )); then
    if [ "$SECURITY_ENABLED" == true]; then
      CREDENTIAL="admin:myStrongPassword123!"
      echo "A password is required to execute the integTest. This must the same as the password used to setup admin user."
    fi
fi

USERNAME=`echo $CREDENTIAL | awk -F ':' '{print $1}'`
PASSWORD=`echo $CREDENTIAL | awk -F ':' '{print $2}'`
# Common ends


# Check if test is run on multiple cluster

if [ -z "$BIND_ADDRESS" ] || [ -z "$BIND_PORT" ]
then
  #Proceeding with multi cluster test
  if [ -z "$ENDPOINT_LIST" ]
  then
    echo "requires an argument -e <endpoint:port>"
    usage
    exit 1
  fi

  data=$(python3 -c "import json; cluster=$ENDPOINT_LIST ; data_nodes=cluster; print(data_nodes[0][\"data_nodes\"][0][\"endpoint\"],':',data_nodes[0][\"data_nodes\"][0][\"port\"],':',data_nodes[0][\"data_nodes\"][0][\"transport\"],',',data_nodes[1][\"data_nodes\"][0][\"endpoint\"],':',data_nodes[1][\"data_nodes\"][0][\"port\"],':',data_nodes[1][\"data_nodes\"][0][\"transport\"])" | tr -d "[:blank:]")


  leader=$(echo  $data | cut -d ',' -f1 | cut -d ':' -f1,2 )
  follower=$(echo $data  |  cut -d ',' -f2 | cut -d ':' -f1,2 )
  echo "leader: $leader"
  echo "follower: $follower"
  
  # Get number of nodes, assuming both leader and follower have same number of nodes
  numNodes=$((${follower##*:} - ${leader##*:}))
  echo "numNodes: $numNodes"
  
  LTRANSPORT_PORT=$(echo  $data | cut -d ',' -f1 | cut -d ':' -f1,3 )
  FTRANSPORT_PORT=$(echo $data  |  cut -d ',' -f2 | cut -d ':' -f1,3 )
  echo "LTRANSPORT_PORT: $LTRANSPORT_PORT"
  echo "FTRANSPORT_PORT: $FTRANSPORT_PORT"
  
  eval "./gradlew integTestRemote -Dleader.http_host=\"$leader\" -Dfollower.http_host=\"$follower\" -Dfollower.transport_host=\"$FTRANSPORT_PORT\"  -Dleader.transport_host=\"$LTRANSPORT_PORT\"  -Dsecurity_enabled=\"$SECURITY_ENABLED\" -Duser=\"$USERNAME\" -Dpassword=\"$PASSWORD\" -PnumNodes=$numNodes --console=plain "

else
  # Single cluster
  if [ -z "$TRANSPORT_PORT" ]
  then
    TRANSPORT_PORT="9300"
  fi
  ./gradlew singleClusterSanityTest -Dfollower.http_host="$BIND_ADDRESS:$BIND_PORT" -Dfollower.transport_host="$BIND_ADDRESS:$TRANSPORT_PORT" -Dsecurity_enabled=$SECURITY_ENABLED -Duser=$USERNAME -Dpassword=$PASSWORD --console=plain
fi
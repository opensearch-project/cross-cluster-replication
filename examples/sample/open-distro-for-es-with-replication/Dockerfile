#   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   A copy of the License is located at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   or in the "license" file accompanying this file. This file is distributed
#   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#   express or implied. See the License for the specific language governing
#   permissions and limitations under the License.

FROM amazon/opendistro-for-elasticsearch:1.13.0
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install -v https://github.com/opendistro-for-elasticsearch/cross-cluster-replication/releases/download/v1.13.0.0-experimental/opendistro-cross-cluster-replication-1.13.0.0.zip

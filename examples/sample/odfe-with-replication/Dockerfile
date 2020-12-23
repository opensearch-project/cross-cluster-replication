FROM amazon/opendistro-for-elasticsearch:1.13.0
COPY --chown=elasticsearch:elasticsearch opendistro-cross-cluster-replication-1.13.0.0.zip /tmp/opendistro-cross-cluster-replication-1.13.0.0.zip
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install -v file:///tmp/opendistro-cross-cluster-replication-1.13.0.0.zip
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-performance-analyzer
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-alerting
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-anomaly-detection
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-asynchronous-search
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-index-management
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-reports-scheduler
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-job-scheduler
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-knn
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-sql
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin remove -v opendistro-security
#COPY --chown=elasticsearch:elasticsearch opendistro-cross-cluster-replication-1.13.0.0.zip /tmp/opendistro-cross-cluster-replication-1.13.0.0.zip
#RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install -v file:///tmp/opendistro-cross-cluster-replication-1.13.0.0.zip

FROM grafana/grafana:latest

USER root

# Install Redis datasource plugin
RUN grafana-cli plugins install redis-datasource

# Copy provisioning files
COPY grafana/provisioning /etc/grafana/provisioning

# Ensure proper permissions
RUN chmod -R 755 /etc/grafana/provisioning && \
    chown -R grafana:grafana /etc/grafana/provisioning

USER grafana
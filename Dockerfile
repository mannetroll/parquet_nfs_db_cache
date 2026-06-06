# Lightweight-ish Oracle for dev/testing (Oracle Database Free)
FROM gvenzl/oracle-free:23-slim

# Init scripts: executed once on first DB initialization, in alphabetical order
# SQL scripts run as SYS; if you want app schema objects, you must CONNECT as that user inside the script.
COPY ./init/ /container-entrypoint-initdb.d/

EXPOSE 1521

# Image ships with healthcheck.sh; keep it simple
HEALTHCHECK --interval=10s --timeout=5s --retries=20 CMD ["healthcheck.sh"]

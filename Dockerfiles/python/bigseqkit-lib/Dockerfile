
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}python-builder${TAG}
ARG RELPATH=""

COPY /bigseqkit-py /tmp/src

RUN ${IGNIS_HOME}/bin/ignis-python-install.sh && \
    mkdir -p ${IGNIS_HOME}/core/python/bigseqkit-py && \
    mv /tmp/src/* ${IGNIS_HOME}/core/python/bigseqkit-py && \
    rm -fR /tmp/src && \
    echo "#!/bin/bash\npython3 ${IGNIS_HOME}/core/python/bigseqkit-py/setup.py develop" > ${IGNIS_HOME}/bin/ignis-python-bigseqkit-lib-install.sh && \
    chmod +x ${IGNIS_HOME}/bin/ignis-python-bigseqkit-lib-install.sh


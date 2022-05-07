
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}go-compiler${TAG}
ARG RELPATH=""

COPY / /tmp/src

RUN cd /tmp/src && \
    cd seqkit && \
    go mod edit -replace ignis=${IGNIS_GO}/ignis go.mod && \
    go get && \
    cd .. && \
    cp -R seqkit ${IGNIS_GO}/ && \
    cd seqkit-lib && \
    go mod edit -replace ignis=${IGNIS_GO}/ignis go.mod && \
    go get && \
    go build -buildmode=plugin -trimpath -o ${IGNIS_GO}/seqkit.so && \
    rm -fR /tmp/src

	
RUN echo "#!/bin/bash" > ${IGNIS_HOME}/bin/ignis-go-seqkit-lib-install.sh && \
	chmod +x ${IGNIS_HOME}/bin/ignis-go-seqkit-lib-install.sh


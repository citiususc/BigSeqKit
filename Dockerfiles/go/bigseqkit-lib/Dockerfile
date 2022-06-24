
ARG REGISTRY=""
ARG NAMESPACE="ignishpc/"
ARG TAG=""
FROM ${REGISTRY}${NAMESPACE}go-compiler${TAG}
ARG RELPATH=""

COPY / /tmp/src

RUN cd /tmp/src && \
    cd bigseqkit && \
    go mod edit -replace ignis=${IGNIS_GO}/ignis go.mod && \
    go get && \
    cd .. && \
    cp -R bigseqkit ${IGNIS_GO}/ && \
    cd bigseqkit-lib && \
    go mod edit -replace ignis=${IGNIS_GO}/ignis go.mod && \
    go get && \
    go build -buildmode=plugin -trimpath -o ${IGNIS_GO}/bigseqkit.so && \
    rm -fR /tmp/src

COPY ${RELPATH}igo-bigseqkit-build $IGNIS_HOME/bin/igo-bigseqkit-build
RUN echo "#!/bin/bash" > ${IGNIS_HOME}/bin/ignis-go-bigseqkit-lib-install.sh && \
	chmod +x ${IGNIS_HOME}/bin/ignis-go-bigseqkit-lib-install.sh && \
    chmod +x $IGNIS_HOME/bin/igo-bigseqkit-build


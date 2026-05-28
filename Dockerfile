### Creates a storage Docker image with openGauss, pageserver, safekeeper and proxy binaries.
### The image itself is mainly used as a container for the binaries and for starting e2e tests with custom parameters.
### By default, the binaries inside the image have some mock parameters and can start, but are not intended to be used
### inside this image in the real deployments.
ARG REPOSITORY=ghcr.io/neondatabase
ARG IMAGE=build-tools
ARG TAG=pinned
ARG APT_DEBIAN_MIRROR=
ARG APT_SECURITY_MIRROR=
ARG CARGO_REGISTRY_MIRROR=
ARG DEBIAN_VERSION=bookworm
ARG DEBIAN_FLAVOR=${DEBIAN_VERSION}-slim
ARG OPENGAUSS_BINARYLIBS_DIR

# Here are the INDEX DIGESTS for the images we use.
ARG BOOKWORM_SLIM_SHA=sha256:40b107342c492725bc7aacbe93a49945445191ae364184a6d24fedb28172f6f7
ARG BULLSEYE_SLIM_SHA=sha256:e831d9a884d63734fe3dd9c491ed9a5a3d4c6a6d32c5b14f2067357c49b0b7e1

# Here we use ${var/search/replace} syntax, to check
# if base image is one of the images, we pin image index for.
ARG BASE_IMAGE_SHA=debian:${DEBIAN_FLAVOR}
ARG BASE_IMAGE_SHA=${BASE_IMAGE_SHA/debian:bookworm-slim/debian@$BOOKWORM_SLIM_SHA}
ARG BASE_IMAGE_SHA=${BASE_IMAGE_SHA/debian:bullseye-slim/debian@$BULLSEYE_SLIM_SHA}

# 1. Build openGauss
FROM openeuler/openeuler:22.03-lts AS og-build

USER root
RUN rm -f /etc/yum.repos.d/*.repo
COPY openEuler_aarch64.repo /etc/yum.repos.d/openEuler_aarch64.repo
RUN set -e \
    && yum makecache \
    && yum install -y \
        autoconf automake bison ccache cmake dkms flex gcc gcc-c++ git java-1.8.0-openjdk-devel \
        libaio-devel libcurl-devel libedit-devel libtool libxml2-devel libxslt-devel lz4-devel make \
        ncurses-devel numactl-devel openblas-devel openssl-devel patch perl python3 python3-devel readline-devel \
        tar unzip util-linux-devel wget which zlib-devel zstd zstd-devel unixODBC-devel \
    && ln -sfn /usr/lib/jvm/java /usr/lib/jvm/default-java \
    && yum clean all \
    && rm -rf /var/cache/yum \
    && useradd -ms /bin/bash nonroot -b /home
USER nonroot

WORKDIR /home/nonroot

COPY --chown=nonroot vendor/openGauss vendor/openGauss
COPY --chown=nonroot Makefile Makefile
COPY --chown=nonroot opengauss.mk opengauss.mk
COPY --chown=nonroot scripts/ninstall.sh scripts/ninstall.sh

ARG OPENGAUSS_BINARYLIBS_DIR
RUN set -e \
    && curl -SL -o /tmp/binarylibs.tar.gz https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz \
    && tar -xzf /tmp/binarylibs.tar.gz \
    && mv openGauss-third_party_binarylibs_openEuler_2203_arm 3rd_og \
    && rm -f /tmp/binarylibs.tar.gz
ENV BUILD_TYPE=release
ENV OPENGAUSS_BINARYLIBS_DIR=/home/nonroot/3rd_og
RUN set -e \
    && make -j $(nproc) -s opengauss-install

# 2. Prepare cargo-chef recipe
FROM $REPOSITORY/$IMAGE:$TAG AS plan
ARG CARGO_REGISTRY_MIRROR
WORKDIR /home/nonroot

COPY --chown=nonroot . .
COPY --chown=nonroot .cargo-cache/git/      /home/nonroot/.cargo/git/
COPY --chown=nonroot .cargo-cache/registry/ /home/nonroot/.cargo/registry/

RUN set -e \
    && mkdir -p /home/nonroot/.cargo \
    && { \
        if [ -n "${CARGO_REGISTRY_MIRROR}" ]; then \
            printf '[source.crates-io]\nreplace-with = "mirror"\n\n[source.mirror]\nregistry = "%s"\n\n' "${CARGO_REGISTRY_MIRROR}"; \
        fi; \
        printf '[net]\noffline = true\ngit-fetch-with-cli = true\nretry = 10\n\n[http]\ntimeout = 600\nmultiplexing = false\n'; \
    } > /home/nonroot/.cargo/config.toml \
    && git config --global http.version HTTP/1.1 \
    && git config --global http.lowSpeedLimit 1 \
    && git config --global http.lowSpeedTime 600

RUN --mount=type=secret,uid=1000,id=SUBZERO_ACCESS_TOKEN \
    set -e \
    && if [ -s /run/secrets/SUBZERO_ACCESS_TOKEN ]; then \
        export CARGO_NET_GIT_FETCH_WITH_CLI=true && \
        git config --global url."https://$(cat /run/secrets/SUBZERO_ACCESS_TOKEN)@github.com/neondatabase/subzero".insteadOf "https://github.com/neondatabase/subzero" && \
        cargo add -p proxy subzero-core --git https://github.com/neondatabase/subzero --rev 396264617e78e8be428682f87469bb25429af88a; \
    fi \
    && cargo chef prepare --recipe-path recipe.json

# Main build image
FROM $REPOSITORY/$IMAGE:$TAG AS build
ARG APT_DEBIAN_MIRROR
ARG APT_SECURITY_MIRROR

USER root
RUN set -e \
    && mkdir -p /etc/apt/sources.list.d.disabled \
    && for list in docker.list nodesource.list llvm.stable.list; do \
        mv /etc/apt/sources.list.d/$list /etc/apt/sources.list.d.disabled/ 2>/dev/null || true; \
    done \
    && if [ -n "${APT_DEBIAN_MIRROR}" ]; then \
        security_mirror="${APT_SECURITY_MIRROR:-${APT_DEBIAN_MIRROR}-security}"; \
        sed -i \
            -e "s|http://deb.debian.org/debian-security|${security_mirror}|g" \
            -e "s|http://deb.debian.org/debian|${APT_DEBIAN_MIRROR}|g" \
            /etc/apt/sources.list.d/debian.sources; \
    fi \
    && apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 update \
    && apt-get install -y --no-install-recommends libaio-dev libkrb5-dev lsb-release \
    && rm -rf /var/lib/apt/lists/*
USER nonroot

WORKDIR /home/nonroot
ARG GIT_VERSION=local
ARG BUILD_TAG
ARG ADDITIONAL_RUSTFLAGS=""
ARG CARGO_REGISTRY_MIRROR
ENV CARGO_FEATURES="default"
ENV CARGO_NET_OFFLINE=true
ENV CARGO_NET_RETRY=10
ENV CARGO_HTTP_TIMEOUT=600

# 3. Build cargo dependencies. Note that this step doesn't depend on anything else than
# `recipe.json`, so the layer can be reused as long as none of the dependencies change.
# We use --no-build to prevent cargo-chef from overwriting vendor source files with dummy stubs.
# Vendor dirs must be copied before cargo chef cook because they are [patch.crates-io] path deps,
# not workspace members, so cargo-chef does not create them from recipe.json.
COPY --chown=nonroot --from=plan     /home/nonroot/recipe.json                              recipe.json
COPY --chown=nonroot --from=plan     /home/nonroot/.cargo/git/                              /home/nonroot/.cargo/git/
COPY --chown=nonroot --from=plan     /home/nonroot/.cargo/registry/                         /home/nonroot/.cargo/registry/
COPY --chown=nonroot --from=plan     /home/nonroot/vendor/diesel-async-0.5.2/               vendor/diesel-async-0.5.2/
COPY --chown=nonroot --from=plan     /home/nonroot/vendor/openGauss-connector-rust/         vendor/openGauss-connector-rust/
RUN set -e \
    && mkdir -p /home/nonroot/.cargo \
    && { \
        if [ -n "${CARGO_REGISTRY_MIRROR}" ]; then \
            printf '[source.crates-io]\nreplace-with = "mirror"\n\n[source.mirror]\nregistry = "%s"\n\n' "${CARGO_REGISTRY_MIRROR}"; \
        fi; \
        printf '[net]\noffline = true\ngit-fetch-with-cli = true\nretry = 10\n\n[http]\ntimeout = 600\nmultiplexing = false\n'; \
    } > /home/nonroot/.cargo/config.toml \
    && git config --global http.version HTTP/1.1 \
    && git config --global http.lowSpeedLimit 1 \
    && git config --global http.lowSpeedTime 600
RUN --mount=type=secret,uid=1000,id=SUBZERO_ACCESS_TOKEN \
    set -e \
    && if [ -s /run/secrets/SUBZERO_ACCESS_TOKEN ]; then \
        export CARGO_NET_GIT_FETCH_WITH_CLI=true && \
        git config --global url."https://$(cat /run/secrets/SUBZERO_ACCESS_TOKEN)@github.com/neondatabase/subzero".insteadOf "https://github.com/neondatabase/subzero"; \
    fi \
    && RUSTFLAGS="-Clinker=clang -Clink-arg=-fuse-ld=mold -Clink-arg=-Wl,--no-rosegment -Cforce-frame-pointers=yes ${ADDITIONAL_RUSTFLAGS}" cargo chef cook --no-build --release --recipe-path recipe.json

# cargo chef cook --no-build creates dummy source files for workspace members.
# Restore actual vendor source to overwrite any dummies.
COPY --chown=nonroot --from=plan     /home/nonroot/vendor/diesel-async-0.5.2/               vendor/diesel-async-0.5.2/
COPY --chown=nonroot --from=plan     /home/nonroot/vendor/openGauss-connector-rust/         vendor/openGauss-connector-rust/

# Copy openGauss install before building, so postgres_ffi build.rs can find headers.
COPY --chown=nonroot --from=og-build /home/nonroot/og_install/ og_install
COPY --chown=nonroot --from=og-build /home/nonroot/3rd_og/ 3rd_og/
ENV OPENGAUSS_INSTALL_DIR=/home/nonroot/og_install
ENV OPENGAUSS_BINARYLIBS_DIR=/home/nonroot/3rd_og

RUN --mount=type=secret,uid=1000,id=SUBZERO_ACCESS_TOKEN \
    set -e \
    && rm -f og_install/V702/bin/pg_config \
    && if [ -s /run/secrets/SUBZERO_ACCESS_TOKEN ]; then \
        export CARGO_NET_GIT_FETCH_WITH_CLI=true && \
        git config --global url."https://$(cat /run/secrets/SUBZERO_ACCESS_TOKEN)@github.com/neondatabase/subzero".insteadOf "https://github.com/neondatabase/subzero"; \
    fi \
    && for attempt in 1 2 3 4 5; do \
        RUSTFLAGS="-Clinker=clang -Clink-arg=-fuse-ld=mold -Clink-arg=-Wl,--no-rosegment -Cforce-frame-pointers=yes ${ADDITIONAL_RUSTFLAGS}" cargo build --release && break; \
        status=$?; \
        if [ "$attempt" = 5 ]; then exit "$status"; fi; \
        sleep $((attempt * 30)); \
    done

# Perform the main build. We reuse the cargo dependencies built in the previous step.
COPY --chown=nonroot . .
COPY --chown=nonroot --from=plan     /home/nonroot/proxy/Cargo.toml         proxy/Cargo.toml
COPY --chown=nonroot --from=plan     /home/nonroot/Cargo.lock               Cargo.lock
RUN set -e \
    && mkdir -p /home/nonroot/.cargo \
    && { \
        if [ -n "${CARGO_REGISTRY_MIRROR}" ]; then \
            printf '[source.crates-io]\nreplace-with = "mirror"\n\n[source.mirror]\nregistry = "%s"\n\n' "${CARGO_REGISTRY_MIRROR}"; \
        fi; \
        printf '[net]\noffline = true\ngit-fetch-with-cli = true\nretry = 10\n\n[http]\ntimeout = 600\nmultiplexing = false\n'; \
    } > /home/nonroot/.cargo/config.toml \
    && git config --global http.version HTTP/1.1 \
    && git config --global http.lowSpeedLimit 1 \
    && git config --global http.lowSpeedTime 600

RUN  --mount=type=secret,uid=1000,id=SUBZERO_ACCESS_TOKEN \
    set -e \
    && if [ -s /run/secrets/SUBZERO_ACCESS_TOKEN ]; then \
        export CARGO_FEATURES="rest_broker"; \
    fi \
    && for attempt in 1 2 3 4 5; do \
        RUSTFLAGS="-Clinker=clang -Clink-arg=-fuse-ld=mold -Clink-arg=-Wl,--no-rosegment -Cforce-frame-pointers=yes ${ADDITIONAL_RUSTFLAGS}" cargo build \
          --features $CARGO_FEATURES \
          --bin pg_sni_router  \
          --bin pageserver  \
          --bin pagectl  \
          --bin safekeeper  \
          --bin storage_broker  \
          --bin storage_controller  \
          --bin proxy  \
          --bin endpoint_storage \
          --bin neon_local \
          --bin storage_scrubber \
          --locked --release && break; \
        status=$?; \
        if [ "$attempt" = 5 ]; then exit "$status"; fi; \
        sleep $((attempt * 30)); \
      done

# Free cargo intermediates before building the openGauss extension. The final
# binaries above remain in target/release and are copied into the runtime image.
RUN rm -rf target/release/deps target/release/build target/release/.fingerprint target/release/incremental

COPY --chown=nonroot --from=og-build /home/nonroot/og_install/V702/bin/pg_config og_install/V702/bin/pg_config

RUN mkdir -p og_install/V702/include/postgresql/include \
    && cp og_install/V702/include/securec*.h og_install/V702/include/postgresql/include/ \
    && cp -a og_install/V702/include/postgresql/server/. og_install/V702/include/postgresql/include/

RUN mold -run env \
    GAUSSHOME=/home/nonroot/og_install/V702 \
    PATH="/home/nonroot/og_install/V702/bin:$PATH" \
    OG_INSTALL_CACHED=1 \
    make -j 1 -s neon-pg-ext COPT="-w" PG_CONFIG_ENV="GAUSSHOME=/home/nonroot/og_install/V702"

# Assemble the final image
FROM openeuler/openeuler:22.03-lts
WORKDIR /data

RUN rm -f /etc/yum.repos.d/*.repo
COPY openEuler_aarch64.repo /etc/yum.repos.d/openEuler_aarch64.repo

RUN set -e \
    && yum makecache \
    && yum install -y \
        readline-devel \
        libseccomp-devel \
        ca-certificates \
        openssl \
        unzip \
        curl \
        procps-ng \
        vim \
        libaio-devel \
        numactl-devel \
        lapack \
        openblas \
        libxml2 \
        shadow \
    && ARCH=$(uname -m) \
    && if [ "$ARCH" = "x86_64" ]; then \
        curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf aws awscliv2.zip \
    && yum clean all \
    && rm -rf /var/cache/yum /tmp/* /var/tmp/* \
    && printf '%s\n' '#!/usr/bin/env sh' 'exec ls -alF "$@"' > /usr/local/bin/ll \
    && chmod 0755 /usr/local/bin/ll \
    && useradd -d /data neon \
    && chown -R neon:neon /data

COPY --from=build --chown=neon:neon /home/nonroot/target/release/pg_sni_router       /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/pageserver          /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/pagectl             /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/safekeeper          /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/storage_broker      /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/storage_controller  /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/proxy               /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/endpoint_storage    /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/neon_local          /usr/local/bin
COPY --from=build --chown=neon:neon /home/nonroot/target/release/storage_scrubber    /usr/local/bin
COPY --from=build /home/nonroot/og_install/V702 /usr/local/V702/

RUN set -e \
    && mv /usr/local/V702/bin/gaussdb /usr/local/V702/bin/gaussdb.real \
    && printf '%s\n' \
        '#!/usr/bin/env sh' \
        'export LD_LIBRARY_PATH="/usr/lib64:/usr/local/V702/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"' \
        'export LD_PRELOAD="/usr/lib64/libopenblas.so.0:/usr/lib64/liblapack.so.3:/usr/lib64/liblapacke.so.3${LD_PRELOAD:+:${LD_PRELOAD}}"' \
        'exec /usr/local/V702/bin/gaussdb.real "$@"' \
        > /usr/local/V702/bin/gaussdb \
    && chmod 0755 /usr/local/V702/bin/gaussdb

RUN tar -C /usr/local -cvzf /data/opengauss_install.tar.gz V702

# By default, pageserver uses `.neon/pageserver/` working directory in WORKDIR, so create one and fill it with the dummy config.
RUN mkdir -p /data/.neon/pageserver/ && \
  echo "id=1234" > "/data/.neon/pageserver/identity.toml" && \
  printf "%s\n" \
       "broker_endpoint='http://storage_broker:50051'" \
       "pg_distrib_dir='/usr/local/'" \
       "listen_pg_addr='0.0.0.0:6400'" \
       "listen_http_addr='0.0.0.0:9898'" \
       "availability_zone='local'" \
       "remote_storage={local_path='/data/.neon/pageserver/remote_storage'}" \
  > /data/.neon/pageserver/pageserver.toml && \
  chown -R neon:neon /data/.neon

VOLUME ["/data"]
USER neon
ENV OG_VERSION=V702
ENV PATH="/usr/local/V702/bin:${PATH}"
EXPOSE 6400
EXPOSE 9898

CMD ["/usr/local/bin/pageserver", "-D", "/data/.neon/pageserver"]

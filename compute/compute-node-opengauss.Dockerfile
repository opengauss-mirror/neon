#
# This Dockerfile builds the compute image for openGauss.
# It produces images for each supported openGauss version.
#
# ## Intermediary layers
#
# build-tools:   This contains Rust compiler toolchain and other tools needed at compile
#                time. This image is defined in build-tools/Dockerfile.
#
# build-deps:    Contains C compiler, other build tools, and compile-time dependencies
#                needed to compile openGauss.
#
# og-build:      Result of compiling openGauss. The openGauss binaries are copied from
#                this to the final image.
#
# compute-tools: This contains compute_ctl, the launcher program that starts openGauss
#                in Neon.
#
# ## Final image
#
# The final image puts together the openGauss binaries (og-build), the compute tools
# (compute-tools) into one image.

ARG OG_VERSION
ARG BUILD_TAG
ARG DEBIAN_VERSION=bookworm
ARG DEBIAN_FLAVOR=${DEBIAN_VERSION}-slim
ARG OPENGAUSS_BINARYLIBS_DIR=3rd_og
ARG APT_DEBIAN_MIRROR=
ARG APT_SECURITY_MIRROR=
ARG CARGO_REGISTRY_MIRROR=

ARG BOOKWORM_SLIM_SHA=sha256:40b107342c492725bc7aacbe93a49945445191ae364184a6d24fedb28172f6f7
ARG BULLSEYE_SLIM_SHA=sha256:e831d9a884d63734fe3dd9c491ed9a5a3d4c6a6d32c5b14f2067357c49b0b7e1

ARG BASE_IMAGE_SHA=debian:${DEBIAN_FLAVOR}
ARG BASE_IMAGE_SHA=${BASE_IMAGE_SHA/debian:bookworm-slim/debian@$BOOKWORM_SLIM_SHA}
ARG BASE_IMAGE_SHA=${BASE_IMAGE_SHA/debian:bullseye-slim/debian@$BULLSEYE_SLIM_SHA}

ARG REPOSITORY=ghcr.io/neondatabase
ARG IMAGE=build-tools
ARG TAG=pinned
ARG NEON_IMAGE=neon:latest_opgs

#########################################################################################
#
# Layer "build-deps"
#
#########################################################################################
FROM openeuler/openeuler:22.03-lts AS build-deps

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN rm -f /etc/yum.repos.d/*.repo
COPY openEuler_aarch64.repo /etc/yum.repos.d/openEuler_aarch64.repo

RUN yum makecache && \
    yum install -y \
    ninja-build git autoconf automake libtool make gcc gcc-c++ bison flex readline-devel \
    zlib-devel libxml2-devel libcurl-devel wget ca-certificates pkgconfig openssl-devel \
    libicu-devel libxslt-devel lz4-devel libzstd-devel zstd curl unzip \
    cmake libaio-devel numactl-devel libuuid-devel \
    && yum clean all && rm -rf /var/cache/yum \
    && useradd -ms /bin/bash nonroot -b /home

#########################################################################################
#
# Layer "og-build"
# Build openGauss from the neon openGauss repository.
#
#########################################################################################
FROM openeuler/openeuler:22.03-lts AS og-build
ARG OG_VERSION
ARG OPENGAUSS_BINARYLIBS_DIR

USER root
RUN rm -f /etc/yum.repos.d/*.repo
COPY openEuler_aarch64.repo /etc/yum.repos.d/openEuler_aarch64.repo
RUN yum makecache \
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

ENV BUILD_TYPE=release
ENV OPENGAUSS_BINARYLIBS_DIR=/home/nonroot/3rd_og
RUN set -e \
    && curl -SL -o /tmp/binarylibs.tar.gz https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/binarylibs/gcc10.3/openGauss-third_party_binarylibs_openEuler_2203_arm.tar.gz \
    && tar -xzf /tmp/binarylibs.tar.gz \
    && mv openGauss-third_party_binarylibs_openEuler_2203_arm 3rd_og \
    && rm -f /tmp/binarylibs.tar.gz
RUN set -e \
    && make -j $(nproc) -s opengauss-install

#########################################################################################
#
# Layer "compute-tools"
# Build compute_ctl and other tools.
#
#########################################################################################
FROM $REPOSITORY/$IMAGE:$TAG AS compute-tools
ARG CARGO_REGISTRY_MIRROR
WORKDIR /home/nonroot

COPY --chown=nonroot . .
COPY --chown=nonroot .cargo-cache/git/      /home/nonroot/.cargo/git/
COPY --chown=nonroot .cargo-cache/registry/ /home/nonroot/.cargo/registry/
COPY --chown=nonroot --from=og-build /home/nonroot/og_install/ og_install

ENV OPENGAUSS_INSTALL_DIR=/home/nonroot/og_install
ENV BUILD_TYPE=release

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
    && git config --global http.lowSpeedTime 600 \
    && chown -R nonroot:nonroot /home/nonroot/.cargo

ENV CARGO_NET_OFFLINE=true
ENV CARGO_NET_RETRY=10
ENV CARGO_HTTP_TIMEOUT=600

RUN set -e \
    && for attempt in 1 2 3 4 5; do \
        RUSTFLAGS="-Clinker=clang -Clink-arg=-fuse-ld=mold -Clink-arg=-Wl,--no-rosegment -Cforce-frame-pointers=yes" \
        cargo build --release --bin compute_ctl --bin fast_import --locked && break; \
        status=$?; \
        if [ "$attempt" = 5 ]; then exit "$status"; fi; \
        sleep $((attempt * 30)); \
    done \
    && mkdir -p /home/nonroot/target-bin \
    && cp target/release/compute_ctl /home/nonroot/target-bin/ \
    && cp target/release/fast_import /home/nonroot/target-bin/

#########################################################################################
#
# Layer "neon_extensions"
# Extract pre-built Neon extensions from the neon image.
#
#########################################################################################
FROM ${NEON_IMAGE} AS neon_extensions

#########################################################################################
#
# Final layer
# Put it all together into the final image
#
#########################################################################################
FROM openeuler/openeuler:22.03-lts
ARG OG_VERSION

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN rm -f /etc/yum.repos.d/*.repo
COPY openEuler_aarch64.repo /etc/yum.repos.d/openEuler_aarch64.repo

RUN yum makecache && \
    yum install -y \
        ca-certificates \
        gdb \
        iproute \
        libcurl \
        libevent \
        lz4 \
        libuuid \
        readline \
        libxml2 \
        libxslt \
        libzstd \
        libaio \
        libedit \
        lapack \
        ncurses-libs \
        numactl-libs \
        openblas \
        glibc-common \
        glibc-locale-source \
        perl \
        lsof \
        procps \
        screen \
        tcpdump \
        vim \
        jq \
        nmap \
        libicu \
        shadow \
        findutils \
    && localedef -i en_US -c -f UTF-8 en_US.UTF-8 \
    && yum clean all && rm -rf /var/cache/yum /tmp/* /var/tmp/* \
    && printf '%s\n' '#!/usr/bin/env sh' 'exec ls -alF "$@"' > /usr/local/bin/ll \
    && chmod 0755 /usr/local/bin/ll

RUN groupadd -g 1000 omm && useradd -m -u 1000 -g omm -d /var/db/omm omm && \
    mkdir -p /var/db/omm /var/db/gaussdb/compute /var/db/gaussdb/specs && \
    chown -R omm:omm /var/db/omm && \
    chown -R omm:omm /var/db/gaussdb && \
    chmod 0750 /var/db/gaussdb/compute && \
    mkdir -p -m 777 /neon/cache

COPY --from=og-build /home/nonroot/og_install/${OG_VERSION} /usr/local/${OG_VERSION}
COPY --from=compute-tools --chown=omm /home/nonroot/target-bin/compute_ctl /usr/local/bin/compute_ctl
COPY --from=compute-tools --chown=omm /home/nonroot/target-bin/fast_import /usr/local/bin/fast_import

RUN set -e \
    && mkdir -p /usr/local/${OG_VERSION}/lib/disabled-system-libs \
    && for f in /usr/local/${OG_VERSION}/lib/libstdc++.so*; do [ ! -e "$f" ] || mv "$f" /usr/local/${OG_VERSION}/lib/disabled-system-libs/; done \
    && mv /usr/local/${OG_VERSION}/bin/gaussdb /usr/local/${OG_VERSION}/bin/gaussdb.real \
    && printf '%s\n' \
        '#!/usr/bin/env sh' \
        'export LD_LIBRARY_PATH="/usr/lib64:/usr/local/${OG_VERSION}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"' \
        'export LD_PRELOAD="/usr/lib64/libopenblas.so.0:/usr/lib64/liblapack.so.3:/usr/lib64/liblapacke.so.3${LD_PRELOAD:+:${LD_PRELOAD}}"' \
        'exec /usr/local/${OG_VERSION}/bin/gaussdb.real "$@"' \
        > /usr/local/${OG_VERSION}/bin/gaussdb \
    && chmod 0755 /usr/local/${OG_VERSION}/bin/gaussdb \
    && ln -sf /usr/local/${OG_VERSION}/bin/gs_ctl /usr/local/${OG_VERSION}/bin/pg_ctl \
    && mkdir -p /var/db/gaussdb/compute \
    && chown -R omm:omm /var/db/gaussdb \
    && echo /usr/local/${OG_VERSION}/lib > /etc/ld.so.conf.d/00-neon.conf \
    && /sbin/ldconfig

COPY --from=neon_extensions /usr/local/V702/lib/postgresql/neon*.so /usr/local/${OG_VERSION}/lib/postgresql/
COPY --from=neon_extensions /usr/local/V702/share/postgresql/extension/neon* /usr/local/${OG_VERSION}/share/postgresql/extension/

RUN set -e \
    && LC_ALL=C sed -i 's/SELECT FROM/SELECT*FROM/g' /usr/local/bin/compute_ctl \
    && LC_ALL=C sed -i 's/BYPASSRLS/         /g; s/NOBYPASSRLS/           /g' /usr/local/bin/compute_ctl \
    && p="starts_with(rolname, 'pg_')" \
    && r=$(printf "%-${#p}s" "rolname LIKE 'pg_%'") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && perl -0777 -pi -e 'BEGIN { $p = "EXECUTE '\''ALTER ROLE '\'' || quote_ident(role_name) || '\'' INHERIT'\'';"; $r = "NULL;"; die "replacement is longer" if length($r) > length($p); $r .= " " x (length($p) - length($r)); } s/\Q$p\E/$r/g' /usr/local/bin/compute_ctl \
    && perl -0777 -pi -e 'BEGIN { $p = "EXECUTE '\''ALTER ROLE '\'' || quote_ident(role_name) || '\'' NO         '\'';"; $r = "NULL;"; die "replacement is longer" if length($r) > length($p); $r .= " " x (length($p) - length($r)); } s/\Q$p\E/$r/g' /usr/local/bin/compute_ctl \
    && p='GRANT pg_monitor TO {privileged_role_name} WITH ADMIN OPTION;' \
    && r=$(printf "%-${#p}s" "-- SKIP pg_monitor") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && p='GRANT pg_monitor TO  WITH ADMIN OPTION;' \
    && r=$(printf "%-${#p}s" "-- SKIP pg_monitor") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && p='INSERT INTO neon_migration.migration_id VALUES (0, 0) ON CONFLICT DO NOTHING' \
    && r=$(printf "%-${#p}s" "INSERT INTO neon_migration.migration_id VALUES (0, 0)") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && perl -0777 -pi -e 'BEGIN { $p = "INSERT INTO health_check VALUES (1, now())\n        ON CONFLICT (id) DO UPDATE\n         SET updated_at = now();"; $r = "DELETE FROM health_check WHERE id = 1; INSERT INTO health_check VALUES (1, now());"; die "replacement is longer" if length($r) > length($p); $r .= " " x (length($p) - length($r)); } s/\Q$p\E/$r/g' /usr/local/bin/compute_ctl \
    && perl -0777 -pi -e 'BEGIN { $p = "INSERT INTO neon.drop_subscriptions_done VALUES (1, current_setting('"'"'neon.timeline_id'"'"'))\n    ON CONFLICT (id) DO UPDATE\n    SET timeline_id = current_setting('"'"'neon.timeline_id'"'"');"; $r = "DELETE FROM neon.drop_subscriptions_done WHERE id = 1; INSERT INTO neon.drop_subscriptions_done VALUES (1, current_setting('"'"'neon.timeline_id'"'"'));"; die "replacement is longer" if length($r) > length($p); $r .= " " x (length($p) - length($r)); } s/\Q$p\E/$r/g' /usr/local/bin/compute_ctl \
    && p='CREATE EXTENSION IF NOT EXISTS neon WITH SCHEMA neon' \
    && r=$(printf "%-${#p}s" "SELECT 1") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && p='ALTER EXTENSION neon SET SCHEMA neon' \
    && r=$(printf "%-${#p}s" "SELECT 1") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && p='ALTER EXTENSION neon UPDATE' \
    && r=$(printf "%-${#p}s" "SELECT 1") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && p='GRANT EXECUTE ON FUNCTION pg_show_replication_origin_status TO ' \
    && r=$(printf "%-${#p}s" "-- SKIP pg_show_replication_origin_status") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && p='GRANT pg_signal_backend TO ' \
    && r=$(printf "%-${#p}s" "-- SKIP pg_signal_backend") \
    && LC_ALL=C sed -i "s|$p|$r|g" /usr/local/bin/compute_ctl \
    && sed -i -E 's/[[:space:]]*PARALLEL (UNSAFE|SAFE)//g' /usr/local/${OG_VERSION}/share/postgresql/extension/neon*.sql \
    && sed -i -E '/TO [Pp][Gg]_[Mm][Oo][Nn][Ii][Tt][Oo][Rr];/d' /usr/local/${OG_VERSION}/share/postgresql/extension/neon*.sql

COPY --chown=omm:omm compute/gaussdb/configs/ /var/db/gaussdb/configs/
COPY --chown=omm:omm compute/shell/ /shell/

ENV LANG=en_US.utf8
ENV OG_VERSION=${OG_VERSION}
ENV PATH="/usr/local/${OG_VERSION}/bin:${PATH}"
USER omm
ENTRYPOINT ["/usr/local/bin/compute_ctl"]

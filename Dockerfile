# syntax=docker/dockerfile:1.7

ARG UBUNTU_VERSION=26.04
ARG NODE_VERSION=22

FROM node:${NODE_VERSION}-bookworm-slim AS web-builder

WORKDIR /src/web-ui
COPY web-ui/package.json web-ui/package-lock.json ./
RUN npm ci --include=dev
COPY web-ui/ ./
RUN npm run build

FROM ubuntu:${UBUNTU_VERSION} AS builder

ARG UBUNTU_VERSION
ARG BUILD2_VERSION=0.17.0
ARG BUILD2_REPO_FINGERPRINT=70:64:FE:E4:E0:F3:60:F1:B4:51:E1:FA:12:5C:E0:B3:DB:DF:96:33:39:B9:2E:E5:C2:68:63:4C:A6:47:39:43
ARG BUILD_JOBS=0

ENV DEBIAN_FRONTEND=noninteractive
ENV CC=gcc-15
ENV CXX=g++-15
ENV PATH=/usr/local/bin:/root/.local/bin:${PATH}
ENV BUILD2_CONFIG_ROOT=/tmp/build2-configs
ENV BUILD2_CONFIG_NAME=docker-release
ENV BUILD2_CONFIG_DIR=/tmp/build2-configs/dagforge-docker-release
ENV BUILD2_CONFIG_DEFAULT=0
ENV BUILD2_CONFIG_FORWARD=0
ENV BUILD2_BIN_LIB=static

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gcc-15 \
    g++-15 \
    git \
    libboost-dev \
    libboost-charconv-dev \
    libboost-filesystem-dev \
    libboost-process-dev \
    libboost-system-dev \
    libboost-url-dev \
    libssl-dev \
    liburing-dev \
    make \
    pkg-config \
    util-linux \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSLO "https://download.build2.org/${BUILD2_VERSION}/build2-install-${BUILD2_VERSION}.sh" \
    && sh "build2-install-${BUILD2_VERSION}.sh" \
         --yes \
         --sudo false \
         --no-modules \
         --local \
         --cxx g++-15 \
         --trust "${BUILD2_REPO_FINGERPRINT}" \
    && rm -f "build2-install-${BUILD2_VERSION}.sh"

WORKDIR /src

COPY README.md README_CN.md LICENSE manifest repositories.manifest buildfile ./
COPY build ./build
COPY bin ./bin
COPY include ./include
COPY scripts ./scripts
COPY src ./src
COPY tests ./tests
COPY third_party ./third_party
COPY dags ./dags
COPY system_config.toml ./system_config.toml

RUN bash scripts/check-module-graph.sh

RUN jobs="${BUILD_JOBS}" \
    && if [ "${jobs}" = "0" ]; then jobs="$(nproc)"; fi \
    && BUILD2_JOBS="${jobs}" \
       BUILD2_TARGETS='bin/exe{dagforge}' \
       bash scripts/build.sh

COPY --from=web-builder /src/web-ui/dist /release/web-ui-dist

RUN set -eux; \
    install -d /release/bin /release/dags; \
    install -m 0755 \
      "${BUILD2_CONFIG_DIR}/dagforge/bin/dagforge" \
      /release/bin/dagforge; \
    cp -a dags/. /release/dags/; \
    install -m 0644 system_config.toml /release/system_config.toml; \
    install -m 0644 README.md /release/README.md; \
    install -m 0644 README_CN.md /release/README_CN.md; \
    install -m 0644 LICENSE /release/LICENSE; \
    { \
      printf 'base_image=ubuntu:%s\n' "${UBUNTU_VERSION}"; \
      printf 'compiler='; g++-15 --version | head -n 1; \
      printf 'build2='; b --version | head -n 1; \
      awk '$1 == "#define" && $2 == "BOOST_LIB_VERSION" { \
        gsub(/\"/, "", $3); print "boost=" $3 \
      }' /usr/include/boost/version.hpp; \
      printf 'project_linkage=static\n'; \
    } > /release/BUILD-INFO; \
    ldd /release/bin/dagforge \
      | awk '/=>/ {print $1} /^[[:space:]]*\// {print $1}' \
      | sort -u > /release/RUNTIME-DEPENDENCIES; \
    if readelf -d /release/bin/dagforge | grep -Fq 'libdagforge.so'; then \
      echo 'release binary depends on build-tree libdagforge.so' >&2; \
      exit 1; \
    fi

FROM ubuntu:${UBUNTU_VERSION} AS release-verify

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libboost-charconv1.90.0 \
    libboost-filesystem1.90.0 \
    libboost-process1.90.0 \
    libboost-url1.90.0 \
    libquadmath0 \
    libssl3t64 \
    libstdc++6 \
    tzdata \
    zlib1g \
    libzstd1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagforge
COPY --from=builder /release/ /opt/dagforge/

RUN set -eux; \
    if ldd ./bin/dagforge | grep -Fq 'not found'; then \
      ldd ./bin/dagforge >&2; \
      exit 1; \
    fi; \
    ./bin/dagforge --help >/dev/null

FROM scratch AS release-bundle
COPY --from=release-verify /opt/dagforge/ /

FROM release-verify AS runtime

EXPOSE 8888

CMD ["/opt/dagforge/bin/dagforge", "serve", "-c", "/opt/dagforge/system_config.toml"]

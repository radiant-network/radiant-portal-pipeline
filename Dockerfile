FROM --platform=$BUILDPLATFORM apache/airflow:2.10.5-python3.12

USER root

# Install dependencies for htslib
RUN apt-get update && apt-get install -y bzip2 git build-essential zlib1g-dev libbz2-dev liblzma-dev libssl-dev libpsl-dev && apt-get clean

ARG LIBCURL_VERSION=8.13.0
RUN curl -L -O https://curl.se/download/curl-${LIBCURL_VERSION}.tar.gz && \
    tar -xzf curl-${LIBCURL_VERSION}.tar.gz && \
    cd curl-${LIBCURL_VERSION} && \
    ./configure --with-ssl --prefix=/usr/local && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd .. && \
    rm -rf curl-${LIBCURL_VERSION} curl-${LIBCURL_VERSION}.tar.gz

# Install htslib with flag enable-s3
RUN curl -L -O https://github.com/samtools/htslib/releases/download/1.21/htslib-1.21.tar.bz2 && \
    tar -xvf htslib-1.21.tar.bz2 && \
    cd htslib-1.21 && \
    ./configure --enable-libcurl && \
    make && \
    make install && \
    cd .. && \
    rm -rf htslib-1.21 htslib-1.21.tar.bz2

USER airflow

RUN mkdir -p /home/airflow/.venv/radiant

RUN python3 -m venv /home/airflow/.venv/radiant

# Force Install Cyvcf2 using installed htslib
RUN CYVCF2_HTSLIB_MODE=EXTERNAL /home/airflow/.venv/radiant/bin/pip install --force --no-binary cyvcf2 cyvcf2

COPY requirements.txt /home/airflow/.venv/radiant/requirements.txt
RUN /home/airflow/.venv/radiant/bin/pip install -r /home/airflow/.venv/radiant/requirements.txt

COPY requirements-airflow.txt /home/airflow/.venv/radiant/requirements-airflow.txt
RUN /home/airflow/.venv/radiant/bin/pip install --no-deps -r /home/airflow/.venv/radiant/requirements-airflow.txt






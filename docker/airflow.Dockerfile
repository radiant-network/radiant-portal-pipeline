FROM --platform=$BUILDPLATFORM apache/airflow:slim-2.10.5-python3.12


USER root

# Install dependencies for htslib
RUN apt-get update && apt-get install -y curl bzip2 build-essential zlib1g-dev libbz2-dev liblzma-dev libcurl4-openssl-dev libssl-dev pkg-config libmysqlclient-dev && apt-get clean

# Install htslib with flag enable-s3
RUN curl -L -O https://github.com/samtools/htslib/releases/download/1.21/htslib-1.21.tar.bz2 && \
    tar -xvf htslib-1.21.tar.bz2 && \
    cd htslib-1.21 && \
    ./configure --enable-libcurl --enable-s3 && \
    make && \
    make install && \
    cd .. && \
    rm -rf htslib-1.21 htslib-1.21.tar.bz2

USER airflow


# Force Install Cyvcf2 using installed htslib
RUN CYVCF2_HTSLIB_MODE=EXTERNAL pip install --force --no-binary cyvcf2 cyvcf2

RUN pip install apache-airflow-providers-postgres apache-airflow-providers-mysql[common.sql] apache-airflow-providers-celery==3.10.0 pyiceberg[s3fs] pyarrow






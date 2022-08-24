FROM ubuntu:20.04

ENV APP_PATH=/app \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends \
        ca-certificates git make build-essential libssl-dev zlib1g-dev \
        libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
        libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN curl https://pyenv.run | bash
ENV PATH="/root/.pyenv/bin:${PATH}"
WORKDIR /app
COPY . .
ARG PYTHON_VERSION=3.10.4
RUN eval "$(pyenv init --path)" \
    && eval "$(pyenv virtualenv-init -)" \
    && pyenv install ${PYTHON_VERSION} \
    && pyenv global ${PYTHON_VERSION} \
    && pip install -e .[test]

RUN echo 'eval "$(pyenv init --path)"' >> /root/.bashrc
RUN echo 'eval "$(pyenv virtualenv-init -)"' >> /root/.bashrc

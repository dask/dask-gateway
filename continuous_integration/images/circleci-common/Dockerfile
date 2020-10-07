FROM python:3.8

RUN apt-get update \
    && apt-get install -y golang-go npm \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

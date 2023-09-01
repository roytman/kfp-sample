# Base image for KFP. It installs oc in the image and additional libraries, required for codeflare sdk
FROM python:3.10-slim-bullseye

RUN pip install --upgrade pip && pip install codeflare-sdk==v0.6.1
# due to https://github.com/ray-project/ray/issues/37019
RUN pip install "pydantic<2" 
RUN pip install "ray[default]"==2.6.3

COPY ./src /pipelines/component/src

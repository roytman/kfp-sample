# Base image for KFP. It installs oc in the image and additional libraries, required for codeflare sdk
FROM python:3.10-slim-bullseye

# install libraries
RUN pip install --upgrade pip && pip install codeflare-sdk==v0.6.1
# due to https://github.com/ray-project/ray/issues/37019
RUN pip install "pydantic<2" 
RUN pip install "ray[default]"==2.6.3
# Adding pyarrow and scipy install, required by dedup
RUN pip install pyarrow scipy==1.9.0
# FIXME: a fix to kfp-tekton mentioned in https://github.com/yaml/pyyaml/issues/601#issuecomment-1660456820
RUN pip install "cython<3.0.0" wheel && pip install pyyaml==5.4.1 --no-build-isolation
# Adding kfp_tekton install, required by sub workflow
RUN pip install kfp-tekton==1.5.4
RUN pip install -i https://test.pypi.org/simple/ manage-kubeflow-pipelines==0.4.0

COPY ./src /pipelines/component/src

# Example of a Ray-based KubeFlow Pipeline Step

## Installation:

### Docker images deployment

In order to run this example you have to push 2 container images:
- The base KFP image with Ray and CodeFlare modules, and with OpenShift CLI client. In order to build and push it run 
```bash
docker build -t <your repository and image name > -f ../shared/BaseKFPDockerfile .
docker push <your repository and image name>
```
e.g.
```bash
docker build -t us.icr.io/cil15-shared-registry/preprocessing-pipelines/kfp-oc:0.0.1 -f ../shared/BaseKFPDockerfile .
docker push us.icr.io/cil15-shared-registry/preprocessing-pipelines/kfp-oc:0.0.1
```
- The Ray custom image with your code, if required, update the `RayDockerfile`
```bash
docker build -t <your repository and image name > -f RayDockerfile .
docker push <your repository and image name>
```
e.g.
```bash
docker build -t us.icr.io/cil15-shared-registry/preprocessing-pipelines/custom-ray:2.5.1-py10 -f RayDockerfile .
docker push us.icr.io/cil15-shared-registry/preprocessing-pipelines/custom-ray:2.5.1-py10
```
### Change OpenShift Project
Go to your Data Science Project
```bash
oc project <your project namespace>
```

### Create Image Pull Secret
If you use a private image registry, create an image pulls secret, named `prod-all-icr-io`
Note: if you want to use another name, you have to update it in the `ray_workflow.yaml` and `ray_template.yaml` files.

### Create a Template Config Map.
This step involves creating a Ray cluster based on the configuration specified in the [ray-template-cm.yaml](../shared/ray-template-cm.yaml) file. If desired, you can make updates to this template. Once the modifications are complete, you will need to create a ConfigMap using the updated template.
```bash
kubectl apply -f ../shared/ray-template-cm.yaml
```

### Install an S3 Access Secret [optional]
This particular example does not currently access S3 storage. However, if it were to access it, you would need to provide the S3 access parameters. To accomplish this, please fill in the required entries in the [cos-access-secret.yaml](../shared/ray-template-cm.yaml) file and proceed with its deployment.
```bash
oc apply -f cos-access-secret.yaml
```

### Upload Pipeline
Compile the pipeline
```bash
python ray_workflow.py
```
And use the ODH UO to upload the new pipeline.
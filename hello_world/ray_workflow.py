import kfp.components as comp
import kfp.dsl as dsl
from kfp_tekton.compiler import TektonCompiler
from kubernetes import client as k8s_client


# execute Ray jobs
def execute_ray_jobs(
    ray_dashboard_uri: str,
    print_tmout: int,  # print timeout
    wait_tmout: int,  # wait tmout for job ready
    wait_retries: int,  # wait retries for job ready
):
    import json
    import sys
    import time

    import ray
    import requests
    from ray.job_submission import JobStatus

    def _get_job_status(ray_dashboard_uri: str, job_id: str) -> str:
        resp = requests.get(f"{ray_dashboard_uri}/api/jobs/{job_id}")
        if resp.status_code == 200:
            rst = json.loads(resp.text)
            return rst["status"]
        else:
            print(f"Getting job execution status failed, code {resp.status_code}")
            return JobStatus.PENDING

    def _get_job_log(ray_dashboard_uri: str, job_id: str) -> str:
        resp = requests.get(f"{ray_dashboard_uri}/api/jobs/{job_id}/logs")
        if resp.status_code == 200:
            rst = json.loads(resp.text)
            return rst["logs"]
        else:
            print(f"Getting job execution log failed, code {resp.status_code}")
            return ""

    def _print_log(log: str, previous_log_len: int) -> None:
        l_to_print = log[previous_log_len:]
        if len(l_to_print) > 0:
            print(log)

    print(f" Ray dashboard is {ray_dashboard_uri}")

    # Submitting job
    job_id = None
    try:
        resp = requests.post(
            f"{ray_dashboard_uri}/api/jobs/",
            json={
                "entrypoint": "python ray-counter.py --kwargs iterations=7",
                "runtime_env": {
                    "pip": ["requests==2.26.0"],
                    "env_vars": {"MY_VARIABLE": "foo", "MY_VARIABLE2": "bar"},
                },
                "metadata": {"job_submission_id": "123"},
            },
        )
        if resp.status_code == 200:
            rst = json.loads(resp.text)
            job_id = rst["job_id"]
            print(f"Submitted job to Ray with the id: {job_id}")
        else:
            print(f"Failed to submitted job to Ray, code : {resp.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Failed to submit job to Ray cluster, error {e}")
        sys.exit(1)

    # Waiting job to start
    try:
        status = JobStatus.PENDING
        tries = 0
        while status != JobStatus.RUNNING:
            tries = tries + 1
            status = _get_job_status(ray_dashboard_uri, job_id)
            if status in {JobStatus.STOPPED, JobStatus.SUCCEEDED, JobStatus.FAILED}:
                break
            if tries >= wait_retries:
                print(f"Failed to get job success status in {wait_retries} tries")
            time.sleep(wait_tmout)
        print(f"Job execution status is {status}")

        # while job is running get job's log
        previous_log_len = 0
        while status == JobStatus.RUNNING:
            log = _get_job_log(ray_dashboard_uri, job_id)
            _print_log(log, previous_log_len)
            previous_log_len = len(log)
            time.sleep(print_tmout)
            # Update status
            status = _get_job_status(ray_dashboard_uri, job_id)
        # print final log and status
        _print_log(_get_job_log(ray_dashboard_uri, job_id), previous_log_len)
        print(f"Job execution status is {status}")
        if status == JobStatus.FAILED:
            sys.exit(1)
    except Exception as e:
        print(f"Failed to get Ray job execution result, error {e}")
        sys.exit(1)


# components
base_kfp_image = "us.icr.io/cil15-shared-registry/preprocessing-pipelines/kfp-oc:0.0.1"
execute_ray_job_op = comp.func_to_container_op(func=execute_ray_jobs, base_image=base_kfp_image)

start_ray_op = comp.load_component_from_file("../shared/rayComponents/startRayComponent.yaml")
shutdown_ray_op = comp.load_component_from_file("../shared/rayComponents/stopRayComponent.yaml")


# Pipeline to invoke execution on remote resource
@dsl.pipeline(
    name="sample-ray-pipeline",
    description="Pipeline to show how to use codeflare sdk to create Ray cluster and run jobs",
)
def sample_ray_pipeline(
    name: str = "sample-kfp-ray",  # name of Ray cluster
    num_workers: int = 2,  # number of workers
    cluster_up_tmout: int = 5,  # minutes to wait for the Ray cluster achived min_worker number
    wait_cluster_ready_tmout: int = 0,  # seconds to wait for Ray cluster to become available
    wait_cluster_nodes_ready_tmout: int = 1,  # seconds to wait for cluster nodes to be ready
    cpus: int = 4,  # cpus per worker
    memory: int = 8,  # memory per worker
    wait_tmout: int = 1,  # wait timeout for job running
    wait_retries: int = 100,  # wait retries for job running
    print_tmout: int = 1,  # print timeout
    image: str = "us.icr.io/cil15-shared-registry/preprocessing-pipelines/custom-ray:2.5.1-py10",
):
    clean_up_task = shutdown_ray_op(name=name)
    clean_up_task.set_image_pull_policy("Always")
    clean_up_task.add_env_variable(
        k8s_client.V1EnvVar(
            name="NAMESPACE",
            value_from=k8s_client.V1EnvVarSource(
                field_ref=k8s_client.V1ObjectFieldSelector(field_path="metadata.namespace")
            ),
        )
    )

    with dsl.ExitHandler(clean_up_task):

        # invoke pipeline
        ray_cluster = start_ray_op(
            name=name,
            num_workers=num_workers,
            cluster_up_tmout=cluster_up_tmout,
            wait_cluster_ready_tmout=wait_cluster_ready_tmout,
            wait_cluster_nodes_ready_tmout=wait_cluster_nodes_ready_tmout,
            cpus=cpus,
            memory=memory,
            image=image,
        )
        # No cashing
        ray_cluster.execution_options.caching_strategy.max_cache_staleness = "P0D"
        # image pull policy
        ray_cluster.set_image_pull_policy("Always")
        # environment variables
        ray_cluster.add_env_variable(
            k8s_client.V1EnvVar(
                name="NAMESPACE",
                value_from=k8s_client.V1EnvVarSource(
                    field_ref=k8s_client.V1ObjectFieldSelector(field_path="metadata.namespace")
                ),
            )
        )
        execute_job = execute_ray_job_op(
            ray_cluster.output,
            wait_tmout=wait_tmout,
            wait_retries=wait_retries,
            print_tmout=print_tmout,
        )
        # No cashing
        execute_job.execution_options.caching_strategy.max_cache_staleness = "P0D"
        # image pull policy
        execute_job.set_image_pull_policy("Always")


if __name__ == "__main__":
    # Compiling the pipeline
    TektonCompiler().compile(sample_ray_pipeline, __file__.replace(".py", ".yaml"))

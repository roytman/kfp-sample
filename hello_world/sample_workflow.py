import kfp.components as comp
import kfp.dsl as dsl
from kfp_tekton.compiler import TektonCompiler
from kubernetes import client as k8s_client

def wait(delay: int):
    import time
    time.sleep(delay)

# execute start Ray cluster
def start_ray_cluster(
    name: str,  # name of Ray cluster
):
    # Import pieces from codeflare-sdk
    import os
    import sys
    from codeflare_sdk.cluster.auth import TokenAuthentication
    from codeflare_sdk.cluster.cluster import Cluster, ClusterConfiguration

    # get current namespace
    ns = os.getenv("NAMESPACE", "default")
    # change the current directory to ensure that we can write
    os.chdir("/tmp")
    print(f"Executing in namespace {ns}, current working directory is {os.getcwd()}")
   
    # Create authentication object for oc user permissions
    secret_path = "/var/run/secrets/kubernetes.io/serviceaccount"
    with open(f"{secret_path}/token", "r") as file:
        token = file.read().rstrip()
    auth = TokenAuthentication(
        token=token, server="https://kubernetes.default:443", skip_tls=False, ca_cert_path=f"{secret_path}/ca.crt"
    )
    try:
        auth.login()
    except Exception as e:
        print(f"Failed to log into openshift cluster, error {e}. Please check token/server values provided")
        sys.exit(1)
    print("successfully logged in")

   
    # Create and configure our cluster object (and appwrapper)
    cluster = Cluster(
        ClusterConfiguration(
            name=name,
            namespace=ns,
        )
    )
    print(f"Configuration for Ray cluster {name} in namespace {ns} is created")

    try:
        import time
        # bring up the cluster
        cluster.up()
        print(f"Creating Ray cluster {name} in namespace {ns}...")

        # and wait for it being up
        # Give it a moment, to avoid racing conditions
        time.sleep(40)
        cluster.wait_ready()
        rc = cluster.details(print_to_console=False)
        print("Ray cluster is ready")
        print(rc)

        # Get cluster connection point for job submission
        ray_dashboard_uri = cluster.cluster_dashboard_uri()
        ray_cluster_uri = cluster.cluster_uri()
        print(f"Ray_cluster is at {ray_cluster_uri}")
        print(f"Ray_cluster dashboard is at {ray_dashboard_uri}")

        # wait to ensure that cluster is really up. Without this we can get ocasional 503 error
        time.sleep(5)

        # make sure all the nodes are up
    except Exception as e:
        print(f"Failed to init Ray cluster, error {e}")
        sys.exit(1)

# Cleans and shutdowns the Ray cluster
def cleanup_ray_cluster(
    name: str,  # name of Ray cluster
):
    import os
    import sys

    from codeflare_sdk.cluster.auth import TokenAuthentication
    from codeflare_sdk.cluster.cluster import Cluster, ClusterConfiguration

    # get current namespace
    ns = os.getenv("NAMESPACE", "default")
    # change the current directory to ensure that we can write
    os.chdir("/tmp")
    print(f"Executing in namespace {ns}, current working directory is {os.getcwd()}")

    # Create authentication object for oc user permissions
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token", "r") as file:
        token = file.read().rstrip()
    auth = TokenAuthentication(token=token, server="https://kubernetes.default:443", skip_tls=True)
    try:
        auth.login()
    except Exception as e:
        print(f"Failed to log into openshift cluster, error {e}. Please check token/server values provided")
        sys.exit(1)
    print("successfully logged in")
    # Create and configure our cluster object (and appwrapper)
    cluster = Cluster(
        ClusterConfiguration(
            name=name,
            namespace=ns,
        )
    )
    print(f"Configuration for Ray cluster {name} in namespace {ns} is created")
    # delete cluster
    print("All done. Cleaning up")
    try:
        cluster.down()
    except Exception as e:
        print(f"Failed to down the Ray cluster, error {e}. Please check that the {name} appwrapper is removed")
        sys.exit(1)

base_kfp_image = "docker.io/roytman/kfp-oc:0.0.1"
# components
start_op = comp.func_to_container_op(func=start_ray_cluster, base_image=base_kfp_image)
wait_op = comp.func_to_container_op(func=wait, base_image=base_kfp_image)
stop_op = comp.func_to_container_op(func=cleanup_ray_cluster, base_image=base_kfp_image)


@dsl.pipeline(
    name="sample-pipeline",
)
def sample_pipeline(
    name: str = "kfp-test",
    delay: int = 10, 
):

    clean_up_task = stop_op(name=name)
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
        start = start_op(name=name)
        # No cashing
        start.execution_options.caching_strategy.max_cache_staleness = "P0D"
        # image pull policy
        start.set_image_pull_policy("Always")
        # environment variables
        start.add_env_variable(
            k8s_client.V1EnvVar(
                name="NAMESPACE",
                value_from=k8s_client.V1EnvVarSource(
                    field_ref=k8s_client.V1ObjectFieldSelector(field_path="metadata.namespace")
                ),
            )
        )

        wait = wait_op(delay=delay).after(start)

if __name__ == "__main__":
    # Compiling the pipeline
    TektonCompiler().compile(sample_pipeline, __file__.replace(".py", ".yaml"))

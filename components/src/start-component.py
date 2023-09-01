# execute start Ray cluster
def start(
    name: str,  # name of Ray cluster
    delay: int,  # delay
) -> str:
    # Import pieces from codeflare-sdk
    import os
    import sys
    import time

    import ray
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

    print(f"{name}: before delay of {delay} sec")
    time.sleep(delay)
    print(f"{name}: after delay of {delay} sec")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start component")
    parser.add_argument("-n", "--name", type=str, default="my-test-cluster")
    parser.add_argument("-d", "--delay", default=5, type=int)

    args = parser.parse_args()

    start(
        name=args.name,
        delay=args.delay,
    )

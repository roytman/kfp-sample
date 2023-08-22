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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stop Ray cluster operation")
    parser.add_argument("-n", "--name", type=str, default="my-test-cluster")
    args = parser.parse_args()

    cleanup_ray_cluster(
        name=args.name,
    )

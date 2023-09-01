# Cleans and shutdowns the Ray cluster
def stop(
    name: str,  # name of Ray cluster
):
    import os
    import sys

    from codeflare_sdk.cluster.cluster import Cluster, ClusterConfiguration
    from codeflare_sdk.cluster.auth import TokenAuthentication

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


    print(name)
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

    parser = argparse.ArgumentParser(description="Stop component")
    parser.add_argument("-n", "--name", type=str)
    args = parser.parse_args()

    stop(
        name=args.name,
    )

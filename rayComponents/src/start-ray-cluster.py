# execute start Ray cluster
def start_ray_cluster(
    name: str,  # name of Ray cluster
    num_workers: int,  # min number of workers
    cluster_up_tmout: int,  # minutes to wait for the Ray cluster achived min_worker number
    wait_cluster_ready_tmout: int,  # seconds to wait for Ray cluster to become available
    wait_cluster_nodes_ready_tmout: int,  # seconds to wait for cluster nodes to be ready
    cpus: int,  # cpus per worker
    memory: int,  # memory per worker
    image: str,  # image for Ray cluster
    output_ray_uri_path: str,
) -> str:
    # Import pieces from codeflare-sdk
    import os
    import sys
    import time
    from pathlib import Path

    import ray
    from codeflare_sdk.cluster.auth import TokenAuthentication
    from codeflare_sdk.cluster.cluster import Cluster, ClusterConfiguration

    @ray.remote
    def get_cluster_nodes() -> int:
        nodes = ray.nodes()
        nnodes = -1
        for n in nodes:
            if n["alive"]:
                nnodes = nnodes + 1
        return nnodes

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
            num_workers=num_workers,
            min_cpus=cpus,
            max_cpus=cpus,
            min_memory=memory,
            max_memory=memory,
            image=image,
            instascale=False,
        )
    )
    print(f"Configuration for Ray cluster {name} in namespace {ns} is created")

    try:
        # bring up the cluster
        cluster.up()
        print(f"Creating Ray cluster {name} in namespace {ns}...")

        # and wait for it being up
        if wait_cluster_ready_tmout > 0:
            cluster.wait_ready(wait_cluster_ready_tmout)
        else:
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
        ray.init(address=f"{ray_cluster_uri}")
        running = 0
        timeout = time.time() + cluster_up_tmout * 60
        while time.time() < timeout:
            if running < min_worker:
                running = ray.get(get_cluster_nodes.remote())
                print(f"{running} nodes are currently available")
                time.sleep(wait_cluster_nodes_ready_tmout)
            else:
                break
        if running < min_worker:
            print(
                f"Timeout: cannot achive {min_worker} running workers, during {cluster_up_tmout} minutes, currently running {running} workers"
            )
            sys.exit(1)
        ray.shutdown()
        Path(args.output_ray_uri_path).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_ray_uri_path).write_text(ray_dashboard_uri)
        return ray_dashboard_uri
    except Exception as e:
        print(f"Failed to init Ray cluster, error {e}")
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start Ray cluster operation")
    parser.add_argument("-n", "--name", type=str, default="my-test-cluster")
    parser.add_argument("-nw", "--num_workers", default=1, type=int)
    parser.add_argument("-cut", "--cluster_up_tmout", default=5, type=int)
    parser.add_argument("-wct", "--wait_cluster_ready_tmout", default=0, type=int)
    parser.add_argument("-wnt", "--wait_cluster_nodes_ready_tmout", default=1, type=int)
    parser.add_argument("--cpus", default=2, type=int)
    parser.add_argument("-m", "--memory", default=16, type=int)
    parser.add_argument("-img", "--image", default="", type=str)
    parser.add_argument(
        "--output_ray_uri_path", type=str, help="Output path for Ray dashboard URL", default="/tmp/ray-dashboard.txt"
    )

    args = parser.parse_args()

    start_ray_cluster(
        name=args.name,
        num_workers=args.num_workers,
        cluster_up_tmout=args.cluster_up_tmout,
        wait_cluster_ready_tmout=args.wait_cluster_ready_tmout,
        wait_cluster_nodes_ready_tmout=args.wait_cluster_nodes_ready_tmout,
        cpus=args.cpus,
        memory=args.memory,
        image=args.image,
        output_ray_uri_path=args.output_ray_uri_path,
    )

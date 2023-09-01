import kfp.components as comp
import kfp.dsl as dsl
from kfp_tekton.compiler import TektonCompiler
from kubernetes import client as k8s_client

# components
start_op = comp.load_component_from_file("../components/startComponent.yaml")
wait_op = comp.load_component_from_file("../components/waitComponent.yaml")
stop_op = comp.load_component_from_file("../components/stopComponent.yaml")


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

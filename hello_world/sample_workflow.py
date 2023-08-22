import kfp.components as comp
import kfp.dsl as dsl
from kfp_tekton.compiler import TektonCompiler

# components
start_op = comp.load_component_from_file("../components/startComponent.yaml")
stop_op = comp.load_component_from_file("../components/stopComponent.yaml")


@dsl.pipeline(
    name="sample-pipeline",
)
def sample_pipeline(
    name: str = "ktp-test",
    delay: int = 6, 
):

    # invoke pipeline
    name_from_start = start_op(
            name=name,
            delay=delay,
        )
    # No cashing
    name_from_start.execution_options.caching_strategy.max_cache_staleness = "P0D"
    # image pull policy
    name_from_start.set_image_pull_policy("Always")
    
    stop = stop_op(name_from_start.output)
    # No cashing
    stop.execution_options.caching_strategy.max_cache_staleness = "P0D"
    # image pull policy
    stop.set_image_pull_policy("Always")


if __name__ == "__main__":
    # Compiling the pipeline
    TektonCompiler().compile(sample_pipeline, __file__.replace(".py", ".yaml"))

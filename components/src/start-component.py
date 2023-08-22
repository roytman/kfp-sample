# execute start Ray cluster
def start(
    name: str,  # name of Ray cluster
    delay: int,  # delay
    output_path: str,
) -> str:
    import time
    from pathlib import Path

    print(f"{name}: before delay of {delay} sec")
    time.sleep(delay)
    print(f"{name}: after delay of {delay} sec")

    name = name + " awesome"
    Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_path).write_text(name)
    return name


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start component")
    parser.add_argument("-n", "--name", type=str, default="my-test-cluster")
    parser.add_argument("-d", "--delay", default=5, type=int)
    parser.add_argument(
        "--output_path", type=str, help="Output path", default="/tmp/output.txt"
    )

    args = parser.parse_args()

    start(
        name=args.name,
        delay=args.delay,
        output_path=args.output_path,
    )

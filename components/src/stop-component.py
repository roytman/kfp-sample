# Cleans and shutdowns the Ray cluster
def stop(
    name: str,  # name of Ray cluster
):
    print(name)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stop component")
    parser.add_argument("-n", "--name", type=str)
    args = parser.parse_args()

    stop(
        name=args.name,
    )

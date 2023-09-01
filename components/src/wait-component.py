import time

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start component")
    parser.add_argument("-d", "--delay", default=5, type=int)

    args = parser.parse_args()

    time.sleep(args.delay)
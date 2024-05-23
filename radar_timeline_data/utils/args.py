import argparse


def get_args():
    parser = argparse.ArgumentParser(description="TimeLineData importer script")
    # Add the arguments
    parser.add_argument("-a", "--audit", type=str, help="Audit a directory")
    parser.add_argument(
        "-c", "--commit", help="Commit to server", action="store_true", default=False
    )
    parser.add_argument(
        "-tr",
        "--test_run",
        help="run on staging servers",
        action="store_true",
        default=False,
    )
    # Parse the arguments
    return parser.parse_args()

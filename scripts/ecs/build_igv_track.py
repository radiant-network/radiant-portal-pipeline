import argparse
import json
import logging
import sys

from radiant.tasks.open_data.igv_tracks import build_igv_track

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stderr)])
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Build one public IGV track and write it with its index to S3")
    parser.add_argument("--track_id", required=True, help="Track id, a key of IGV_TRACK_SOURCES (e.g. clinvar)")
    parser.add_argument("--prefix", required=True, help="S3 prefix to write the track and its .tbi under")
    args = parser.parse_args()
    logger.info(f"Received arguments --track_id={args.track_id} --prefix={args.prefix}")

    try:
        summary = build_igv_track(args.track_id, args.prefix)
    except Exception as e:
        logger.exception(f"Error while building the IGV track {args.track_id}: {e}")
        sys.exit(1)
    # stdout is the task's XCom on ECS: keep it to the JSON summary (logs go to stderr above).
    print(json.dumps(summary))


if __name__ == "__main__":
    main()

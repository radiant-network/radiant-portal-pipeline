import argparse
import json
import logging
import sys

from radiant.tasks.open_data.cosmic_mutation_set import normalize_cosmic_mutation_set

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stderr)])
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Normalize the COSMIC Mutation Census export for the StarRocks load")
    parser.add_argument("--input", required=True, help="S3 URI of cmc_export.tsv.gz")
    parser.add_argument(
        "--fasta", required=True, help="S3 URI of the GRCh38 reference FASTA (its .fai must sit next to it)"
    )
    parser.add_argument("--output", required=True, help="S3 URI to write the normalized TSV (gzip) to")
    args = parser.parse_args()
    logger.info(f"Received arguments --input={args.input} --fasta={args.fasta} --output={args.output}")

    try:
        summary = normalize_cosmic_mutation_set(args.input, args.fasta, args.output)
    except Exception as e:
        logger.exception(f"Error while normalizing the COSMIC Mutation Census: {e}")
        sys.exit(1)
    # stdout is the task's XCom on ECS: keep it to the JSON summary (logs go to stderr above).
    print(json.dumps(summary))


if __name__ == "__main__":
    main()

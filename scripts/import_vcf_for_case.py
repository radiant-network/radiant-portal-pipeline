import json
import logging
import os
import sys
from urllib import parse

import boto3

from radiant.tasks.vcf.experiment import Case
from radiant.tasks.vcf.process import process_case

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

def generate_presigned_url(s3_path, expiration=3600):
    """
    Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_key: string
    :param expiration: Time in seconds for the presigned URL to remain valid (default: 1 hour)
    :return: Presigned URL as string. If error, returns None.
    """
    s3_client = boto3.client("s3")

    def extract_bucket_key(s3_path):
        parsed = parse.urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    bucket_name, object_key = extract_bucket_key(s3_path)
    try:
        response = s3_client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket_name, "Key": object_key}, ExpiresIn=expiration
        )
    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        return None

    return response

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Import VCF for case")
    parser.add_argument("--case", required=True, help="Case JSON string")
    args = parser.parse_args()
    case = json.loads(args.case)

    logger.info("Generating presigned URLs for VCF and index files")
    url = generate_presigned_url(case["vcf_filepath"], expiration=7200)
    index_url = generate_presigned_url(case["vcf_filepath"] + ".tbi", expiration=7200)
    case["vcf_filepath"] = url
    case["index_vcf_filepath"] = index_url

    print(f"Presigned VCF URL: {case['vcf_filepath']}")
    case = Case.model_validate(case)
    logger.info(f"🔁 STARTING IMPORT for Case: {case.case_id}")
    logger.info("=" * 80)

    namespace = os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant")
    res = process_case(case, namespace=namespace, vcf_threads=4)
    logger.info(f"✅ Parquet files created: {case.case_id}, file {case.vcf_filepath}")
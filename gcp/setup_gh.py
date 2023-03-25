"""Copy deploy keys + config from GCS for pushing output to GH"""

import os
import logging
from pathlib import Path

from google.cloud import storage


SSH_CRED_URI = os.getenv("SSH_CRED_URI")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(filename)16s:%(lineno)4d:%(levelname)8s] - %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    ssh_config_dir = os.path.expanduser("~/.ssh")
    os.makedirs(ssh_config_dir, exist_ok=True)

    logger.info(f"Attempting to save contents of {SSH_CRED_URI} to {ssh_config_dir}")
    _, bucket, *blob = Path(SSH_CRED_URI).parts
    blob = "/".join(blob)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    for f in bucket.list_blobs(prefix=blob):
        filename = Path(f.name).name
        out_path = Path(ssh_config_dir).joinpath(filename)
        f.download_to_filename(out_path)
        logger.info(f"Saved {filename} to {str(out_path)}")


if __name__ == "__main__":
    main()

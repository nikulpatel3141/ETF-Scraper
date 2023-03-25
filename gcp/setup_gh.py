"""Copy deploy keys + config from GCS for pushing output to GH"""

import os
from pathlib import Path

from google.cloud import storage


SSH_CRED_URI = os.getenv("SSH_CRED_URI")


def main():
    ssh_config_dir = os.path.expanduser("~/.sssh")
    os.makedirs(ssh_config_dir, exist_ok=True)

    _, bucket, *blob = Path(SSH_CRED_URI).parts
    blob = "/".join(blob)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    for f in bucket.list_blobs(prefix=blob):
        filename = Path(f.name).name
        f.download_to_filename(Path(ssh_config_dir).joinpath(filename))


if __name__ == "__main__":
    main()

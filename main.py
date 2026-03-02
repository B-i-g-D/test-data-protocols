import sys

from kaggle_s3_lake.cli import main

if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.append("ingest")
    main()

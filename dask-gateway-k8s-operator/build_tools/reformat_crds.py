"""Reformat the CRD yaml files to make them not unreasonably long"""
import argparse
import glob
import json
import os

import yaml


def reformat_crds():
    placeholder = "PLACEHOLDER_STRING_123_456"
    this_dir = os.path.dirname(__file__)
    crds = os.path.abspath(os.path.join(this_dir, "..", "deploy", "crds", "*_crd.yaml"))

    for path in glob.glob(crds):
        print("Reformatting %s" % os.path.relpath(path))

        with open(path) as f:
            data = yaml.safe_load(f)

        # Compress the schema into one line
        schema = data["spec"]["validation"].pop("openAPIV3Schema")
        data["spec"]["validation"]["openAPIV3Schema"] = placeholder
        out = yaml.dump(data)
        out = out.replace(placeholder, json.dumps(schema, separators=(",", ":")), 1)

        with open(path, "w") as f:
            f.write(out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reformat the CRD yaml files")
    args = parser.parse_args()
    reformat_crds()

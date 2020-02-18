"""Reformat the CRD yaml files to make them not unreasonably long"""
import argparse
import glob
import json
import os

import yaml


def strip_descriptions(obj):
    if isinstance(obj, dict):
        obj.pop("description", None)
        for v in obj.values():
            strip_descriptions(v)
    elif isinstance(obj, list):
        for v in obj:
            strip_descriptions(v)


def reformat_crds():
    placeholder = "PLACEHOLDER_STRING_123_456"
    this_dir = os.path.dirname(__file__)
    crds = os.path.abspath(os.path.join(this_dir, "..", "deploy", "crds", "*_crd.yaml"))

    for path in glob.glob(crds):
        print("Reformatting %s" % os.path.relpath(path))

        with open(path) as f:
            data = yaml.safe_load(f)

        # Compress the schema into one line, and strip the descriptions field
        # This makes the CRD definition smaller, which lets `kubectl apply`
        # still work (kubectl apply passes the value as an annotation, which
        # has a size limit)
        schema = data["spec"]["validation"].get("openAPIV3Schema")
        strip_descriptions(schema)
        data["spec"]["validation"]["openAPIV3Schema"] = placeholder
        out = yaml.dump(data)
        out = out.replace(placeholder, json.dumps(schema), 1)

        with open(path, "w") as f:
            f.write(out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reformat the CRD yaml files")
    args = parser.parse_args()
    reformat_crds()

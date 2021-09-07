#!/usr/bin/env python3
"""
This scripts validates the charts default values against the values.schema.yaml
file, and optionally also another file against the values.schema.yaml.

This script originated from the jupyterhub/zero-to-jupyterhub-k8s project. It is
not yet extracted to be a standalone package, but may be in the future.
"""

import jsonschema
import os
import yaml

here_dir = os.path.abspath(os.path.dirname(__file__))
schema_yaml = os.path.join(here_dir, os.pardir, "dask-gateway", "values.schema.yaml")
values_yaml = os.path.join(here_dir, os.pardir, "dask-gateway", "values.yaml")
lint_and_validate_values_yaml = os.path.join(
    here_dir, os.pardir, "testing", "chart-install-values.yaml"
)

with open(schema_yaml) as f:
    schema = yaml.safe_load(f)
with open(values_yaml) as f:
    values = yaml.safe_load(f)
with open(lint_and_validate_values_yaml) as f:
    lint_and_validate_values = yaml.safe_load(f)

# Validate values.yaml against schema
print("Validating values.yaml against values.schema.yaml...")
jsonschema.validate(values, schema)
print("OK!")
print()

# FIXME: Create a lint-and-validate-values.yaml file that covers all kinds of
#        configuration properly and let it be tested to function with the schema
#        and successfully render valid k8s templates.
#
# # Validate chart-install-values.yaml against schema
# print("Validating chart-install-values.yaml against values.schema.yaml...")
# jsonschema.validate(lint_and_validate_values, schema)
# print("OK!")

# Operator Development 

Helpful alias for running Operator-SDK on your local machine:
```bash
alias osdk=operator-sdk
alias osdkrun='operator-sdk run --local --operator-flags="--zap-encoder=console"'
```

After updating `pkgs/apis/gateway/v1alpha1/daskcluster_types.go`, run:
```bash
operator-sdk generate k8s \
&& operator-sdk generate crds \
&& python build_tools/reformat_crds.py \
&& kubectl apply -f deploy/crds/gateway.dask.org_daskclusters_crd.yaml
```

A sample custom resource for testing purposes is available in [deploy/crds/cr.yaml](deploy/crds/cr.yaml).
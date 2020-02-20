package controller

import (
	"github.com/dask/dask-gateway/dask-gateway-k8s-operator/pkg/controller/daskcluster"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, daskcluster.Add)
}

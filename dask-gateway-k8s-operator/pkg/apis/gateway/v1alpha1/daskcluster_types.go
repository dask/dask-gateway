package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// Scheduler scheduler
type Scheduler struct {
	Template corev1.PodTemplateSpec `json:"template"`
}

// Worker worker
type Worker struct {
	Template corev1.PodTemplateSpec `json:"template"`
	Replicas int32                  `json:"replicas,omitempty"`
}

// DaskClusterSpec defines the desired state of DaskCluster
type DaskClusterSpec struct {
	Scheduler Scheduler         `json:"scheduler"`
	Worker    Worker            `json:"worker"`
	Info      map[string]string `json:"info,omitempty"`
	Active    bool              `json:"active"`
}

// DaskClusterStatus defines the observed state of DaskCluster
type DaskClusterStatus struct {
	Replicas int32    `json:"replicas"`
	Pods     []string `json:"pods"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DaskCluster is the Schema for the daskclusters API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=daskclusters,scope=Namespaced
type DaskCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DaskClusterSpec   `json:"spec,omitempty"`
	Status DaskClusterStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DaskClusterList contains a list of DaskCluster
type DaskClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DaskCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DaskCluster{}, &DaskClusterList{})
}

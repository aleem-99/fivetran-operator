package kubeutils

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Annotation utility functions using metav1.Object interface methods

// GetAnnotation safely gets an annotation value
func GetAnnotation(obj metav1.Object, key string) string {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return ""
	}
	return annotations[key]
}

// SetAnnotation safely sets an annotation value
func SetAnnotation(obj metav1.Object, key, value string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[key] = value
	obj.SetAnnotations(annotations)
}

// HasAnnotation checks if an annotation exists
func HasAnnotation(obj metav1.Object, key string) bool {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return false
	}
	_, exists := annotations[key]
	return exists
}

// RemoveAnnotation removes an annotation
func RemoveAnnotation(obj metav1.Object, key string) {
	annotations := obj.GetAnnotations()
	if annotations != nil {
		delete(annotations, key)
		obj.SetAnnotations(annotations)
	}
}

// Label utility functions using metav1.Object interface methods

// GetLabel safely gets a label value
func GetLabel(obj metav1.Object, key string) string {
	labels := obj.GetLabels()
	if labels == nil {
		return ""
	}
	return labels[key]
}

// SetLabel safely sets a label value
func SetLabel(obj metav1.Object, key, value string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[key] = value
	obj.SetLabels(labels)
}

// HasLabel checks if a label exists
func HasLabel(obj metav1.Object, key string) bool {
	labels := obj.GetLabels()
	if labels == nil {
		return false
	}
	_, exists := labels[key]
	return exists
}

// RemoveLabel removes a label
func RemoveLabel(obj metav1.Object, key string) {
	labels := obj.GetLabels()
	if labels != nil {
		delete(labels, key)
		obj.SetLabels(labels)
	}
}

package kubeutils

import (
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Custom Predicate to filter by a specific label key
type CustomLabelKeyChangedPredicate struct {
	LabelKey string
	predicate.Funcs
}

// Custom Predicate label to force reconciliation on label addition
func (p CustomLabelKeyChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return false
	}

	oldLabels := e.ObjectOld.GetLabels()
	newLabels := e.ObjectNew.GetLabels()

	_, oldExists := oldLabels[p.LabelKey]
	_, newExists := newLabels[p.LabelKey]

	// Trigger reconciliation only if the label is added
	if !oldExists && newExists {
		return true
	}

	return false
}

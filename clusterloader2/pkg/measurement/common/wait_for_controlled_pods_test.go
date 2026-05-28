/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/perf-tests/clusterloader2/pkg/config"
	"k8s.io/perf-tests/clusterloader2/pkg/framework"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
)

func TestWaitForControlledPodsRunning_NonManagedNamespaceRegression(t *testing.T) {
	var int1 int32 = 1
	var trueVal = true

	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dep",
			Namespace: "kube-system",
			Labels:    map[string]string{"app": "test"},
			UID:       "dep-uid",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &int1,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "nginx"}},
				},
			},
		},
	}

	rs := &appsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dep-rs",
			Namespace: "kube-system",
			Labels:    map[string]string{"app": "test", "pod-template-hash": "hash123"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-dep",
				UID:        "dep-uid",
				Controller: &trueVal,
			}},
			UID: "rs-uid",
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &int1,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test", "pod-template-hash": "hash123"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "nginx"}},
				},
			},
		},
	}

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dep-rs-pod1",
			Namespace: "kube-system",
			Labels:    map[string]string{"app": "test", "pod-template-hash": "hash123"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "test-dep-rs",
				UID:        "rs-uid",
				Controller: &trueVal,
			}},
			UID: "pod-uid",
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "c", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}

	unstructuredDepContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(deployment)
	if err != nil {
		t.Fatalf("failed to convert deployment to unstructured: %v", err)
	}
	unstructuredDep := &unstructured.Unstructured{Object: unstructuredDepContent}

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, &appsv1.Deployment{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "DeploymentList"}, &appsv1.DeploymentList{})

	client := fakeclientset.NewSimpleClientset(pod, rs)
	dynamicClient := fakedynamic.NewSimpleDynamicClient(scheme, unstructuredDep)

	clusterConfig := &config.ClusterConfig{}
	clusterFramework := framework.NewFakeFramework(client, dynamicClient, clusterConfig)

	m := createWaitForControlledPodsRunningMeasurement()

	startConfig := &measurement.Config{
		ClusterFramework: clusterFramework,
		Params: map[string]interface{}{
			"action":                "start",
			"apiVersion":            "apps/v1",
			"kind":                  "Deployment",
			"labelSelector":         "app=test",
			"checkIfPodsAreUpdated": true,
			"operationTimeout":      "5s",
		},
	}

	if _, err := m.Execute(startConfig); err != nil {
		t.Fatalf("start action failed: %v", err)
	}

	time.Sleep(1 * time.Second)

	gatherConfig := &measurement.Config{
		ClusterFramework: clusterFramework,
		Params: map[string]interface{}{
			"action":      "gather",
			"syncTimeout": "2s",
		},
	}

	if _, err := m.Execute(gatherConfig); err != nil {
		t.Fatalf("gather action failed: %v", err)
	}
}

/*
Copyright 2018 The Kubernetes Authors.

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

package measurement

import (
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"k8s.io/perf-tests/clusterloader2/api"
)

// Factory is a default global factory instance.
var factory = newMeasurementFactory()

func newMeasurementFactory() *measurementFactory {
	return &measurementFactory{
		createFuncs:        make(map[string]createMeasurementFunc),
		registeredIndexers: make(map[schema.GroupVersionResource]cache.Indexers),
	}
}

// measurementFactory is a factory that creates measurement instances.
type measurementFactory struct {
	lock               sync.RWMutex
	createFuncs        map[string]createMeasurementFunc
	registeredIndexers map[schema.GroupVersionResource]cache.Indexers
}

func (mc *measurementFactory) register(methodName string, createFunc createMeasurementFunc) error {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	_, exists := mc.createFuncs[methodName]
	if exists {
		return fmt.Errorf("measurement with method %v already exists", methodName)
	}
	mc.createFuncs[methodName] = createFunc
	api.RegisteredMeasurements[methodName] = true
	return nil
}

func (mc *measurementFactory) createMeasurement(methodName string) (Measurement, error) {
	mc.lock.RLock()
	defer mc.lock.RUnlock()
	createFunc, exists := mc.createFuncs[methodName]
	if !exists {
		return nil, fmt.Errorf("unknown measurement method %s", methodName)
	}
	return createFunc(), nil
}

// Register registers create measurement function in measurement factory.
func Register(methodName string, createFunc createMeasurementFunc) error {
	return factory.register(methodName, createFunc)
}

// CreateMeasurement creates measurement instance.
func CreateMeasurement(methodName string) (Measurement, error) {
	return factory.createMeasurement(methodName)
}


// RegisterIndexer registers indexers for the given resource globally.
func RegisterIndexer(gvr schema.GroupVersionResource, indexers cache.Indexers) {
	factory.lock.Lock()
	defer factory.lock.Unlock()

	if factory.registeredIndexers[gvr] == nil {
		factory.registeredIndexers[gvr] = make(cache.Indexers)
	}

	for key, idxFunc := range indexers {
		factory.registeredIndexers[gvr][key] = idxFunc
	}
}

// GetRegisteredIndexers returns all registered indexers.
func GetRegisteredIndexers() map[schema.GroupVersionResource]cache.Indexers {
	factory.lock.RLock()
	defer factory.lock.RUnlock()

	result := make(map[schema.GroupVersionResource]cache.Indexers)
	for gvr, indexers := range factory.registeredIndexers {
		result[gvr] = make(cache.Indexers)
		for k, v := range indexers {
			result[gvr][k] = v
		}
	}
	return result
}

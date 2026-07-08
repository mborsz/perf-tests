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

package framework

import (
	"fmt"
	"io/fs"
	"regexp"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/config"
	"k8s.io/perf-tests/clusterloader2/pkg/errors"
	"k8s.io/perf-tests/clusterloader2/pkg/framework/client"
	frconfig "k8s.io/perf-tests/clusterloader2/pkg/framework/config"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement/util/informer"

	// ensure auth plugins are loaded
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

var namespaceID = regexp.MustCompile(`^test-[a-z0-9]+-[0-9]+$`)

// Framework allows for interacting with Kubernetes cluster via
// official Kubernetes client.
type Framework struct {
	automanagedNamespacePrefix string
	mux                        sync.Mutex
	// automanagedNamespaces stores values as information if should the namespace be deleted
	automanagedNamespaces map[string]bool
	clientSets            *MultiClientSet
	dynamicClients        *MultiDynamicClient
	clusterConfig         *config.ClusterConfig
	restClientConfig      *restclient.Config
	discoveryClient       *discovery.DiscoveryClient

	sharedInformerFactory        informers.SharedInformerFactory
	dynamicSharedInformerFactory dynamicinformer.DynamicSharedInformerFactory
}

// NewFramework creates new framework based on given clusterConfig.
func NewFramework(clusterConfig *config.ClusterConfig, clientsNumber int) (*Framework, error) {
	return newFramework(clusterConfig, clientsNumber, clusterConfig.KubeConfigPath)
}

// NewRootFramework creates framework for the root cluster.
// For clusters other than kubemark there is no difference between NewRootFramework and NewFramework.
func NewRootFramework(clusterConfig *config.ClusterConfig, clientsNumber int) (*Framework, error) {
	kubeConfigPath := clusterConfig.KubeConfigPath
	if override := clusterConfig.Provider.GetConfig().RootFrameworkKubeConfigOverride(); override != "" {
		kubeConfigPath = override
	}
	return newFramework(clusterConfig, clientsNumber, kubeConfigPath)
}

func newFramework(clusterConfig *config.ClusterConfig, clientsNumber int, kubeConfigPath string) (*Framework, error) {
	klog.Infof("Creating framework with %d clients and %q kubeconfig.", clientsNumber, kubeConfigPath)
	var err error
	f := Framework{
		automanagedNamespaces: map[string]bool{},
		clusterConfig:         clusterConfig,
	}
	if f.clientSets, err = NewMultiClientSet(kubeConfigPath, clientsNumber); err != nil {
		return nil, fmt.Errorf("multi client set creation error: %v", err)
	}
	if f.dynamicClients, err = NewMultiDynamicClient(kubeConfigPath, clientsNumber); err != nil {
		return nil, fmt.Errorf("multi dynamic client creation error: %v", err)
	}

	if f.restClientConfig, err = frconfig.GetConfig(kubeConfigPath); err != nil {
		return nil, fmt.Errorf("rest client creation error: %v", err)
	}

	if f.discoveryClient, err = discovery.NewDiscoveryClientForConfig(f.restClientConfig); err != nil {
		return nil, fmt.Errorf("discovery client creation error: %v", err)
	}

	f.sharedInformerFactory = informers.NewSharedInformerFactoryWithOptions(f.clientSets.GetClient(), 0, informers.WithTransform(informer.TrimManagedFields))
	f.dynamicSharedInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(f.dynamicClients.GetClient(), 0, metav1.NamespaceAll, nil)

	// Register central indexers
	if err := f.sharedInformerFactory.Core().V1().Pods().Informer().AddIndexers(cache.Indexers{measurementutil.ControllerUIDIndex: measurementutil.ControllerUIDIndexFunc}); err != nil {
		return nil, fmt.Errorf("failed to register controllerUID indexer: %w", err)
	}

	return &f, nil
}

// NewFrameworkFromClients creates new Framework with given clientSets and dynamicClients for testing.
func NewFrameworkFromClients(clientSets *MultiClientSet, dynamicClients *MultiDynamicClient) *Framework {
	f := &Framework{
		automanagedNamespaces: map[string]bool{},
		clientSets:            clientSets,
		dynamicClients:        dynamicClients,
	}
	f.sharedInformerFactory = informers.NewSharedInformerFactoryWithOptions(clientSets.GetClient(), 0, informers.WithTransform(informer.TrimManagedFields))
	f.dynamicSharedInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynamicClients.GetClient(), 0, metav1.NamespaceAll, nil)

	if err := f.sharedInformerFactory.Core().V1().Pods().Informer().AddIndexers(cache.Indexers{measurementutil.ControllerUIDIndex: measurementutil.ControllerUIDIndexFunc}); err != nil {
		// Using panic here as it's a test helper and failure to register indexer is fatal for tests using it.
		panic(fmt.Sprintf("failed to register controllerUID indexer: %v", err))
	}
	return f
}

// GetAutomanagedNamespacePrefix returns automanaged namespace prefix.
func (f *Framework) GetAutomanagedNamespacePrefix() string {
	return f.automanagedNamespacePrefix
}

// SetAutomanagedNamespacePrefix sets automanaged namespace prefix.
func (f *Framework) SetAutomanagedNamespacePrefix(nsName string) {
	f.automanagedNamespacePrefix = nsName
}

// GetClientSets returns clientSet clients.
func (f *Framework) GetClientSets() *MultiClientSet {
	return f.clientSets
}

// GetDynamicClients returns dynamic clients.
func (f *Framework) GetDynamicClients() *MultiDynamicClient {
	return f.dynamicClients
}

func (f *Framework) GetRestClient() *restclient.Config {
	return f.restClientConfig
}

// GetClusterConfig returns cluster config.
func (f *Framework) GetClusterConfig() *config.ClusterConfig {
	return f.clusterConfig
}

func (f *Framework) GetDiscoveryClient() *discovery.DiscoveryClient {
	return f.discoveryClient
}

func (f *Framework) GetSharedInformerFactory() informers.SharedInformerFactory {
	return f.sharedInformerFactory
}

func (f *Framework) GetDynamicSharedInformerFactory() dynamicinformer.DynamicSharedInformerFactory {
	return f.dynamicSharedInformerFactory
}

// CreateAutomanagedNamespaces creates automanged namespaces.
func (f *Framework) CreateAutomanagedNamespaces(namespaceCount int, allowExistingNamespaces bool, deleteAutomanagedNamespaces bool) error {
	f.mux.Lock()
	defer f.mux.Unlock()
	// get all pre-created namespaces and store in a hash set
	namespacesList, err := client.ListNamespaces(f.clientSets.GetClient())
	if err != nil {
		return err
	}
	existingNamespaceSet := make(map[string]bool)
	for _, ns := range namespacesList {
		existingNamespaceSet[ns.Name] = true
	}

	for i := 1; i <= namespaceCount; i++ {
		name := fmt.Sprintf("%v-%d", f.automanagedNamespacePrefix, i)
		if _, created := existingNamespaceSet[name]; !created {
			if err := client.CreateNamespace(f.clientSets.GetClient(), name); err != nil {
				return err
			}
		} else {
			if !allowExistingNamespaces {
				return fmt.Errorf("automanaged namespace %s already created", name)
			}
		}
		f.automanagedNamespaces[name] = deleteAutomanagedNamespaces
	}
	return nil
}

// ListAutomanagedNamespaces returns all existing automanged namespace names.
func (f *Framework) ListAutomanagedNamespaces() ([]string, []string, error) {
	var automanagedNamespacesCurrentPrefixList, staleNamespaces []string
	namespacesList, err := client.ListNamespaces(f.clientSets.GetClient())
	if err != nil {
		return automanagedNamespacesCurrentPrefixList, staleNamespaces, err
	}
	for _, namespace := range namespacesList {
		matched, err := f.isAutomanagedNamespaceCurrentPrefix(namespace.Name)
		if err != nil {
			return automanagedNamespacesCurrentPrefixList, staleNamespaces, err
		}
		if matched {
			automanagedNamespacesCurrentPrefixList = append(automanagedNamespacesCurrentPrefixList, namespace.Name)
		} else {
			// check further whether the namespace is a automanaged namespace created in previous test execution.
			// this could happen when the execution is aborted abornamlly, and the resource is not able to be
			// clean up.
			matched := f.isStaleAutomanagedNamespace(namespace.Name)
			if matched {
				staleNamespaces = append(staleNamespaces, namespace.Name)
			}
		}
	}
	return automanagedNamespacesCurrentPrefixList, staleNamespaces, nil
}

func (f *Framework) deleteNamespace(namespace string, timeout time.Duration) error {
	clientSet := f.clientSets.GetClient()
	if err := client.DeleteNamespace(clientSet, namespace); err != nil {
		return err
	}
	if err := client.WaitForDeleteNamespace(clientSet, namespace, timeout); err != nil {
		return err
	}
	f.removeAutomanagedNamespace(namespace)
	return nil
}

// DeleteAutomanagedNamespaces deletes all automanged namespaces.
func (f *Framework) DeleteAutomanagedNamespaces(timeout time.Duration) *errors.ErrorList {
	var wg wait.Group
	errList := errors.NewErrorList()
	for namespace, shouldBeDeleted := range f.getAutomanagedNamespaces() {
		namespace := namespace
		if shouldBeDeleted {
			wg.Start(func() {
				if err := f.deleteNamespace(namespace, timeout); err != nil {
					errList.Append(err)
					return
				}
			})
		}
	}
	wg.Wait()
	return errList
}

func (f *Framework) removeAutomanagedNamespace(namespace string) {
	f.mux.Lock()
	defer f.mux.Unlock()
	delete(f.automanagedNamespaces, namespace)
}

func (f *Framework) getAutomanagedNamespaces() map[string]bool {
	f.mux.Lock()
	defer f.mux.Unlock()
	m := map[string]bool{}
	for k, v := range f.automanagedNamespaces {
		m[k] = v
	}
	return m
}

// DeleteNamespaces deletes the list of namespaces.
func (f *Framework) DeleteNamespaces(namespaces []string, timeout time.Duration) *errors.ErrorList {
	var wg wait.Group
	errList := errors.NewErrorList()
	for _, namespace := range namespaces {
		namespace := namespace
		wg.Start(func() {
			if err := f.deleteNamespace(namespace, timeout); err != nil {
				errList.Append(err)
				return
			}
		})
	}
	wg.Wait()
	return errList
}

// CreateObject creates object base on given object description.
func (f *Framework) CreateObject(namespace string, name string, obj *unstructured.Unstructured, options ...*client.APICallOptions) error {
	return client.CreateObject(f.dynamicClients.GetClient(), namespace, name, obj, options...)
}

// PatchObject updates object (using patch) with given name using given object description.
func (f *Framework) PatchObject(namespace string, name string, obj *unstructured.Unstructured, _ ...*client.APICallOptions) error {
	return client.PatchObject(f.dynamicClients.GetClient(), namespace, name, obj)
}

// DeleteObject deletes object with given name and group-version-kind.
func (f *Framework) DeleteObject(gvk schema.GroupVersionKind, namespace string, name string, _ ...*client.APICallOptions) error {
	return client.DeleteObject(f.dynamicClients.GetClient(), gvk, namespace, name)
}

// GetObject retrieves object with given name and group-version-kind.
func (f *Framework) GetObject(gvk schema.GroupVersionKind, namespace string, name string, _ ...*client.APICallOptions) (*unstructured.Unstructured, error) {
	return client.GetObject(f.dynamicClients.GetClient(), gvk, namespace, name)
}

// ApplyTemplatedManifests finds and applies all manifest template files matching the provided
// manifestGlob pattern. It substitutes the template placeholders using the templateMapping map.
func (f *Framework) ApplyTemplatedManifests(fsys fs.FS, manifestGlob string, templateMapping map[string]interface{}, options ...*client.APICallOptions) error {
	// TODO(mm4tt): Consider using the out-of-the-box "kubectl create -f".
	klog.Infof("Applying templates for %q", manifestGlob)

	templateProvider := config.NewTemplateProvider(fsys)
	manifests, err := fs.Glob(fsys, manifestGlob)
	if err != nil {
		return err
	}
	if manifests == nil {
		klog.Warningf("There is no matching file for pattern %v.\n", manifestGlob)
	}
	for _, manifest := range manifests {
		klog.V(1).Infof("Applying %s\n", manifest)
		objs, err := templateProvider.TemplateToObjects(manifest, templateMapping)
		if err != nil {
			if err == config.ErrorEmptyFile {
				klog.Warningf("Skipping empty manifest %s", manifest)
				continue
			}
			return fmt.Errorf("TemplateToObjects error: %+v", err)
		}
		for _, obj := range objs {
			objList := []unstructured.Unstructured{*obj}
			if obj.IsList() {
				list, err := obj.ToList()
				if err != nil {
					return err
				}
				objList = list.Items
			}
			for _, item := range objList {
				if err := f.CreateObject(item.GetNamespace(), item.GetName(), &item, options...); err != nil {
					return fmt.Errorf("error while applying (%s): %v", manifest, err)
				}
			}
		}

	}
	return nil
}

func (f *Framework) isAutomanagedNamespaceCurrentPrefix(name string) (bool, error) {
	return regexp.MatchString(f.automanagedNamespacePrefix+"-[1-9][0-9]*", name)
}

func (f *Framework) isStaleAutomanagedNamespace(name string) bool {
	if namespaceID.MatchString(name) {
		_, isFromThisExecution := f.getAutomanagedNamespaces()[name]
		return !isFromThisExecution
	}
	return false
}

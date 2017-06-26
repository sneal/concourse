// Code generated by counterfeiter. DO NOT EDIT.
package dbfakes

import (
	"sync"

	"github.com/concourse/atc/db"
)

type FakeCreatedVolume struct {
	HandleStub        func() string
	handleMutex       sync.RWMutex
	handleArgsForCall []struct{}
	handleReturns     struct {
		result1 string
	}
	handleReturnsOnCall map[int]struct {
		result1 string
	}
	PathStub        func() string
	pathMutex       sync.RWMutex
	pathArgsForCall []struct{}
	pathReturns     struct {
		result1 string
	}
	pathReturnsOnCall map[int]struct {
		result1 string
	}
	TypeStub        func() db.VolumeType
	typeMutex       sync.RWMutex
	typeArgsForCall []struct{}
	typeReturns     struct {
		result1 db.VolumeType
	}
	typeReturnsOnCall map[int]struct {
		result1 db.VolumeType
	}
	TeamIDStub        func() int
	teamIDMutex       sync.RWMutex
	teamIDArgsForCall []struct{}
	teamIDReturns     struct {
		result1 int
	}
	teamIDReturnsOnCall map[int]struct {
		result1 int
	}
	CreateChildForContainerStub        func(db.CreatingContainer, string) (db.CreatingVolume, error)
	createChildForContainerMutex       sync.RWMutex
	createChildForContainerArgsForCall []struct {
		arg1 db.CreatingContainer
		arg2 string
	}
	createChildForContainerReturns struct {
		result1 db.CreatingVolume
		result2 error
	}
	createChildForContainerReturnsOnCall map[int]struct {
		result1 db.CreatingVolume
		result2 error
	}
	DestroyingStub        func() (db.DestroyingVolume, error)
	destroyingMutex       sync.RWMutex
	destroyingArgsForCall []struct{}
	destroyingReturns     struct {
		result1 db.DestroyingVolume
		result2 error
	}
	destroyingReturnsOnCall map[int]struct {
		result1 db.DestroyingVolume
		result2 error
	}
	WorkerNameStub        func() string
	workerNameMutex       sync.RWMutex
	workerNameArgsForCall []struct{}
	workerNameReturns     struct {
		result1 string
	}
	workerNameReturnsOnCall map[int]struct {
		result1 string
	}
	SizeInBytesStub        func() int64
	sizeInBytesMutex       sync.RWMutex
	sizeInBytesArgsForCall []struct{}
	sizeInBytesReturns     struct {
		result1 int64
	}
	sizeInBytesReturnsOnCall map[int]struct {
		result1 int64
	}
	InitializeResourceCacheStub        func(*db.UsedResourceCache) error
	initializeResourceCacheMutex       sync.RWMutex
	initializeResourceCacheArgsForCall []struct {
		arg1 *db.UsedResourceCache
	}
	initializeResourceCacheReturns struct {
		result1 error
	}
	initializeResourceCacheReturnsOnCall map[int]struct {
		result1 error
	}
	InitializeTaskCacheStub        func(int, string, string) error
	initializeTaskCacheMutex       sync.RWMutex
	initializeTaskCacheArgsForCall []struct {
		arg1 int
		arg2 string
		arg3 string
	}
	initializeTaskCacheReturns struct {
		result1 error
	}
	initializeTaskCacheReturnsOnCall map[int]struct {
		result1 error
	}
	ContainerHandleStub        func() string
	containerHandleMutex       sync.RWMutex
	containerHandleArgsForCall []struct{}
	containerHandleReturns     struct {
		result1 string
	}
	containerHandleReturnsOnCall map[int]struct {
		result1 string
	}
	ParentHandleStub        func() string
	parentHandleMutex       sync.RWMutex
	parentHandleArgsForCall []struct{}
	parentHandleReturns     struct {
		result1 string
	}
	parentHandleReturnsOnCall map[int]struct {
		result1 string
	}
	ResourceTypeStub        func() (*db.VolumeResourceType, error)
	resourceTypeMutex       sync.RWMutex
	resourceTypeArgsForCall []struct{}
	resourceTypeReturns     struct {
		result1 *db.VolumeResourceType
		result2 error
	}
	resourceTypeReturnsOnCall map[int]struct {
		result1 *db.VolumeResourceType
		result2 error
	}
	BaseResourceTypeStub        func() (*db.UsedWorkerBaseResourceType, error)
	baseResourceTypeMutex       sync.RWMutex
	baseResourceTypeArgsForCall []struct{}
	baseResourceTypeReturns     struct {
		result1 *db.UsedWorkerBaseResourceType
		result2 error
	}
	baseResourceTypeReturnsOnCall map[int]struct {
		result1 *db.UsedWorkerBaseResourceType
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeCreatedVolume) Handle() string {
	fake.handleMutex.Lock()
	ret, specificReturn := fake.handleReturnsOnCall[len(fake.handleArgsForCall)]
	fake.handleArgsForCall = append(fake.handleArgsForCall, struct{}{})
	fake.recordInvocation("Handle", []interface{}{})
	fake.handleMutex.Unlock()
	if fake.HandleStub != nil {
		return fake.HandleStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.handleReturns.result1
}

func (fake *FakeCreatedVolume) HandleCallCount() int {
	fake.handleMutex.RLock()
	defer fake.handleMutex.RUnlock()
	return len(fake.handleArgsForCall)
}

func (fake *FakeCreatedVolume) HandleReturns(result1 string) {
	fake.HandleStub = nil
	fake.handleReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) HandleReturnsOnCall(i int, result1 string) {
	fake.HandleStub = nil
	if fake.handleReturnsOnCall == nil {
		fake.handleReturnsOnCall = make(map[int]struct {
			result1 string
		})
	}
	fake.handleReturnsOnCall[i] = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) Path() string {
	fake.pathMutex.Lock()
	ret, specificReturn := fake.pathReturnsOnCall[len(fake.pathArgsForCall)]
	fake.pathArgsForCall = append(fake.pathArgsForCall, struct{}{})
	fake.recordInvocation("Path", []interface{}{})
	fake.pathMutex.Unlock()
	if fake.PathStub != nil {
		return fake.PathStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.pathReturns.result1
}

func (fake *FakeCreatedVolume) PathCallCount() int {
	fake.pathMutex.RLock()
	defer fake.pathMutex.RUnlock()
	return len(fake.pathArgsForCall)
}

func (fake *FakeCreatedVolume) PathReturns(result1 string) {
	fake.PathStub = nil
	fake.pathReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) PathReturnsOnCall(i int, result1 string) {
	fake.PathStub = nil
	if fake.pathReturnsOnCall == nil {
		fake.pathReturnsOnCall = make(map[int]struct {
			result1 string
		})
	}
	fake.pathReturnsOnCall[i] = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) Type() db.VolumeType {
	fake.typeMutex.Lock()
	ret, specificReturn := fake.typeReturnsOnCall[len(fake.typeArgsForCall)]
	fake.typeArgsForCall = append(fake.typeArgsForCall, struct{}{})
	fake.recordInvocation("Type", []interface{}{})
	fake.typeMutex.Unlock()
	if fake.TypeStub != nil {
		return fake.TypeStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.typeReturns.result1
}

func (fake *FakeCreatedVolume) TypeCallCount() int {
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	return len(fake.typeArgsForCall)
}

func (fake *FakeCreatedVolume) TypeReturns(result1 db.VolumeType) {
	fake.TypeStub = nil
	fake.typeReturns = struct {
		result1 db.VolumeType
	}{result1}
}

func (fake *FakeCreatedVolume) TypeReturnsOnCall(i int, result1 db.VolumeType) {
	fake.TypeStub = nil
	if fake.typeReturnsOnCall == nil {
		fake.typeReturnsOnCall = make(map[int]struct {
			result1 db.VolumeType
		})
	}
	fake.typeReturnsOnCall[i] = struct {
		result1 db.VolumeType
	}{result1}
}

func (fake *FakeCreatedVolume) TeamID() int {
	fake.teamIDMutex.Lock()
	ret, specificReturn := fake.teamIDReturnsOnCall[len(fake.teamIDArgsForCall)]
	fake.teamIDArgsForCall = append(fake.teamIDArgsForCall, struct{}{})
	fake.recordInvocation("TeamID", []interface{}{})
	fake.teamIDMutex.Unlock()
	if fake.TeamIDStub != nil {
		return fake.TeamIDStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.teamIDReturns.result1
}

func (fake *FakeCreatedVolume) TeamIDCallCount() int {
	fake.teamIDMutex.RLock()
	defer fake.teamIDMutex.RUnlock()
	return len(fake.teamIDArgsForCall)
}

func (fake *FakeCreatedVolume) TeamIDReturns(result1 int) {
	fake.TeamIDStub = nil
	fake.teamIDReturns = struct {
		result1 int
	}{result1}
}

func (fake *FakeCreatedVolume) TeamIDReturnsOnCall(i int, result1 int) {
	fake.TeamIDStub = nil
	if fake.teamIDReturnsOnCall == nil {
		fake.teamIDReturnsOnCall = make(map[int]struct {
			result1 int
		})
	}
	fake.teamIDReturnsOnCall[i] = struct {
		result1 int
	}{result1}
}

func (fake *FakeCreatedVolume) CreateChildForContainer(arg1 db.CreatingContainer, arg2 string) (db.CreatingVolume, error) {
	fake.createChildForContainerMutex.Lock()
	ret, specificReturn := fake.createChildForContainerReturnsOnCall[len(fake.createChildForContainerArgsForCall)]
	fake.createChildForContainerArgsForCall = append(fake.createChildForContainerArgsForCall, struct {
		arg1 db.CreatingContainer
		arg2 string
	}{arg1, arg2})
	fake.recordInvocation("CreateChildForContainer", []interface{}{arg1, arg2})
	fake.createChildForContainerMutex.Unlock()
	if fake.CreateChildForContainerStub != nil {
		return fake.CreateChildForContainerStub(arg1, arg2)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fake.createChildForContainerReturns.result1, fake.createChildForContainerReturns.result2
}

func (fake *FakeCreatedVolume) CreateChildForContainerCallCount() int {
	fake.createChildForContainerMutex.RLock()
	defer fake.createChildForContainerMutex.RUnlock()
	return len(fake.createChildForContainerArgsForCall)
}

func (fake *FakeCreatedVolume) CreateChildForContainerArgsForCall(i int) (db.CreatingContainer, string) {
	fake.createChildForContainerMutex.RLock()
	defer fake.createChildForContainerMutex.RUnlock()
	return fake.createChildForContainerArgsForCall[i].arg1, fake.createChildForContainerArgsForCall[i].arg2
}

func (fake *FakeCreatedVolume) CreateChildForContainerReturns(result1 db.CreatingVolume, result2 error) {
	fake.CreateChildForContainerStub = nil
	fake.createChildForContainerReturns = struct {
		result1 db.CreatingVolume
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) CreateChildForContainerReturnsOnCall(i int, result1 db.CreatingVolume, result2 error) {
	fake.CreateChildForContainerStub = nil
	if fake.createChildForContainerReturnsOnCall == nil {
		fake.createChildForContainerReturnsOnCall = make(map[int]struct {
			result1 db.CreatingVolume
			result2 error
		})
	}
	fake.createChildForContainerReturnsOnCall[i] = struct {
		result1 db.CreatingVolume
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) Destroying() (db.DestroyingVolume, error) {
	fake.destroyingMutex.Lock()
	ret, specificReturn := fake.destroyingReturnsOnCall[len(fake.destroyingArgsForCall)]
	fake.destroyingArgsForCall = append(fake.destroyingArgsForCall, struct{}{})
	fake.recordInvocation("Destroying", []interface{}{})
	fake.destroyingMutex.Unlock()
	if fake.DestroyingStub != nil {
		return fake.DestroyingStub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fake.destroyingReturns.result1, fake.destroyingReturns.result2
}

func (fake *FakeCreatedVolume) DestroyingCallCount() int {
	fake.destroyingMutex.RLock()
	defer fake.destroyingMutex.RUnlock()
	return len(fake.destroyingArgsForCall)
}

func (fake *FakeCreatedVolume) DestroyingReturns(result1 db.DestroyingVolume, result2 error) {
	fake.DestroyingStub = nil
	fake.destroyingReturns = struct {
		result1 db.DestroyingVolume
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) DestroyingReturnsOnCall(i int, result1 db.DestroyingVolume, result2 error) {
	fake.DestroyingStub = nil
	if fake.destroyingReturnsOnCall == nil {
		fake.destroyingReturnsOnCall = make(map[int]struct {
			result1 db.DestroyingVolume
			result2 error
		})
	}
	fake.destroyingReturnsOnCall[i] = struct {
		result1 db.DestroyingVolume
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) WorkerName() string {
	fake.workerNameMutex.Lock()
	ret, specificReturn := fake.workerNameReturnsOnCall[len(fake.workerNameArgsForCall)]
	fake.workerNameArgsForCall = append(fake.workerNameArgsForCall, struct{}{})
	fake.recordInvocation("WorkerName", []interface{}{})
	fake.workerNameMutex.Unlock()
	if fake.WorkerNameStub != nil {
		return fake.WorkerNameStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.workerNameReturns.result1
}

func (fake *FakeCreatedVolume) WorkerNameCallCount() int {
	fake.workerNameMutex.RLock()
	defer fake.workerNameMutex.RUnlock()
	return len(fake.workerNameArgsForCall)
}

func (fake *FakeCreatedVolume) WorkerNameReturns(result1 string) {
	fake.WorkerNameStub = nil
	fake.workerNameReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) WorkerNameReturnsOnCall(i int, result1 string) {
	fake.WorkerNameStub = nil
	if fake.workerNameReturnsOnCall == nil {
		fake.workerNameReturnsOnCall = make(map[int]struct {
			result1 string
		})
	}
	fake.workerNameReturnsOnCall[i] = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) SizeInBytes() int64 {
	fake.sizeInBytesMutex.Lock()
	ret, specificReturn := fake.sizeInBytesReturnsOnCall[len(fake.sizeInBytesArgsForCall)]
	fake.sizeInBytesArgsForCall = append(fake.sizeInBytesArgsForCall, struct{}{})
	fake.recordInvocation("SizeInBytes", []interface{}{})
	fake.sizeInBytesMutex.Unlock()
	if fake.SizeInBytesStub != nil {
		return fake.SizeInBytesStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.sizeInBytesReturns.result1
}

func (fake *FakeCreatedVolume) SizeInBytesCallCount() int {
	fake.sizeInBytesMutex.RLock()
	defer fake.sizeInBytesMutex.RUnlock()
	return len(fake.sizeInBytesArgsForCall)
}

func (fake *FakeCreatedVolume) SizeInBytesReturns(result1 int64) {
	fake.SizeInBytesStub = nil
	fake.sizeInBytesReturns = struct {
		result1 int64
	}{result1}
}

func (fake *FakeCreatedVolume) SizeInBytesReturnsOnCall(i int, result1 int64) {
	fake.SizeInBytesStub = nil
	if fake.sizeInBytesReturnsOnCall == nil {
		fake.sizeInBytesReturnsOnCall = make(map[int]struct {
			result1 int64
		})
	}
	fake.sizeInBytesReturnsOnCall[i] = struct {
		result1 int64
	}{result1}
}

func (fake *FakeCreatedVolume) InitializeResourceCache(arg1 *db.UsedResourceCache) error {
	fake.initializeResourceCacheMutex.Lock()
	ret, specificReturn := fake.initializeResourceCacheReturnsOnCall[len(fake.initializeResourceCacheArgsForCall)]
	fake.initializeResourceCacheArgsForCall = append(fake.initializeResourceCacheArgsForCall, struct {
		arg1 *db.UsedResourceCache
	}{arg1})
	fake.recordInvocation("InitializeResourceCache", []interface{}{arg1})
	fake.initializeResourceCacheMutex.Unlock()
	if fake.InitializeResourceCacheStub != nil {
		return fake.InitializeResourceCacheStub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fake.initializeResourceCacheReturns.result1
}

func (fake *FakeCreatedVolume) InitializeResourceCacheCallCount() int {
	fake.initializeResourceCacheMutex.RLock()
	defer fake.initializeResourceCacheMutex.RUnlock()
	return len(fake.initializeResourceCacheArgsForCall)
}

func (fake *FakeCreatedVolume) InitializeResourceCacheArgsForCall(i int) *db.UsedResourceCache {
	fake.initializeResourceCacheMutex.RLock()
	defer fake.initializeResourceCacheMutex.RUnlock()
	return fake.initializeResourceCacheArgsForCall[i].arg1
}

func (fake *FakeCreatedVolume) InitializeResourceCacheReturns(result1 error) {
	fake.InitializeResourceCacheStub = nil
	fake.initializeResourceCacheReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCreatedVolume) InitializeResourceCacheReturnsOnCall(i int, result1 error) {
	fake.InitializeResourceCacheStub = nil
	if fake.initializeResourceCacheReturnsOnCall == nil {
		fake.initializeResourceCacheReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.initializeResourceCacheReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCreatedVolume) InitializeTaskCache(arg1 int, arg2 string, arg3 string) error {
	fake.initializeTaskCacheMutex.Lock()
	ret, specificReturn := fake.initializeTaskCacheReturnsOnCall[len(fake.initializeTaskCacheArgsForCall)]
	fake.initializeTaskCacheArgsForCall = append(fake.initializeTaskCacheArgsForCall, struct {
		arg1 int
		arg2 string
		arg3 string
	}{arg1, arg2, arg3})
	fake.recordInvocation("InitializeTaskCache", []interface{}{arg1, arg2, arg3})
	fake.initializeTaskCacheMutex.Unlock()
	if fake.InitializeTaskCacheStub != nil {
		return fake.InitializeTaskCacheStub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fake.initializeTaskCacheReturns.result1
}

func (fake *FakeCreatedVolume) InitializeTaskCacheCallCount() int {
	fake.initializeTaskCacheMutex.RLock()
	defer fake.initializeTaskCacheMutex.RUnlock()
	return len(fake.initializeTaskCacheArgsForCall)
}

func (fake *FakeCreatedVolume) InitializeTaskCacheArgsForCall(i int) (int, string, string) {
	fake.initializeTaskCacheMutex.RLock()
	defer fake.initializeTaskCacheMutex.RUnlock()
	return fake.initializeTaskCacheArgsForCall[i].arg1, fake.initializeTaskCacheArgsForCall[i].arg2, fake.initializeTaskCacheArgsForCall[i].arg3
}

func (fake *FakeCreatedVolume) InitializeTaskCacheReturns(result1 error) {
	fake.InitializeTaskCacheStub = nil
	fake.initializeTaskCacheReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCreatedVolume) InitializeTaskCacheReturnsOnCall(i int, result1 error) {
	fake.InitializeTaskCacheStub = nil
	if fake.initializeTaskCacheReturnsOnCall == nil {
		fake.initializeTaskCacheReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.initializeTaskCacheReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeCreatedVolume) ContainerHandle() string {
	fake.containerHandleMutex.Lock()
	ret, specificReturn := fake.containerHandleReturnsOnCall[len(fake.containerHandleArgsForCall)]
	fake.containerHandleArgsForCall = append(fake.containerHandleArgsForCall, struct{}{})
	fake.recordInvocation("ContainerHandle", []interface{}{})
	fake.containerHandleMutex.Unlock()
	if fake.ContainerHandleStub != nil {
		return fake.ContainerHandleStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.containerHandleReturns.result1
}

func (fake *FakeCreatedVolume) ContainerHandleCallCount() int {
	fake.containerHandleMutex.RLock()
	defer fake.containerHandleMutex.RUnlock()
	return len(fake.containerHandleArgsForCall)
}

func (fake *FakeCreatedVolume) ContainerHandleReturns(result1 string) {
	fake.ContainerHandleStub = nil
	fake.containerHandleReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) ContainerHandleReturnsOnCall(i int, result1 string) {
	fake.ContainerHandleStub = nil
	if fake.containerHandleReturnsOnCall == nil {
		fake.containerHandleReturnsOnCall = make(map[int]struct {
			result1 string
		})
	}
	fake.containerHandleReturnsOnCall[i] = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) ParentHandle() string {
	fake.parentHandleMutex.Lock()
	ret, specificReturn := fake.parentHandleReturnsOnCall[len(fake.parentHandleArgsForCall)]
	fake.parentHandleArgsForCall = append(fake.parentHandleArgsForCall, struct{}{})
	fake.recordInvocation("ParentHandle", []interface{}{})
	fake.parentHandleMutex.Unlock()
	if fake.ParentHandleStub != nil {
		return fake.ParentHandleStub()
	}
	if specificReturn {
		return ret.result1
	}
	return fake.parentHandleReturns.result1
}

func (fake *FakeCreatedVolume) ParentHandleCallCount() int {
	fake.parentHandleMutex.RLock()
	defer fake.parentHandleMutex.RUnlock()
	return len(fake.parentHandleArgsForCall)
}

func (fake *FakeCreatedVolume) ParentHandleReturns(result1 string) {
	fake.ParentHandleStub = nil
	fake.parentHandleReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) ParentHandleReturnsOnCall(i int, result1 string) {
	fake.ParentHandleStub = nil
	if fake.parentHandleReturnsOnCall == nil {
		fake.parentHandleReturnsOnCall = make(map[int]struct {
			result1 string
		})
	}
	fake.parentHandleReturnsOnCall[i] = struct {
		result1 string
	}{result1}
}

func (fake *FakeCreatedVolume) ResourceType() (*db.VolumeResourceType, error) {
	fake.resourceTypeMutex.Lock()
	ret, specificReturn := fake.resourceTypeReturnsOnCall[len(fake.resourceTypeArgsForCall)]
	fake.resourceTypeArgsForCall = append(fake.resourceTypeArgsForCall, struct{}{})
	fake.recordInvocation("ResourceType", []interface{}{})
	fake.resourceTypeMutex.Unlock()
	if fake.ResourceTypeStub != nil {
		return fake.ResourceTypeStub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fake.resourceTypeReturns.result1, fake.resourceTypeReturns.result2
}

func (fake *FakeCreatedVolume) ResourceTypeCallCount() int {
	fake.resourceTypeMutex.RLock()
	defer fake.resourceTypeMutex.RUnlock()
	return len(fake.resourceTypeArgsForCall)
}

func (fake *FakeCreatedVolume) ResourceTypeReturns(result1 *db.VolumeResourceType, result2 error) {
	fake.ResourceTypeStub = nil
	fake.resourceTypeReturns = struct {
		result1 *db.VolumeResourceType
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) ResourceTypeReturnsOnCall(i int, result1 *db.VolumeResourceType, result2 error) {
	fake.ResourceTypeStub = nil
	if fake.resourceTypeReturnsOnCall == nil {
		fake.resourceTypeReturnsOnCall = make(map[int]struct {
			result1 *db.VolumeResourceType
			result2 error
		})
	}
	fake.resourceTypeReturnsOnCall[i] = struct {
		result1 *db.VolumeResourceType
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) BaseResourceType() (*db.UsedWorkerBaseResourceType, error) {
	fake.baseResourceTypeMutex.Lock()
	ret, specificReturn := fake.baseResourceTypeReturnsOnCall[len(fake.baseResourceTypeArgsForCall)]
	fake.baseResourceTypeArgsForCall = append(fake.baseResourceTypeArgsForCall, struct{}{})
	fake.recordInvocation("BaseResourceType", []interface{}{})
	fake.baseResourceTypeMutex.Unlock()
	if fake.BaseResourceTypeStub != nil {
		return fake.BaseResourceTypeStub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fake.baseResourceTypeReturns.result1, fake.baseResourceTypeReturns.result2
}

func (fake *FakeCreatedVolume) BaseResourceTypeCallCount() int {
	fake.baseResourceTypeMutex.RLock()
	defer fake.baseResourceTypeMutex.RUnlock()
	return len(fake.baseResourceTypeArgsForCall)
}

func (fake *FakeCreatedVolume) BaseResourceTypeReturns(result1 *db.UsedWorkerBaseResourceType, result2 error) {
	fake.BaseResourceTypeStub = nil
	fake.baseResourceTypeReturns = struct {
		result1 *db.UsedWorkerBaseResourceType
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) BaseResourceTypeReturnsOnCall(i int, result1 *db.UsedWorkerBaseResourceType, result2 error) {
	fake.BaseResourceTypeStub = nil
	if fake.baseResourceTypeReturnsOnCall == nil {
		fake.baseResourceTypeReturnsOnCall = make(map[int]struct {
			result1 *db.UsedWorkerBaseResourceType
			result2 error
		})
	}
	fake.baseResourceTypeReturnsOnCall[i] = struct {
		result1 *db.UsedWorkerBaseResourceType
		result2 error
	}{result1, result2}
}

func (fake *FakeCreatedVolume) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.handleMutex.RLock()
	defer fake.handleMutex.RUnlock()
	fake.pathMutex.RLock()
	defer fake.pathMutex.RUnlock()
	fake.typeMutex.RLock()
	defer fake.typeMutex.RUnlock()
	fake.teamIDMutex.RLock()
	defer fake.teamIDMutex.RUnlock()
	fake.createChildForContainerMutex.RLock()
	defer fake.createChildForContainerMutex.RUnlock()
	fake.destroyingMutex.RLock()
	defer fake.destroyingMutex.RUnlock()
	fake.workerNameMutex.RLock()
	defer fake.workerNameMutex.RUnlock()
	fake.sizeInBytesMutex.RLock()
	defer fake.sizeInBytesMutex.RUnlock()
	fake.initializeResourceCacheMutex.RLock()
	defer fake.initializeResourceCacheMutex.RUnlock()
	fake.initializeTaskCacheMutex.RLock()
	defer fake.initializeTaskCacheMutex.RUnlock()
	fake.containerHandleMutex.RLock()
	defer fake.containerHandleMutex.RUnlock()
	fake.parentHandleMutex.RLock()
	defer fake.parentHandleMutex.RUnlock()
	fake.resourceTypeMutex.RLock()
	defer fake.resourceTypeMutex.RUnlock()
	fake.baseResourceTypeMutex.RLock()
	defer fake.baseResourceTypeMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeCreatedVolume) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ db.CreatedVolume = new(FakeCreatedVolume)

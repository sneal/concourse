package db

type Pod interface {
	ID() int
	State() string
	Name() string
	Namespace() string
	Metadata() ContainerMetadata
}

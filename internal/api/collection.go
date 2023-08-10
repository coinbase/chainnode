package api

type Collection string

// Collections applicable to all blockchains are defined here.
const (
	CollectionCheckpoints        Collection = "checkpoints"
	CollectionLatestCheckpoint   Collection = "latest"
	CollectionEarliestCheckpoint Collection = "earliest"
)

var globalCheckpointsMap = map[Collection]bool{
	CollectionLatestCheckpoint: true,
	// TODO: enable earliest. we have an issue with enabling earliest since earliest checkpoint was created once and not getting updated ever
	// CollectionEarliestCheckpoint: true,
}

func (c Collection) String() string {
	return string(c)
}

func (c Collection) IsGlobal() bool {
	_, ok := globalCheckpointsMap[c]
	return ok
}

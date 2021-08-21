package parse

type EntityType = int

const (
	FileType EntityType = iota
	DirectoryType
)

type Entity struct {
	Filename string
	Path     string
	Type     EntityType
}

func ParseFilepath(filepath string) (Entity, error) {
	var entity = Entity{Filename: "", Path: "", Type: FileType}

	return entity, nil
}

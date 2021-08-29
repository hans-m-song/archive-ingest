package parse

type RawEntity struct {
	RawAuthors string `regroup:"RawAuthors"`
	Title      string `regroup:"Title"`
	Publisher  string `regroup:"Publisher"`
	Collection string `regroup:"Collection"`
	RawTags    string `regroup:"RawTags"`
}

type Entity struct {
	Filepath, Filename, Title, Publisher, Collection string
	Authors                                          []string
	Tags                                             []string
}

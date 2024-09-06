package dev

type Doc struct {
	Id       int      // value
	Keywords []string // key
}

func BuildInvertIndex(docs []*Doc) map[string][]int {
	invertIndex := make(map[string][]int, 100)
	for _, doc := range docs {
		for _, keyword := range doc.Keywords {
			invertIndex[keyword] = append(invertIndex[keyword], doc.Id)
		}
	}
	return invertIndex
}

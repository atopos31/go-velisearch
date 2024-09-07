package types

import "strings"

// 实例化一个搜索表达式
func NewTermQuery(field, keyword string) *TermQuery {
	return &TermQuery{
		Keyword: &Keyword{Field: field, Word: keyword},
	}
}

func (tq *TermQuery) Empty() bool {
	return tq.Keyword == nil && len(tq.Must) == 0 && len(tq.Should) == 0
}

func (tq *TermQuery) And(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return tq
	}
	array := make([]*TermQuery, 0, len(queries)+1)
	if !tq.Empty() {
		array = append(array, tq)
	}
	for _, ele := range queries {
		if !ele.Empty() {
			array = append(array, ele)
		}
	}

	if len(array) == 0 {
		return tq
	}

	return &TermQuery{Must: array}
}

func (tq *TermQuery) Or(queries ...*TermQuery) *TermQuery {
	if len(queries) == 0 {
		return tq
	}
	array := make([]*TermQuery, 0, 1+len(queries))

	if !tq.Empty() {
		array = append(array, tq)
	}
	for _, ele := range queries {
		if !ele.Empty() {
			array = append(array, ele)
		}
	}

	if len(array) == 0 {
		return tq
	}
	return &TermQuery{Should: array}
}

func (tq *TermQuery) Tostring() string {
	switch {
	case tq.Keyword != nil:
		return tq.Keyword.ToString()
	case len(tq.Must) > 0:
		if len(tq.Must) == 1 {
			return tq.Must[0].Tostring()
		}

		sb := strings.Builder{}
		sb.WriteString("(")
		for _, ele := range tq.Must {
			s := ele.Tostring()
			if len(s) > 0 {
				sb.WriteString(s)
				sb.WriteString("&")
			}
		}
		s := sb.String()
		s = s[0:len(s)-1] + ")"

		return s
	case len(tq.Should) > 0:
		if len(tq.Should) == 1 {
			return tq.Should[0].Tostring()
		}

		sb := strings.Builder{}
		sb.WriteByte('(')
		for _, ele := range tq.Should {
			s := ele.Tostring()
			if len(s) > 0 {
				sb.WriteString(s)
				sb.WriteByte('|')
			}
		}
		s := sb.String()
		s = s[0:len(s)-1] + ")"
		return s
	}

	return ""
}

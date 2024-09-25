package words

import (
	"strings"

	"github.com/wangbin/jiebago"
)

type Tokenizer struct {
	seg jiebago.Segmenter
}

func NewTokenizer(wodsPath string) *Tokenizer {
	tokenizer := &Tokenizer{}
	if err := tokenizer.seg.LoadDictionary(wodsPath); err != nil {
		panic(err)
	}

	return tokenizer
}

func (t *Tokenizer) Cut(text string) []string {
	//不区分大小写
	text = strings.ToLower(text)
	//移除所有的标点符号
	text = ignoredChar(text)

	var wordMap = make(map[string]struct{})

	resultChan := t.seg.CutForSearch(text, true)
	for {
		w, ok := <-resultChan
		if !ok {
			break
		}
		if w == " " {
			continue
		}
		_, found := wordMap[w]
		if !found {
			//去除重复的词
			wordMap[w] = struct{}{}
		}
	}

	var wordsSlice []string
	for k := range wordMap {
		wordsSlice = append(wordsSlice, k)
	}

	return wordsSlice
}

func ignoredChar(str string) string {
	for _, c := range str {
		switch c {
		case '\f', '\n', '\r', '\t', '\v', '!', '"', '#', '$', '%', '&',
			'\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>',
			'?', '@', '[', '\\', '【', '】', ']', '“', '”', '「', '」', '★', '^', '·', '_', '`', '{', '|', '}', '~', '《', '》', '：',
			'（', '）', 0x3000, 0x3001, 0x3002, 0xFF01, 0xFF0C, 0xFF1B, 0xFF1F:
			str = strings.ReplaceAll(str, string(c), "")
		}
	}
	return str
}

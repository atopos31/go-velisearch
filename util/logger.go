package util

import (
	"log"
	"os"
)

var Log = log.New(os.Stdout, "[velisearch] ", log.Lshortfile|log.Ldate|log.Ltime)

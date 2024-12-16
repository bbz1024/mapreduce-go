package util

import "testing"

func TestSplitChunk2Save(t *testing.T) {
	err := SplitChunk2Save("D:\\code\\go\\mapreduce\\wordcounts.txt", "./temp", 3)
	if err != nil {
		t.Error(err)
	}

}

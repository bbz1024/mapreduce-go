package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestWordSum(t *testing.T) {
	MapReduce(
		func(source chan<- int) {
			for i := 1; i <= 100; i++ {
				source <- i
			}
		},
		func(item int, write chan<- int) {
			write <- item
		},
		func(reduceCh <-chan int) {
			sum := 0
			for item := range reduceCh {
				sum += item
			}
			fmt.Println(sum)
		},
	)
}
func TestMapReduceWordCount(t *testing.T) {

	var files = []string{"wordcount.txt"}
	MapReduce(
		func(source chan<- string) {
			for _, file := range files {
				source <- file
			}
		},
		func(item string, write chan<- map[string]int) {
			file, err := os.OpenFile(item, os.O_RDONLY, 0666)
			defer file.Close()
			if err != nil {
				fmt.Println("open file failed")
				return
			}
			reader := bufio.NewReader(file)
			for {
				line, err := reader.ReadString('\n')
				if err == io.EOF {
					if len(line) > 0 {
						fmt.Println(line)
					}
					break
				}
				if err != nil {
					fmt.Println("read file failed")
					return
				}
				for _, word := range strings.Fields(line) {
					fmt.Println(word)
					write <- map[string]int{word: 1}
					//fmt.Println(word)
				}

			}
		},
		func(reduceCh <-chan map[string]int) {
			wordCnt := make(map[string]int)
			for item := range reduceCh {
				for k, v := range item {
					wordCnt[k] += v
				}
			}
			for k, v := range wordCnt {
				fmt.Println(k, v)
			}
		},
	)
}

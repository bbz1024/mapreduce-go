package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"mapreduce/util"
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

func processFile(item string, write chan<- map[string]int) {
	// 打开文件并处理错误
	file, err := os.OpenFile(item, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", item, err)
		return
	}
	defer file.Close()

	// 使用bufio.Reader读取文件
	reader := bufio.NewReader(file)

	// 逐行读取文件
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			// EOF时处理并打印最后一行
			if len(line) > 0 {
				fmt.Println(line)
			}
			break
		}
		if err != nil {
			// 错误处理
			fmt.Printf("Failed to read file %s: %v\n", item, err)
			return
		}

		// 对每一行进行分词并发送到channel
		words := strings.Fields(line)
		for _, word := range words {
			write <- map[string]int{word: 1}
		}
	}
}

func TestMapReduceWordCount(t *testing.T) {
	MapReduce(func(source chan<- string) {
		tempDir := "./temps"
		err := util.SplitChunk2Save("wordcounts.txt", tempDir, 5000)
		if err != nil {
			fmt.Println(err)
			return
		}
		//	 read dir all file
		files, err := os.ReadDir(tempDir)
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, file := range files {
			// to map
			source <- tempDir + "/" + file.Name()
		}
	}, processFile, func(reduceCh <-chan map[string]int) {
		wordCnt := make(map[string]int)
		for item := range reduceCh {
			for k, v := range item {
				wordCnt[k] += v
			}
		}
		for k, v := range wordCnt {
			fmt.Println(k, v)
		}
	})
}
func TestWorkCountSerial(t *testing.T) {
	wordCount := make(map[string]int)
	// to map
	item := "./wordcounts.txt"
	// 打开文件并处理错误
	file, err := os.OpenFile(item, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", item, err)
		return
	}
	defer file.Close()
	// 使用bufio.Reader读取文件
	reader := bufio.NewReader(file)

	// 逐行读取文件
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			// EOF时处理并打印最后一行
			if len(line) > 0 {
				fmt.Println(line)
			}
			break
		}
		if err != nil {
			// 错误处理
			fmt.Printf("Failed to read file %s: %v\n", item, err)
			return
		}

		words := strings.Fields(line)

		for _, word := range words {
			wordCount[word]++
		}

	}

	for k, v := range wordCount {
		fmt.Println(k, v)
	}
}

func BenchmarkWordCountByMapReduce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MapReduce(func(source chan<- string) {
			tempDir := "./temps"
			err := util.SplitChunk2Save("wordcounts.txt", tempDir, 50000)
			if err != nil {
				fmt.Println(err)
				return
			}
			//	 read dir all file
			files, err := os.ReadDir(tempDir)
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, file := range files {
				// to map
				source <- tempDir + "/" + file.Name()
			}
		}, func(item string, write chan<- map[string]int) {
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
					break
				}
				if err != nil {
					fmt.Println("read file failed")
					return
				}
				for _, word := range strings.Fields(line) {
					write <- map[string]int{word: 1}
				}

			}
		}, func(reduceCh <-chan map[string]int) {
			wordCnt := make(map[string]int)
			for item := range reduceCh {
				for k, v := range item {
					wordCnt[k] += v
				}
			}
		})
	}
}
func BenchmarkWordCountBySerial(b *testing.B) {
	for i := 0; i < b.N; i++ {
		wordCount := make(map[string]int)
		// to map
		item := "./wordcounts.txt"
		// 打开文件并处理错误
		file, err := os.OpenFile(item, os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("Failed to open file %s: %v\n", item, err)
			return
		}
		defer file.Close()
		// 使用bufio.Reader读取文件
		reader := bufio.NewReader(file)

		// 逐行读取文件
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				// EOF时处理并打印最后一行
				if len(line) > 0 {
					fmt.Println(line)
				}
				break
			}
			if err != nil {
				// 错误处理
				fmt.Printf("Failed to read file %s: %v\n", item, err)
				return
			}

			words := strings.Fields(line)

			for _, word := range words {
				wordCount[word]++
			}

		}

	}
}

const sum = 100_000

func BenchmarkSumSerial(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var sum int
		for j := 1; j <= sum; j++ {
			sum += j
		}
	}
}
func BenchmarkSumMapReduce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		MapReduce(func(source chan<- int) {
			for j := 1; j <= sum; j++ {
				source <- j
			}
		}, func(item int, write chan<- int) {
			write <- item
		}, func(reduceCh <-chan int) {
			sum := 0
			for item := range reduceCh {
				sum += item
			}
		})
	}
}

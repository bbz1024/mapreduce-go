package util

import (
	"bufio"
	"fmt"
	"os"
)

/*
	将文件按行进行等分割，存储到临时文件中
*/

func SplitChunk2Save(inputFile string, outputDir string, linesPerFile int) error {
	// 检查输出目录是否存在，不存在则创建
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		err := os.MkdirAll(outputDir, 0755) // 创建目录及子目录
		if err != nil {
			return fmt.Errorf("could not create output directory: %v", err)
		}
	}

	// 打开输入文件
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("could not open input file: %v", err)
	}
	defer file.Close()

	// 读取文件内容
	scanner := bufio.NewScanner(file)
	lineCount := 0
	fileCount := 1
	var outputFile *os.File
	var writer *bufio.Writer

	// 按行读取文件并分割
	for scanner.Scan() {
		if lineCount%linesPerFile == 0 {
			// 关闭当前输出文件
			if outputFile != nil {
				writer.Flush()
				outputFile.Close()
			}

			// 创建新的输出文件
			outputFileName := fmt.Sprintf("%s/output_%d.txt", outputDir, fileCount)
			outputFile, err = os.Create(outputFileName)
			if err != nil {
				return fmt.Errorf("could not create output file: %v", err)
			}
			writer = bufio.NewWriter(outputFile)
			fileCount++
		}

		// 写入当前行到输出文件
		writer.WriteString(scanner.Text() + "\n")
		lineCount++
	}

	// 刷新并关闭最后一个文件
	if outputFile != nil {
		writer.Flush()
		outputFile.Close()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %v", err)
	}

	return nil
}

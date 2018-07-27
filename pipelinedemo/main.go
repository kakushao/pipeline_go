package main

import (
	"pipeline_go/pipeline"
	"fmt"
	"os"
	"bufio"
	//"github.com/gpmgo/gopm/modules/log"
)

func main() {

	const fileName = "large.in"
	const n = 100000000

	//从文件写
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipieline.RandomSource(n)

	writer := bufio.NewWriter(file)
	//写到文件里
	pipieline.WriterSink(writer, p)

	//flush，不管多少，把缓存中的数据一次刷入到磁盘
	//write只有在满的时候才会刷入磁盘
	writer.Flush()




	//从文件读
	file, err = os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p = pipieline.ReaderSource(bufio.NewReader(file), -1)

	count := 0
	for v := range p {
		fmt.Print(v,"\n")

		count ++
		if count >= 100 {
			break
		}
	}

}


func mergeDemo() {
	//创建出通道
	//p:= pipieline.ArraySource(6,5,3,8,1,5)
	//p:= pipieline.InMemSort(pipieline.ArraySource(6,5,3,8,1,5))
	p := pipieline.Merge(pipieline.InMemSort(pipieline.ArraySource(6, 5, 3, 8, 1, 5)), pipieline.InMemSort(pipieline.ArraySource(2, 6, 4, 7, 3, 5)))

	//接受通道里的值
	for v := range p {
		fmt.Println("print arraysource", v)
	}
}

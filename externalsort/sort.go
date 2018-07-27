package main

import (
	"os"
	"pipeline_go/pipeline"
	"bufio"
	"fmt"
	"log"
	"strconv"
)

func main() {
	p:= createNetworkPipeline("large.in", 800000000, 4)

	writeToFile(p, "large.out")

	printFile("large.out")

}

func printFile(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipieline.ReaderSource(file, -1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func writeToFile(p <-chan int, fileName string) {
	file, err := os.Create(fileName)

	//os.Create(fileName) //0666
	//file,err := os.Open(fileName)//o_rdonly

	log.Print(".......", err)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	pipieline.WriterSink(writer, p)

	err = writer.Flush()
	if err != nil {
		//log.Println("+++++++",err)//文件描述符有误，o_rdonly!!
		panic(err)
	}

}

//创建通道
//这里其实应该还有一个file.close(),可以作为参数返回出来。这样才是一个好的做法。
func createNetworkPipeline(fileName string, fileSize, chunkCount int) <-chan int {

	//把数据分组
	//假设没有问题
	chunkSize := fileSize / chunkCount
	pipieline.Init()

	//创建容器，存储满载的管道
	//sortResults := []<-chan int{}
	sortAddr := []string{}

	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		//设置偏移量，0代表从开头，1代表当前，2代表末尾。
		file.Seek(int64(i*chunkSize), 0)

		//生成数据，并把他们读入通道
		source := pipieline.ReaderSource(bufio.NewReader(file), chunkSize)

		//排序后的数据读入通道，并追加
		//sortResults = append(sortResults,pipieline.InMemSort(source))

		addr := ":" + strconv.Itoa(7000+i)
		pipieline.NetworkSink(addr, pipieline.InMemSort(source))
		sortAddr = append(sortAddr,addr)
	}



	sortResults := []<-chan int{}
	for _,addr := range sortAddr{
		sortResults = append(sortResults,pipieline.NetworkSource(addr))
	}
	return pipieline.MergeN(sortResults...)
}

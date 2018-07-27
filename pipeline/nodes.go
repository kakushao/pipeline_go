package pipieline

import (
	"sort"
	"io"
	"encoding/binary"
	"math/rand"
	"time"
	"fmt"
)



var startTime time.Time

func Init()  {
	startTime = time.Now()
}



//向通道读数据
func ArraySource(a ...int) <-chan int {
    //声明通道
	out := make(chan int)
	//开启线程，向通道传送数据
	go func() {
		for _, v := range a {
			out <- v
		}
		//关闭通道
		close(out)
	}()
	//返回通道
	return out
}


//写出通道数据，排序后，再读入通道
func InMemSort(in <-chan int) <-chan int {

	//声明通道
	out := make(chan int,1024)

	go func() {
		//写出in通道数据
		s := []int{}
		for v := range in {
			//存储到内存中
			s = append(s, v)
		}

		fmt.Println("read done: ",time.Now().Sub(startTime))

		//排序
		sort.Ints(s)

		fmt.Println("inMemSort done: ",time.Now().Sub(startTime))

		//读入out通道
		for _, v := range s {
			out <- v
		}
		//关闭out
		close(out)

	}()

	return out
}


//合并数组
func Merge(in1, in2 <-chan int) <-chan int {

	out := make(chan int,1024)
	go func() {
		//接受数据
		v1, ok1 := <-in1
		v2, ok2 := <-in2

		//比较
		for ok1 || ok2 {
			if !ok2 || (ok1 && v1 <= v2) {
				//把v1传出去
				out <- v1
				//有点类似于.next
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		//关闭通道
		close(out)
		fmt.Println("merg done: ",time.Now().Sub(startTime))
	}()

	//返回
	return out

}

//从文件里，向通道读数据
func ReaderSource(reader io.Reader,chunkSize int) <-chan int {
	//创建通道
	out := make(chan int,1024)
	go func() {
		//设置一个缓存为8的数组
		buffer := make([]byte, 8)

		//统计读了多少数据
		bytesRead := 0

		for {
			//开始读
			n, err := reader.Read(buffer)
			//累加数据
			bytesRead += n
			if n > 0 {
				//把数据处理一下
				v := int(binary.BigEndian.Uint64(buffer))
				//发送数据
				out <- v
			}
			//-1代表全部读，不是-1代表只能读bytesRead个数据
			if err != nil || (chunkSize != -1 &&bytesRead >= chunkSize) {
				break
			}
		}

		//关闭
		close(out)
	}()

	return out
}

//把通道数据写入文件
func WriterSink(writer io.Writer, in <-chan int) {

	//把通道数据写出来
	for v := range in {
		//设置缓存大小
		buffer := make([]byte, 8)
		//处理数据格式
		binary.BigEndian.PutUint64(buffer, uint64(v))
		//把缓存数据写入文件
		writer.Write(buffer)

	}

}

//以随机数作为数据，写入通道
func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {

			out <- rand.Int()
		}
		close(out)
	}()
	return out
}


//多个通道数据归并
func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}

	//分成多个数组
	//此处不考虑能否整除问题
	m := len(inputs)/2

	return Merge(MergeN(inputs[:m]...), MergeN(inputs[m:]...))
	
}
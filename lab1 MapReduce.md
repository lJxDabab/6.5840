# lab1 MapReduce

mapreduce论文网址：[rfeet.qrk (mit.edu)](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf),感觉讲的一般，唯一比较有用的信息为其中的图片：

![image-20231011194401838](C:\Users\LJX\AppData\Roaming\Typora\typora-user-images\image-20231011194401838.png)

## 大致的流程为：

map worker先将输入文件进行解析，输出为一个个的（key,value），但此时value被统筹为一个最基本个体；将map输出的一个个键值对，通过hash算法，放入一个个hash桶里，每个map创造出的hash桶的个数为reduce任务的个数。而每个hash桶，则表现为一个中间文件（Intermediate files）。接下来的reduce worker，将对应的hash桶的keyvalue值全部统筹收集到一起，并将一个个的基本个体value,组合成一个values的集合个体，让一个key只有对应的一个values。同时将这个key和values输入reduce函数进行values的处理（这个集合应该如何做加减），最后将这些值写入当前reduce函数对应的一个输出文件中。n个reduce task就应该生成n个output file。

## 针对流程理解，以统计文件中单词个数为例：

我们最终希望生成若干“abandon 5”这样的键值对，因此这里key为单词的字符串，value为其出现的次数。

首先,输入文件被Map worker解析，生成若干的"abandon 1","words 1"的键值对，其中如果有多次同一单词的出现，也就会出现"abandon 1""abandon 1""abandon 1"，这样的多个结果。随后map将这些键值对，写入由其key值通过hash映射后得到的哈希桶中，也就是中间文件中，每个Mapworker有其自己的固定ID,X，key值映射后也会得到一个值Y，因此简单的命名为mr-X-Y,又lab建议键值对已json的格式保存，因此文件命名为mr-X-Y.json:<img src="C:\Users\LJX\AppData\Roaming\Typora\typora-user-images\image-20231011200555744.png" alt="image-20231011200555744" style="zoom: 67%;" />

源码展示：

```go
reply:=MapReply{}
	reply.Finished=false
	for true{
	reply=reqMap()//这里向coordinator进行rpc申请map任务
	if reply.Finished==false {
filename:=reply.Filename
	WorkID:=reply.WorkID //得到当前的map worker的ID
	data,_:=ioutil.ReadFile(filename) //读取input file,获得数据
	kva:=mapf(filename,string(data))//经过map函数处理，得到若干值为1的键值对
	mediulname:=""
	exkvs:=make(map[int][]KeyValue)
	for i:=0;i<len(kva);i++{
		idx:=ihash(kva[i].Key)%reply.NReduce //将每个key通过hash函数取reduce任务个数的余，得到其哈希桶的ID号
		exkvs[idx]=append(exkvs[idx],kva[i]) //将键值对存放入哈希桶中
	}
	for idx:=0;idx<reply.NReduce;idx++{
        //这里将每个哈希桶里的键值对写入对应名称的中间文件中
		mediulname="mr-"+strconv.Itoa(WorkID)+"-"+strconv.Itoa(idx)+".json"
		mediulfile,_:=os.OpenFile(mediulname,os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644)
		encoder:=json.NewEncoder(mediulfile)
		encoder.Encode(exkvs[idx])
		mediulfile.Close()
        //MapReturn告诉
		mapReturn(WorkID)
	}
}
```

因为上述中间文件被视为hash桶，其哈希桶ID表现在文件名字的最后一个数字，因此，同一个reduce任务应该读取所有最后一个数字相同的文件，即1-x,2-x,3-x,4-x......,这样。redcue任务负责先把这些键值对，拥有相同key的值，转换为一个集合，再将这个集合交给reduce函数处理（如果value是整数值，那么这个集合应该形如{1，1，1，1，1，1}，如果value是名字，那么这个集合应该形如{张三，李四，赵五}，至于这个集合如何进行加法逻辑，交给reduce处理，事实上，本lab的map和reduce被写成了一个接口，其函数的内容由命令行输入时，其参数的xxx.so的动态链接库决定。

```go
for reply.Finished==true && Rfinished==false{
		replyR=reqReduce()
		if replyR.Finished==true{
			break
		}
		container:=[]KeyValue{}
		exkv:=[]KeyValue{}
				for i:=0;i<8;i++{
                    //打开所有的结尾为reduce taskID的中间文件，并把内容做个整合，塞进container中
					exfile,_:=os.Open("mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(replyR.ReduceID)+".json")
					encoder:=json.NewDecoder(exfile)
					encoder.Decode(&exkv)
					exfile.Close()
					container=append(container,exkv...)
				}
    //这里把contianer里面内容拍一下序，让所有key相同的键值对排到一起。这个sort.Sort函数的使用还需要定义几个接口，详情看Bkey的定义
		sort.Sort(BKey(container))
		containerNew:=[]KeyValues{}
		strList:=[]string{}
    //下面整个for循环的处理就是将相同key的value做一个整合，形成集合并作为reduce函数的输入，这里集合表现为strList
		for idx,_:=range container{
			if idx==0{
				strList=append(strList,container[0].Value)
				continue
			}
			if container[idx].Key!=container[idx-1].Key {
				containerNew=append(containerNew,KeyValues{container[idx-1].Key,strList})
				strList=[]string{container[idx].Value}
				if idx==(len(container)-1){
					containerNew=append(containerNew,KeyValues{container[idx].Key,strList})
				}
				continue
			}
			strList=append(strList,container[idx].Value)
			if idx==(len(container)-1){
				containerNew=append(containerNew,KeyValues{container[idx].Key,strList})
			}
		}
    //形成集合并作为reduce函数的输入,生成key,values键值对
		strs:=""
		for _,ele:=range containerNew{
			strs+=fmt.Sprintf("%v %v\n",ele.Key,reducef(ele.Key,ele.Values))
		}
    //把读取的所有相应中间文件得到的key values值给输入到输出文件中，并向Coordinator报告完成
		file,_:=os.OpenFile("mr-out-"+strconv.Itoa(replyR.ReduceID),os.O_CREATE|os.O_RDWR,0644)
		file.WriteString(strs)
		file.Close()
		Rfinished=ReduceReturn(replyR.ReduceID)
	}
```

整个mapreduce的逻辑大抵如上所示。

## 下面谈一些其他的细节点：

### Coordinator如何应对那些崩溃的，处理速度很慢的，这样长时间占用worker和任务资源得不到释放的worker:

- **The coordinator can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. The best you can do is have the coordinator wait for some amount of time, and then give up and re-issue the task to a different worker. For this lab, have the coordinator wait for ten seconds; after that the coordinator should assume the worker has died (of course, it might not have).**

对那些长久不响应的worker，大概率也完不成了，或者完成太影响效率了，此时应该等待若干时间，剥夺其任务的权限，回收任务资源，并把这个任务交给别的worker做。这个过程可以用一个协程解决，再每次coordinator分配worker任务的时候开启一个协程，这个协程在睡眠若干时间后检查该任务是否被完成了，若未被完成，则应该告知coordinator进行处理，map task worker的相应处理方式如下：

```go
func (c *Coordinator) RPC_handler(args *MapArgs, reply *MapReply) error{
	reply.NReduce=c.nReduce
	c.ayulock.Lock()
	fmt.Print("")
	for idx,file:=range c.ayufiles_undo_M{
		if file!=(ayufile{}) {
			reply.WorkID=idx
			reply.Filename=file.Filename
			c.ayufiles_undo_M[idx]=ayufile{}
			c.ayufiles_doing_M[idx]=file
			go c.Mapwait(reply.WorkID) //创造一个协程
			c.ayulock.Unlock()
			return nil
		}
}
for _,file:=range c.ayufiles_doing_M{
	if file!=(ayufile{}) {
		c.ayulock.Unlock()
		time.Sleep(5*time.Millisecond)
		return nil
	}
}
	reply.Finished=true
	c.ayulock.Unlock()
	return nil
}
func (c *Coordinator)Mapwait(id int){
	time.Sleep(10*time.Second) //该协程睡眠过后来检查任务是否完成
	c.ayulock.Lock()
	if c.ayufiles_doing_M[id]!=(ayufile{}){
		c.ayufiles_undo_M[id]=c.ayufiles_doing_M[id]
		c.ayufiles_doing_M[id]=ayufile{}
	}
	c.ayulock.Unlock()
}
```

### coordinator如何管理它的任务资源的：

我的设计是这样的，我给每种任务分配了2个数组，这里数组被当作了map使用，因为worker的ID同时也就作为了数组下标去查询其对应的文件名或者别的。一个数组为未分配给worker的文件集合，一个数组为正分配给worker处理，但未完成的文件集合。如果worker向coordinator申请了一个任务并被赋予了，则第一个数组的对应单元被置空,第二个数组相应单元被填充；worker处理完了文件，则它会从第二个数组中除去相应单元内容，即被置空；如果worker被coordinator判定为不太能相应的worker(即过了固定时间coordinator还未得到相应)，则将该文件从第二个数组中置空，并填充到第一个数组中。

同时对于其任务资源，作为共享变量，应该适宜的上锁进行保护。

### 关于worker:

每个worker应该能同时执map和reduce的相关内容，即既能当map worker又能当reduce worker,我执行的方案是，考虑到map和reduce的执行先后顺序，将worker写成了先map再reduce的形式，当map全部执行完，才会让所有进程去结束对reduce进程的阻塞。同时同一个worker应该有能连续处理多个task的能力，所以我将每个map/reduce task写成了循环的形式。

至于如何像coordinator进行请求，lab提供了rpc的方法，worker可以直接请求coordinator的方法。
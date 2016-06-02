package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"os"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	contents, _ := ioutil.ReadFile(inFile)
	kvs := mapF(inFile, string(contents))

	outFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outName := reduceName(jobName, mapTaskNumber, i)
		outFiles[i], _ = os.Create(outName)
		encs[i] = json.NewEncoder(outFiles[i])
		defer outFiles[i].Close()
	}

	for _, kv := range kvs {
		r := ihash(kv.Key) % uint32(nReduce)
		encs[r].Encode(kv)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

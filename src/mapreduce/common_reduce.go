package mapreduce

import (
	"os"
	"io"
	"encoding/json"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inName := reduceName(jobName, i, reduceTaskNumber)
		inFile, _ := os.Open(inName)
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break;
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	var keys []string
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outName := mergeName(jobName, reduceTaskNumber)
	outFile, _ := os.Create(outName)
	enc := json.NewEncoder(outFile)
	for _, k := range keys {
		result := reduceF(k, kvMap[k])
		enc.Encode(KeyValue{k, result})
	}
	outFile.Close()
}

package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	cFinish := make(chan bool)

	for i := 0; i < ntasks; i++ {
		go func(i int) {
			args := &DoTaskArgs {
				JobName:       mr.jobName,
				File:          mr.files[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nios }
			for {
				worker := <- mr.registerChannel
				ok := call(worker, "Worker.DoTask", args, new(struct{}))
				if ok {
					cFinish <- true  // this must run before the next command, 
					                 // otherwise the last several tasks get stuck
					mr.registerChannel <- worker
					break
				}
			}
		}(i)
	}

	for i := 0; i < ntasks; i++ {
		<- cFinish
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

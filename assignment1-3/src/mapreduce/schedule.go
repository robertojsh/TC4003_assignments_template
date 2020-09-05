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

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//m

	//mapper
	//reducers

	for task:=0; task < ntasks;task++ {

		//"Special Master Thread"
		//mr


		//RunWorker(mr.address,task,)


		var doTaskArgs DoTaskArgs
		doTaskArgs.JobName = mr.jobName
		doTaskArgs.File = mr.files[task]
		doTaskArgs.Phase = phase
		doTaskArgs.TaskNumber = task
		doTaskArgs.NumOtherPhase = nios

		worker := <- mr.registerChannel
		ok := call(worker, "Worker.DoTask", doTaskArgs, new(struct{}))
		if ok == false {
			fmt.Printf("Register: RPC %s register error\n", mr.workers[task])
		}
		//call() from common_rpc
		//RunWorker() from worker
		//??mapF and reduceF
		//RunWorker(mr.address,string(task),mapF,reduceF,0)


		//Communicate via RPC
		//worker code at worker.go

		// DoTaskArgs holds the arguments that are passed to a worker when a job is
		// scheduled on it.


		//scheduled on workers
		//mr.files[task] //input file per each task
	}



	debug("Schedule: %v phase done\n", phase)
}

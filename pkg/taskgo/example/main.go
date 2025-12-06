package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jursonmo/practise_new/pkg/taskgo"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel

	taskMgr := taskgo.NewTaskGo(ctx)

	taskMgr.Go("tasksleep1", func(ctx context.Context) error {
		time.Sleep(time.Second)
		log.Println("tasksleep1 finished")
		return nil
	})
	taskMgr.Go("tasksleep2", func(ctx context.Context) error {
		time.Sleep(time.Second * 2)
		log.Println("tasksleep2 finished")
		return nil
	})

	taskMgr.Go("tasksleep3", func(ctx context.Context) error {
		<-ctx.Done()
		log.Println("tasksleep3 over, ctx.Done()") //测试任务内的ctx 是否能被cancel
		return nil
	})

	taskMgr.Go("tasksleep4", func(ctx context.Context) error {
		<-ctx.Done()
		panic(fmt.Errorf("tasksleep4 panic")) //测试panic, 能否被捕获。
		//panic("tasksleep4 panic")
		return nil
	})
	taskMgr.Go("tasksleep5", func(ctx context.Context) error {
		time.Sleep(time.Second * 5)
		log.Println("tasksleep5 finished")
		return nil
	})

	//wait for tasksleep1 and tasksleep2 and tasksleep3 and tasksleep4 done, tasksleep4 is panic err; tasksleep5 may be running:unfinished tasks
	time.Sleep(time.Second * 3)
	log.Println("stopping taskgo...")
	err := taskMgr.StopAndWait(time.Millisecond * 100)
	log.Println("StopAndWait err:", err)

	log.Println("finished tasks:", taskMgr.FinishedTasksName())

	log.Printf("unfinished tasksState:%+v", taskMgr.UnfinishedTasksState())
	log.Printf("finished tasksState:%+v", taskMgr.FinishedTasksState())

	//测试重复stop
	err = taskMgr.StopAndWait(time.Millisecond * 100)
	log.Println("StopAndWait err:", err) //return err: already stoped
}

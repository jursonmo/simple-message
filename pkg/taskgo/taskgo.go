package taskgo

//copy from jursonmo/practise_new/pkg/taskgo
import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	Stoped = 2
)

// 一个任务也容易就起几个goroutine去完成, 但是这个stop 这个任务，需要知道哪些goroutine已经
// 处理完成，哪些没有处理完成，不然你可能就有goroutine泄露, 我们不能等待goroutine多到影响业务的
// 时候从用pprof去查看，那时太晚了，而且不容易快速查出问题，
// 应该是再结束一个任务时，就要保证其下的goroutine的能正常地在规定的时间
// 退出,否则就打印error, 开发人员提前去查问题。
type TaskGo struct {
	ctx       context.Context
	cancel    context.CancelFunc //取消任务时默认都调用context.CancelFunc
	canceFunc func()             //用户自定义自己取消任务的回调handler, 必须是非阻塞。默认为空。
	tasks     map[string]*TaskState
	sync.Mutex
	status   int32
	tasksNum int32
	doneCh   chan struct{} // notfiy when all tasks are done
}

type TaskState struct {
	TaskName string
	StartAt  time.Time
	DoneAt   time.Time
	Err      error
}

func NewTaskGo(ctx context.Context) *TaskGo {
	tg := &TaskGo{tasks: make(map[string]*TaskState), doneCh: make(chan struct{}, 1)}
	tg.ctx, tg.cancel = context.WithCancel(ctx)
	return tg
}

func (tg *TaskGo) SetCancelFunc(f func()) {
	tg.canceFunc = f
}

func (tg *TaskGo) stop() {
	tg.Lock()
	defer tg.Unlock()
	tg.status = Stoped
}

func (tg *TaskGo) IsStoped() bool {
	tg.Lock()
	defer tg.Unlock()
	return tg.isStoped()
}

func (tg *TaskGo) isStoped() bool {
	return tg.status == Stoped
}

func (tg *TaskGo) Go(taskName string, f func(ctx context.Context) error) error {
	tg.Lock()
	defer tg.Unlock()

	if tg.isStoped() {
		return errors.New("taskgo is stoped")
	}

	_, b := tg.tasks[taskName]
	if b {
		return fmt.Errorf("task:%s already running", taskName)
	}
	ts := &TaskState{TaskName: taskName, StartAt: time.Now()}
	tg.tasks[taskName] = ts
	tg.tasksNum += 1

	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				if v, ok := r.(error); ok {
					err = fmt.Errorf("panic recover:%w", v)
				} else {
					err = fmt.Errorf("panic recover:%v", r)
				}
			}
			tg.done(ts, err)
		}()
		err = f(tg.ctx)
	}()
	return nil
}

func (tg *TaskGo) done(ts *TaskState, err error) {
	tg.Lock()
	defer tg.Unlock()
	//log.Printf("goroutine:%v finish\n", r.TaskName)
	ts.Err = err
	ts.DoneAt = time.Now()

	tg.tasksNum -= 1
	if tg.tasksNum < 0 {
		panic("tasksNum < 0, never happend")
	}
	//由于每次拉起goroutine之前都会检查是否stop
	//如果关掉，并且目前tasksNum为0，说明不可能有goroutine再运行了
	if tg.isStoped() && tg.tasksNum == 0 {
		tg.doneCh <- struct{}{}
	}
}

func (ts *TaskState) unCompleted() bool {
	return ts.DoneAt.IsZero() //没有结束，DoneAt 为“0”
}

func (tg *TaskGo) UnfinishedTasksName() []string {
	return tg.iterTasks(func(ts *TaskState) bool {
		return ts.unCompleted()
	})
}

func (tg *TaskGo) FinishedTasksName() []string {
	return tg.iterTasks(func(ts *TaskState) bool {
		return !ts.unCompleted()
	})
}

func (tg *TaskGo) AllTasksName() []string {
	return tg.iterTasks(func(ts *TaskState) bool {
		return ts != nil
	})
}

// Traversal tasks
func (tg *TaskGo) iterTasks(condition func(ts *TaskState) bool) []string {
	tg.Lock()
	defer tg.Unlock()
	tasks := make([]string, 0, len(tg.tasks))
	for name, ts := range tg.tasks {
		if condition(ts) {
			tasks = append(tasks, name)
		}
	}
	return tasks
}

func (tg *TaskGo) UnfinishedTasksState() []TaskState {
	return tg.iterTasksState(func(ts *TaskState) bool {
		return ts.unCompleted()
	})
}

func (tg *TaskGo) FinishedTasksState() []TaskState {
	return tg.iterTasksState(func(ts *TaskState) bool {
		return !ts.unCompleted()
	})
}

func (tg *TaskGo) iterTasksState(condition func(ts *TaskState) bool) []TaskState {
	tg.Lock()
	defer tg.Unlock()
	tasks := make([]TaskState, 0, len(tg.tasks))
	for _, ts := range tg.tasks {
		if condition(ts) {
			tasks = append(tasks, *ts)
		}
	}
	return tasks
}

func (tg *TaskGo) StopAndWait(d time.Duration) error {
	// if tg.IsStoped() {
	// 	return errors.New("already stoped")
	// }
	// tg.stop()
	//上面这两个操作不能保证原子性; 用下面的方式来确保只有一个goroutine能执行到后面的取消任务。
	tg.Lock()
	if tg.status == Stoped {
		tg.Unlock()
		return nil
	}
	tg.status = Stoped
	tg.Unlock()

	tg.cancel()
	if tg.canceFunc != nil {
		tg.canceFunc()
	}
	select {
	case <-time.After(d):
		//stop的期限到了，goroutine没有全部退出，把没有退出的goroutine 输出
		tasks := tg.UnfinishedTasksName()
		return fmt.Errorf("unfinish tasks:%v", tasks)
	case <-tg.doneCh:
		//task下的所有goroutine都已经退出了
		return nil
	}
}

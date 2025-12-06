package taskgo

//copy from jursonmo/practise_new/pkg/taskgo
import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// # 运行所有测试
// go test ./
// # 运行测试并显示详细信息
// go test -v ./
// # 运行特定测试
// go test -run TestTaskGo_BasicTask ./
// 测试创建任务管理器
func TestNewTaskGo(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	// 验证创建的任务管理器不为nil
	if tg == nil {
		t.Fatalf("NewTaskGo returned nil")
	}

	// 验证内部状态
	if len(tg.tasks) != 0 {
		t.Errorf("Expected empty tasks map, got %d tasks", len(tg.tasks))
	}

	if tg.tasksNum != 0 {
		t.Errorf("Expected tasksNum to be 0, got %d", tg.tasksNum)
	}

	if tg.IsStoped() {
		t.Errorf("Expected taskgo to not be stopped")
	}
}

// 测试基本的任务启动和完成
func TestTaskGo_BasicTask(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	// 定义一个简单任务
	taskName := "basic-task"
	taskDone := make(chan struct{})

	err := tg.Go(taskName, func(ctx context.Context) error {
		<-taskDone // 等待信号
		return nil
	})

	if err != nil {
		t.Fatalf("Expected no error when starting task, got %v", err)
	}

	// 验证任务已添加
	unfinished := tg.UnfinishedTasksName()
	if len(unfinished) != 1 || unfinished[0] != taskName {
		t.Errorf("Expected one unfinished task named %s, got %v", taskName, unfinished)
	}

	// 完成任务
	close(taskDone)

	// 等待任务完成
	time.Sleep(10 * time.Millisecond)

	// 验证任务已完成
	finished := tg.FinishedTasksName()
	if len(finished) != 1 || finished[0] != taskName {
		t.Errorf("Expected one finished task named %s, got %v", taskName, finished)
	}
}

// 测试任务执行错误
func TestTaskGo_TaskWithError(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)
	expectedErr := errors.New("test error")
	taskName := "error-task"

	err := tg.Go(taskName, func(ctx context.Context) error {
		return expectedErr
	})

	if err != nil {
		t.Fatalf("Expected no error when starting task, got %v", err)
	}

	// 等待任务完成
	time.Sleep(10 * time.Millisecond)

	// 验证任务状态
	finishedTasks := tg.FinishedTasksState()
	if len(finishedTasks) != 1 {
		t.Fatalf("Expected one finished task, got %d", len(finishedTasks))
	}

	if finishedTasks[0].Err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, finishedTasks[0].Err)
	}
}

// 测试任务取消
func TestTaskGo_TaskCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tg := NewTaskGo(ctx)
	taskName := "cancel-task"

	err := tg.Go(taskName, func(ctx context.Context) error {
		// 等待上下文取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second): // 防止测试卡住
			return errors.New("task timed out")
		}
	})

	if err != nil {
		t.Fatalf("Expected no error when starting task, got %v", err)
	}

	// 取消上下文
	cancel()

	// 等待任务处理取消
	time.Sleep(10 * time.Millisecond)

	// 验证任务已完成
	finished := tg.FinishedTasksName()
	if len(finished) != 1 || finished[0] != taskName {
		t.Errorf("Expected one finished task named %s, got %v", taskName, finished)
	}
}

// 测试StopAndWait功能
func TestTaskGo_StopAndWait(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	// 创建多个任务
	taskCount := 3
	tasksDone := make(chan struct{}, taskCount)

	for i := 0; i < taskCount; i++ {
		taskName := fmt.Sprintf("task-0%d", i)
		tg.Go(taskName, func(ctx context.Context) error {
			<-tasksDone // 等待信号
			return nil
		})
	}

	// 停止任务管理器，并设置一个长超时
	go func() {
		// 释放任务
		time.Sleep(10 * time.Millisecond)
		for i := 0; i < taskCount; i++ {
			tasksDone <- struct{}{}
		}
	}()

	timeout := 500 * time.Millisecond
	err := tg.StopAndWait(timeout)
	if err != nil {
		t.Fatalf("Expected StopAndWait to succeed, got error: %v", err)
	}

	// 验证所有任务已完成
	if len(tg.UnfinishedTasksName()) != 0 {
		t.Errorf("Expected all tasks to be finished, but some are still running: %v", tg.UnfinishedTasksName())
	}
}

// 测试StopAndWait超时
func TestTaskGo_StopAndWaitTimeout(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	// 创建一个不会结束的任务
	taskName := "never-ending-task"
	tg.Go(taskName, func(ctx context.Context) error {
		<-ctx.Done()
		// 即使上下文取消，也继续等待很长时间，模拟无法正常退出的任务
		time.Sleep(1 * time.Second)
		return ctx.Err()
	})

	// 停止任务管理器，但设置一个短超时
	timeout := 50 * time.Millisecond
	err := tg.StopAndWait(timeout)
	if err == nil {
		t.Fatalf("Expected StopAndWait to timeout, but it succeeded")
	}

	// 验证错误信息包含未完成的任务
	if len(tg.UnfinishedTasksName()) == 0 {
		t.Errorf("Expected unfinished tasks, but none found")
	}
}

// 测试任务panic恢复
func TestTaskGo_PanicRecovery(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	taskName := "panic-task"
	tg.Go(taskName, func(ctx context.Context) error {
		// 故意触发panic
		panic("test panic")
	})

	// 等待panic被恢复并任务完成
	time.Sleep(10 * time.Millisecond)

	// 验证任务已标记为完成
	finished := tg.FinishedTasksName()
	if len(finished) != 1 || finished[0] != taskName {
		t.Errorf("Expected one finished task named %s, got %v", taskName, finished)
	}

	// 验证错误包含panic信息
	taskStates := tg.FinishedTasksState()
	if len(taskStates) != 1 || taskStates[0].Err == nil || taskStates[0].Err.Error() != "panic recover:test panic" {
		t.Errorf("Expected task to have panic recovery error, got %v", taskStates[0].Err)
	}
}

// 测试并发安全性
func TestTaskGo_Concurrency(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	// 并发启动多个任务
	taskCount := 100
	var wg sync.WaitGroup

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		taskName := fmt.Sprintf("concurrent-task-0%d", i%10) // 复用一些任务名以测试重复任务名检查
		go func(name string) {
			defer wg.Done()
			tg.Go(name, func(ctx context.Context) error {
				time.Sleep(1 * time.Millisecond) // 模拟工作
				return nil
			})
		}(taskName)
	}

	wg.Wait()

	// 等待所有任务完成
	time.Sleep(10 * time.Millisecond)

	// 验证任务数量合理
	allTasks := tg.AllTasksName()
	if len(allTasks) == 0 {
		t.Errorf("Expected some tasks to be recorded, got 0")
	}
}

// 测试自定义取消函数
func TestTaskGo_SetCancelFunc(t *testing.T) {
	ctx := context.Background()
	tg := NewTaskGo(ctx)

	cancelFuncCalled := false
	cancelFuncMutex := sync.Mutex{}

	// 设置自定义取消函数
	tg.SetCancelFunc(func() {
		cancelFuncMutex.Lock()
		defer cancelFuncMutex.Unlock()
		cancelFuncCalled = true
	})

	// 启动一个简单任务
	tg.Go("task-with-cancel-func", func(ctx context.Context) error {
		time.Sleep(1 * time.Second) // 模拟长时间运行
		return nil
	})

	// 停止任务管理器
	tg.StopAndWait(10 * time.Millisecond)

	// 验证自定义取消函数被调用
	cancelFuncMutex.Lock()
	defer cancelFuncMutex.Unlock()
	if !cancelFuncCalled {
		t.Errorf("Expected custom cancel function to be called")
	}
}

package management

import (
	"errors"
	"testing"
	"time"
)

func TestChatGPTWebMutationTaskManagerPrunesOldestTerminalAtCapacity(t *testing.T) {
	manager := newChatGPTWebMutationTaskManager()
	now := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return now }

	var oldestID string
	for index := 0; index < chatGPTWebLoginTaskMaxRetained; index++ {
		task, _, errCreate := manager.create(chatGPTWebMutationTaskImport, []chatGPTWebMutationTaskResult{{Status: chatGPTWebLoginResultQueued}})
		if errCreate != nil {
			t.Fatalf("create task %d: %v", index, errCreate)
		}
		if index == 0 {
			oldestID = task.ID
		}
		manager.finish(chatGPTWebMutationTaskImport, task.ID, true)
		now = now.Add(time.Second)
	}

	replacement, _, errCreate := manager.create(chatGPTWebMutationTaskImport, []chatGPTWebMutationTaskResult{{Status: chatGPTWebLoginResultQueued}})
	if errCreate != nil {
		t.Fatalf("create replacement: %v", errCreate)
	}
	defer manager.finish(chatGPTWebMutationTaskImport, replacement.ID, true)
	if _, ok := manager.get(chatGPTWebMutationTaskImport, oldestID); ok {
		t.Fatal("oldest terminal task was not pruned")
	}
}

func TestChatGPTWebMutationTaskManagerRejectsCapacityWhenAllTasksActive(t *testing.T) {
	manager := newChatGPTWebMutationTaskManager()
	ids := make([]string, 0, chatGPTWebMutationTaskMaxActive)
	for index := 0; index < chatGPTWebMutationTaskMaxActive; index++ {
		task, _, errCreate := manager.create(chatGPTWebMutationTaskConversion, []chatGPTWebMutationTaskResult{{Status: chatGPTWebLoginResultQueued}})
		if errCreate != nil {
			t.Fatalf("create task %d: %v", index, errCreate)
		}
		ids = append(ids, task.ID)
	}
	defer func() {
		for _, id := range ids {
			manager.finish(chatGPTWebMutationTaskConversion, id, true)
		}
	}()

	_, _, errCreate := manager.create(chatGPTWebMutationTaskConversion, []chatGPTWebMutationTaskResult{{Status: chatGPTWebLoginResultQueued}})
	if !errors.Is(errCreate, errChatGPTWebLoginTaskCapacity) {
		t.Fatalf("create at active capacity error = %v, want %v", errCreate, errChatGPTWebLoginTaskCapacity)
	}
}

func TestChatGPTWebMutationTaskFinishCancelsTaskContext(t *testing.T) {
	manager := newChatGPTWebMutationTaskManager()
	task, taskCtx, errCreate := manager.create(chatGPTWebMutationTaskImport, []chatGPTWebMutationTaskResult{{Status: chatGPTWebLoginResultQueued}})
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	manager.finish(chatGPTWebMutationTaskImport, task.ID, true)

	select {
	case <-taskCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("completed mutation task context was not canceled")
	}
}

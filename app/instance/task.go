package instance

import (
	"time"

	"github.com/revel/revel"
	"github.com/shatteredsilicon/qan-api/app/db"
	"github.com/shatteredsilicon/qan-api/app/shared"
)

type Task struct {
	Type shared.InstanceTask
	Data interface{}
}

type Tasker struct {
	queue chan Task
	dbm   db.Manager
}

func NewTasker(size int) *Tasker {
	return &Tasker{
		queue: make(chan Task, size),
		dbm:   db.NewMySQLManager(),
	}
}

func (tasker *Tasker) Add(t shared.InstanceTask, data interface{}) {
	tasker.queue <- Task{
		Type: t,
		Data: data,
	}
}

func (tasker *Tasker) Run() {
	for task := range tasker.queue {
		if err := tasker.dbm.Open(); err != nil {
			revel.ERROR.Printf("InstanceTasker failed to open the db connection: %s for task: %+v", err.Error(), task)
			continue
		}

		switch task.Type {
		case shared.TypeInstanceTaskDelete:
			uuid, ok := task.Data.(string)
			if !ok {
				revel.WARN.Printf("InstanceTasker received an unexpected Delete task: %+v", task)
				break
			}

			ih := NewMySQLHandler(tasker.dbm)
			go func() {
				defer func() {
					recover()
				}()

				// continually remove qan data in case there are some ongoing data insertion
				retryTimes := 30
				for i := 0; i < retryTimes; i++ {
					<-time.NewTimer(5 * time.Second).C
					err := ih.DeleteData(uuid)
					if err != nil {
						revel.ERROR.Printf("InstanceTasker failed to delete instance qan data: %s for task: %+v", err.Error(), task)
					}
				}
			}()
		}
	}
}

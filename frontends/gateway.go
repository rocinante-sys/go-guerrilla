package frontends

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/phires/go-guerrilla/log"
	"github.com/phires/go-guerrilla/response"
)

var ErrProcessorNotFound error

// A frontend gateway is a proxy that implements the Frontend interface.
// It is used to start multiple goroutine workers for saving mail, and then distribute email saving to the workers
// via a channel. Shutting down via Shutdown() will stop all workers.
// The rest of this program always talks to the frontend via this gateway.
type FrontendGateway struct {
	// channel for distributing envelopes to workers
	conveyor chan *workerMsg

	// waits for frontend workers to start/stop
	wg           sync.WaitGroup
	workStoppers []chan bool
	processors   []Processor
	// validators   []Processor

	// controls access to state
	sync.Mutex
	State    frontendState
	config   FrontendConfig
	gwConfig *GatewayConfig
}

type GatewayConfig struct {
	// WorkersSize controls how many concurrent workers to start. Defaults to 1
	WorkersSize int `json:"save_workers_size,omitempty"`
	// PreProcess is like ProcessorStack, but for recipient validation tasks
	PreProcess string `json:"pre_process,omitempty"`
	// TimeoutSave is duration before timeout when saving an email, eg "29s"
	TimeoutSave string `json:"gw_save_timeout,omitempty"`
	// TimeoutProcessCmd duration before timeout when validating a recipient, eg "1s"
	TimeoutProcessCmd string `json:"gw_val_cmd_timeout,omitempty"`
}

// workerMsg is what get placed on the FrontendGateway.saveMailChan channel
type workerMsg struct {
	// The email data
	in *[]byte
	// notifyMe is used to notify the gateway of workers finishing their processing
	notifyMe chan *notifyMsg
	// select the task type
	task SelectTask
}

type frontendState int

// possible values for state
const (
	FrontendStateNew frontendState = iota
	FrontendStateRunning
	FrontendStateShuttered
	FrontendStateError
	FrontendStateInitialized

	// default timeout for saving email, if 'gw_save_timeout' not present in config
	saveTimeout = time.Second * 30
	// default timeout for validating rcpt to, if 'gw_val_rcpt_timeout' not present in config
	validateRcptTimeout = time.Second * 5
	defaultProcessor    = "Debugger"
)

func (s frontendState) String() string {
	switch s {
	case FrontendStateNew:
		return "NewState"
	case FrontendStateRunning:
		return "RunningState"
	case FrontendStateShuttered:
		return "ShutteredState"
	case FrontendStateError:
		return "ErrorSate"
	case FrontendStateInitialized:
		return "InitializedState"
	}
	return strconv.Itoa(int(s))
}

// New makes a new default FrontendGateway frontend, and initializes it using
// frontendConfig and stores the logger
func New(frontendConfig FrontendConfig, l log.Logger) (Frontend, error) {
	Svc.SetMainlog(l)
	gateway := &FrontendGateway{}
	err := gateway.Initialize(frontendConfig)
	if err != nil {
		return nil, fmt.Errorf("error while initializing the frontend: %s", err)
	}
	// keep the config known to be good.
	gateway.config = frontendConfig

	f = Frontend(gateway)
	return f, nil
}

var workerMsgPool = sync.Pool{
	// if not available, then create a new one
	New: func() interface{} {
		return &workerMsg{}
	},
}

// reset resets a workerMsg that has been borrowed from the pool
func (w *workerMsg) reset(in *[]byte, task SelectTask) {
	if w.notifyMe == nil {
		w.notifyMe = make(chan *notifyMsg)
	}
	w.in = in
	w.task = task
}

// Process distributes an envelope to one of the frontend workers with a TaskParseCmd task
func (gw *FrontendGateway) Process(in *[]byte) Result {
	if gw.State != FrontendStateRunning {
		return NewResult(response.Canned.FailFrontendNotRunning, response.SP, gw.State)
	}
	// borrow a workerMsg from the pool
	workerMsg := workerMsgPool.Get().(*workerMsg)
	workerMsg.reset(in, TaskParseCmd)
	// place on the channel so that one of the save mail workers can pick it up
	gw.conveyor <- workerMsg
	// wait for the save to complete
	// or timeout
	select {
	case status := <-workerMsg.notifyMe:
		// email saving transaction completed
		if status.result == FrontendResultOK && status.queuedID != "" {
			return NewResult(response.Canned.SuccessMessageQueued, response.SP, status.queuedID)
		}

		// A custom result, there was probably an error, if so, log it
		if status.result != nil {
			if status.err != nil {
				Log().Error(status.err)
			}
			return status.result
		}

		// if there was no result, but there's an error, then make a new result from the error
		if status.err != nil {
			if _, err := strconv.Atoi(status.err.Error()[:3]); err != nil {
				return NewResult(response.Canned.FailFrontendTransaction, response.SP, status.err)
			}
			return NewResult(status.err)
		}

		// both result & error are nil (should not happen)
		err := errors.New("no response from frontend - processor did not return a result or an error")
		Log().Error(err)
		return NewResult(response.Canned.FailFrontendTransaction, response.SP, err)

	case <-time.After(gw.saveTimeout()):
		Log().Error("Frontend has timed out while saving email")
		// e.Lock() // lock the envelope - it's still processing here, we don't want the server to recycle it
		go func() {
			// keep waiting for the frontend to finish processing
			<-workerMsg.notifyMe
			// e.Unlock()
			workerMsgPool.Put(workerMsg)
		}()
		return NewResult(response.Canned.FailFrontendTimeout)
	}
}

// ProcessCmd asks one of the workers to process the data
func (gw *FrontendGateway) ProcessCmd(in *[]byte) EmailError {
	if gw.State != FrontendStateRunning {
		return StorageNotAvailable
	}
	// place on the channel so that one of the save mail workers can pick it up
	workerMsg := workerMsgPool.Get().(*workerMsg)
	workerMsg.reset(in, TaskParseCmd)
	gw.conveyor <- workerMsg
	// wait for the validation to complete
	// or timeout
	select {
	case status := <-workerMsg.notifyMe:
		workerMsgPool.Put(workerMsg)
		if status.err != nil {
			return status.err
		}
		return nil

	case <-time.After(gw.processCmdTimeout()):
		// in.Lock()
		go func() {
			<-workerMsg.notifyMe
			// in.Unlock()
			workerMsgPool.Put(workerMsg)
			Log().Error("Frontend has timed out while processing cmd")
		}()
		return StorageTimeout
	}
}

// Shutdown shuts down the frontend and leaves it in FrontendStateShuttered state
func (gw *FrontendGateway) Shutdown() error {
	gw.Lock()
	defer gw.Unlock()
	if gw.State != FrontendStateShuttered {
		// send a signal to all workers
		gw.stopWorkers()
		// wait for workers to stop
		gw.wg.Wait()
		// call shutdown on all processor shutdowners
		if err := Svc.shutdown(); err != nil {
			return err
		}
		gw.State = FrontendStateShuttered
	}
	return nil
}

// Reinitialize initializes the gateway with the existing config after it was shutdown
func (gw *FrontendGateway) Reinitialize() error {
	if gw.State != FrontendStateShuttered {
		return errors.New("frontend must be in FrontendStateshuttered state to Reinitialize")
	}
	// clear the Initializers and Shutdowners
	Svc.reset()

	err := gw.Initialize(gw.config)
	if err != nil {
		fmt.Println("reinitialize to ", gw.config, err)
		return fmt.Errorf("error while initializing the frontend: %s", err)
	}

	return err
}

// newStack creates a new Processor by chaining multiple Processors in a call stack
// Decorators are functions of Decorator type, source files prefixed with p_*
// Each decorator does a specific task during the processing stage.
// This function uses the config value save_process or validate_process to figure out which Decorator to use
func (gw *FrontendGateway) newStack(stackConfig string) (Processor, error) {
	var decorators []Decorator
	cfg := strings.ToLower(strings.TrimSpace(stackConfig))
	if len(cfg) == 0 {
		//cfg = strings.ToLower(defaultProcessor)
		return NoopProcessor{}, nil
	}
	items := strings.Split(cfg, "|")
	for i := range items {
		name := items[len(items)-1-i] // reverse order, since decorators are stacked
		if makeFunc, ok := processors[name]; ok {
			decorators = append(decorators, makeFunc())
		} else {
			ErrProcessorNotFound = fmt.Errorf("processor [%s] not found", name)
			return nil, ErrProcessorNotFound
		}
	}
	// build the call-stack of decorators
	p := Decorate(DefaultProcessor{}, decorators...)
	return p, nil
}

// loadConfig loads the config for the GatewayConfig
func (gw *FrontendGateway) loadConfig(cfg FrontendConfig) error {
	configType := BaseConfig(&GatewayConfig{})
	// Note: treat config values as immutable
	// if you need to change a config value, change in the file then
	// send a SIGHUP
	bcfg, err := Svc.ExtractConfig(cfg, configType)
	if err != nil {
		return err
	}
	gw.gwConfig = bcfg.(*GatewayConfig)
	return nil
}

// Initialize builds the workers and initializes each one
func (gw *FrontendGateway) Initialize(cfg FrontendConfig) error {
	gw.Lock()
	defer gw.Unlock()
	if gw.State != FrontendStateNew && gw.State != FrontendStateShuttered {
		return errors.New("can only Initialize in FrontendStateNew or FrontendStateShuttered state")
	}
	err := gw.loadConfig(cfg)
	if err != nil {
		gw.State = FrontendStateError
		return err
	}
	workersSize := gw.workersSize()
	if workersSize < 1 {
		gw.State = FrontendStateError
		return errors.New("must have at least 1 worker")
	}
	gw.processors = make([]Processor, 0)
	// gw.validators = make([]Processor, 0)
	for i := 0; i < workersSize; i++ {
		p, err := gw.newStack(gw.gwConfig.PreProcess)
		if err != nil {
			gw.State = FrontendStateError
			return err
		}
		gw.processors = append(gw.processors, p)

		// 	v, err := gw.newStack(gw.gwConfig.ValidateProcess)
		// 	if err != nil {
		// 		gw.State = FrontendStateError
		// 		return err
		// 	}
		// 	gw.validators = append(gw.validators, v)
	}

	// initialize processors
	if err := Svc.initialize(cfg); err != nil {
		gw.State = FrontendStateError
		return err
	}
	if gw.conveyor == nil {
		gw.conveyor = make(chan *workerMsg, workersSize)
	}
	// ready to start
	gw.State = FrontendStateInitialized
	return nil
}

// Start starts the worker goroutines, assuming it has been initialized or shuttered before
func (gw *FrontendGateway) Start() error {
	gw.Lock()
	defer gw.Unlock()
	if gw.State == FrontendStateInitialized || gw.State == FrontendStateShuttered {
		// we start our workers
		workersSize := gw.workersSize()
		// make our slice of channels for stopping
		gw.workStoppers = make([]chan bool, 0)
		// set the wait group
		gw.wg.Add(workersSize)

		for i := 0; i < workersSize; i++ {
			stop := make(chan bool)
			go func(workerId int, stop chan bool) {
				// blocks here until the worker exits
				for {
					state := gw.workDispatcher(
						gw.conveyor,
						gw.processors[workerId],
						// gw.validators[workerId],
						workerId+1,
						stop)
					// keep running after panic
					if state != dispatcherStatePanic {
						break
					}
				}
				gw.wg.Done()
			}(i, stop)
			gw.workStoppers = append(gw.workStoppers, stop)
		}
		gw.State = FrontendStateRunning
		return nil
	} else {
		return fmt.Errorf("cannot start frontend because it's in %s state", gw.State)
	}
}

// workersSize gets the number of workers to use for saving email by reading the save_workers_size config value
// Returns 1 if no config value was set
func (gw *FrontendGateway) workersSize() int {
	if gw.gwConfig.WorkersSize <= 0 {
		return 1
	}
	return gw.gwConfig.WorkersSize
}

// saveTimeout returns the maximum amount of seconds to wait before timing out a save processing task
func (gw *FrontendGateway) saveTimeout() time.Duration {
	if gw.gwConfig.TimeoutSave == "" {
		return saveTimeout
	}
	t, err := time.ParseDuration(gw.gwConfig.TimeoutSave)
	if err != nil {
		return saveTimeout
	}
	return t
}

// validateRcptTimeout returns the maximum amount of seconds to wait before timing out a recipient validation  task
func (gw *FrontendGateway) processCmdTimeout() time.Duration {
	if gw.gwConfig.TimeoutProcessCmd == "" {
		return validateRcptTimeout
	}
	t, err := time.ParseDuration(gw.gwConfig.TimeoutProcessCmd)
	if err != nil {
		return validateRcptTimeout
	}
	return t
}

type dispatcherState int

const (
	dispatcherStateStopped dispatcherState = iota
	dispatcherStateIdle
	dispatcherStateWorking
	dispatcherStateNotify
	dispatcherStatePanic
)

func (gw *FrontendGateway) workDispatcher(
	workIn chan *workerMsg,
	process Processor,
	workerId int,
	stop chan bool) (state dispatcherState) {

	var msg *workerMsg

	defer func() {

		// panic recovery mechanism: it may panic when processing
		// since processors may call arbitrary code, some may be 3rd party / unstable
		// we need to detect the panic, and notify the frontend that it failed & unlock the envelope
		if r := recover(); r != nil {
			Log().Error("worker recovered from panic:", r, string(debug.Stack()))

			if state == dispatcherStateWorking {
				msg.notifyMe <- &notifyMsg{err: errors.New("storage failed")}
			}
			state = dispatcherStatePanic
			return
		}
		// state is dispatcherStateStopped if it reached here

	}()
	state = dispatcherStateIdle
	Log().Infof("processing worker started (#%d)", workerId)
	for {
		select {
		case <-stop:
			state = dispatcherStateStopped
			Log().Infof("stop signal for worker (#%d)", workerId)
			return
		case msg = <-workIn:
			state = dispatcherStateWorking // recovers from panic if in this state
			result, err := process.Process(msg.in, msg.task)
			state = dispatcherStateNotify
			msg.notifyMe <- &notifyMsg{err: err, result: result}
		}
		state = dispatcherStateIdle
	}
}

// stopWorkers sends a signal to all workers to stop
func (gw *FrontendGateway) stopWorkers() {
	for i := range gw.workStoppers {
		gw.workStoppers[i] <- true
	}
}

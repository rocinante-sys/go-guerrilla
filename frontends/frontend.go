package frontends

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/phires/go-guerrilla/log"
)

var (
	Svc *service

	// Store the constructor for making an new processor decorator.
	processors map[string]ProcessorConstructor

	f Frontend
)

func init() {
	Svc = &service{}
	processors = make(map[string]ProcessorConstructor)
}

type ProcessorConstructor func() Decorator

// Frontends process received mail. Depending on the implementation, they can store mail in the database,
// write to a file, check for spam, re-transmit to another server, etc.
// Must return an SMTP message (i.e. "250 OK") and a boolean indicating
// whether the message was processed successfully.
type Frontend interface {
	// Process processes then saves the mail envelope
	Process(*[]byte) Result
	// PreProcess path
	ProcessCmd(in *[]byte) EmailError
	// Initializes the frontend, eg. creates folders, sets-up database connections
	Initialize(FrontendConfig) error
	// Initializes the frontend after it was Shutdown()
	Reinitialize() error
	// Shutdown frees / closes anything created during initializations
	Shutdown() error
	// Start Starts a frontend that has been initialized
	Start() error
}

type FrontendConfig map[string]interface{}

// All config structs extend from this
type BaseConfig interface{}

type notifyMsg struct {
	err      error
	queuedID string
	result   Result
}

// Result represents a response to an SMTP client after receiving DATA.
// The String method should return an SMTP message ready to send back to the
// client, for example `250 OK: Message received`.
type Result interface {
	fmt.Stringer
	// Code should return the SMTP code associated with this response, ie. `250`
	Code() int
}

// Internal implementation of FrontendResult for use by frontend implementations.
type result struct {
	// we're going to use a bytes.Buffer for building a string
	bytes.Buffer
}

func (r *result) String() string {
	return r.Buffer.String()
}

// Parses the SMTP code from the first 3 characters of the SMTP message.
// Returns 554 if code cannot be parsed.
func (r *result) Code() int {
	trimmed := strings.TrimSpace(r.String())
	if len(trimmed) < 3 {
		return 554
	}
	code, err := strconv.Atoi(trimmed[:3])
	if err != nil {
		return 554
	}
	return code
}

func NewResult(r ...interface{}) Result {
	buf := new(result)
	for _, item := range r {
		switch v := item.(type) {
		case error:
			_, _ = buf.WriteString(v.Error())
		case fmt.Stringer:
			_, _ = buf.WriteString(v.String())
		case string:
			_, _ = buf.WriteString(v)
		}
	}
	return buf
}

type processorInitializer interface {
	Initialize(frontendConfig FrontendConfig) error
}

type processorShutdowner interface {
	Shutdown() error
}

type InitializeWith func(frontendConfig FrontendConfig) error
type ShutdownWith func() error

// Satisfy ProcessorInitializer interface
// So we can now pass an anonymous function that implements ProcessorInitializer
func (i InitializeWith) Initialize(frontendConfig FrontendConfig) error {
	// delegate to the anonymous function
	return i(frontendConfig)
}

// satisfy ProcessorShutdowner interface, same concept as InitializeWith type
func (s ShutdownWith) Shutdown() error {
	// delegate
	return s()
}

type Errors []error

// implement the Error interface
func (e Errors) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	// multiple errors
	msg := ""
	for _, err := range e {
		msg += "\n" + err.Error()
	}
	return msg
}

func convertError(name string) error {
	return fmt.Errorf("failed to load frontend config (%s)", name)
}

type service struct {
	initializers []processorInitializer
	shutdowners  []processorShutdowner
	sync.Mutex
	mainlog atomic.Value
}

// Get loads the log.logger in an atomic operation. Returns a stderr logger if not able to load
func Log() log.Logger {
	if v, ok := Svc.mainlog.Load().(log.Logger); ok {
		return v
	}
	l, _ := log.GetLogger(log.OutputStderr.String(), log.InfoLevel.String())
	return l
}

func (s *service) SetMainlog(l log.Logger) {
	s.mainlog.Store(l)
}

// AddInitializer adds a function that implements ProcessorShutdowner to be called when initializing
func (s *service) AddInitializer(i processorInitializer) {
	s.Lock()
	defer s.Unlock()
	s.initializers = append(s.initializers, i)
}

// AddShutdowner adds a function that implements ProcessorShutdowner to be called when shutting down
func (s *service) AddShutdowner(sh processorShutdowner) {
	s.Lock()
	defer s.Unlock()
	s.shutdowners = append(s.shutdowners, sh)
}

// reset clears the initializers and Shutdowners
func (s *service) reset() {
	s.shutdowners = make([]processorShutdowner, 0)
	s.initializers = make([]processorInitializer, 0)
}

// Initialize initializes all the processors one-by-one and returns any errors.
// Subsequent calls to Initialize will not call the initializer again unless it failed on the previous call
// so Initialize may be called again to retry after getting errors
func (s *service) initialize(frontend FrontendConfig) Errors {
	s.Lock()
	defer s.Unlock()
	var errors Errors
	failed := make([]processorInitializer, 0)
	for i := range s.initializers {
		if err := s.initializers[i].Initialize(frontend); err != nil {
			errors = append(errors, err)
			failed = append(failed, s.initializers[i])
		}
	}
	// keep only the failed initializers
	s.initializers = failed
	return errors
}

// Shutdown shuts down all the processors by calling their shutdowners (if any)
// Subsequent calls to Shutdown will not call the shutdowners again unless it failed on the previous call
// so Shutdown may be called again to retry after getting errors
func (s *service) shutdown() Errors {
	s.Lock()
	defer s.Unlock()
	var errors Errors
	failed := make([]processorShutdowner, 0)
	for i := range s.shutdowners {
		if err := s.shutdowners[i].Shutdown(); err != nil {
			errors = append(errors, err)
			failed = append(failed, s.shutdowners[i])
		}
	}
	s.shutdowners = failed
	return errors
}

// AddProcessor adds a new processor, which becomes available to the frontend_config.save_process option
// and also the frontend_config.validate_process option
// Use to add your own custom processor when using frontends as a package, or after importing an external
// processor.
func (s *service) AddProcessor(name string, p ProcessorConstructor) {
	// wrap in a constructor since we want to defer calling it
	var c ProcessorConstructor
	c = func() Decorator {
		return p()
	}
	// add to our processors list
	processors[strings.ToLower(name)] = c
}

// extractConfig loads the frontend config. It has already been unmarshalled
// configData contains data from the main config file's "frontend_config" value
// configType is a Processor's specific config value.
// The reason why using reflection is because we'll get a nice error message if the field is missing
// the alternative solution would be to json.Marshal() and json.Unmarshal() however that will not give us any
// error messages
func (s *service) ExtractConfig(configData FrontendConfig, configType BaseConfig) (interface{}, error) {
	// Use reflection so that we can provide a nice error message
	v := reflect.ValueOf(configType).Elem() // so that we can set the values
	//m := reflect.ValueOf(configType).Elem()
	t := reflect.TypeOf(configType).Elem()
	typeOfT := v.Type()

	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		// read the tags of the config struct
		fieldName := t.Field(i).Tag.Get("json")
		omitempty := false
		if len(fieldName) > 0 {
			// parse the tag to
			// get the field name from struct tag
			split := strings.Split(fieldName, ",")
			fieldName = split[0]
			if len(split) > 1 {
				if split[1] == "omitempty" {
					omitempty = true
				}
			}
		} else {
			// could have no tag
			// so use the reflected field name
			fieldName = typeOfT.Field(i).Name
		}
		if f.Type().Name() == "int" {
			// in json, there is no int, only floats...
			if intVal, converted := configData[fieldName].(float64); converted {
				v.Field(i).SetInt(int64(intVal))
			} else if intVal, converted := configData[fieldName].(int); converted {
				v.Field(i).SetInt(int64(intVal))
			} else if !omitempty {
				return configType, convertError("property missing/invalid: '" + fieldName + "' of expected type: " + f.Type().Name())
			}
		}
		if f.Type().Name() == "string" {
			if stringVal, converted := configData[fieldName].(string); converted {
				v.Field(i).SetString(stringVal)
			} else if !omitempty {
				return configType, convertError("missing/invalid: '" + fieldName + "' of type: " + f.Type().Name())
			}
		}
		if f.Type().Name() == "bool" {
			if boolVal, converted := configData[fieldName].(bool); converted {
				v.Field(i).SetBool(boolVal)
			} else if !omitempty {
				return configType, convertError("missing/invalid: '" + fieldName + "' of type: " + f.Type().Name())
			}
		}
	}
	return configType, nil
}

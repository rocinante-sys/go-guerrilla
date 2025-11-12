package frontends

type SelectTask int

const (
	TaskParseCmd SelectTask = iota
)

func (o SelectTask) String() string {
	switch o {
	case TaskParseCmd:
		return "parse cmd"
	}
	return "[unnamed task]"
}

var FrontendResultOK = NewResult("200 OK")

// Our processor is defined as something that processes the envelope and returns a result and error
type Processor interface {
	Process(*[]byte, SelectTask) (Result, error)
}

// Signature of Processor
type ProcessWith func(*[]byte, SelectTask) (Result, error)

// Make ProcessWith will satisfy the Processor interface
func (f ProcessWith) Process(in *[]byte, task SelectTask) (Result, error) {
	// delegate to the anonymous function
	return f(in, task)
}

// DefaultProcessor is a undecorated worker that does nothing
// Notice DefaultProcessor has no knowledge of the other decorators that have orthogonal concerns.
type DefaultProcessor struct{}

// do nothing except return the result
// (this is the last call in the decorator stack, if it got here, then all is good)
func (w DefaultProcessor) Process(in *[]byte, task SelectTask) (Result, error) {
	return FrontendResultOK, nil
}

// if no processors specified, skip operation
type NoopProcessor struct{ DefaultProcessor }

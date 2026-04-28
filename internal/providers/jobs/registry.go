package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// Registry maps job Kind strings to Go functions, using reflection to JSON-decode the payload
// (a JSON array) into handler parameters.
//
// Reason: The same string kinds are enqueued in the database and used at run time, matching
// stable handler names as job kinds without an extra code generator.
// Trade-offs: Reflective dispatch is slower than hand-written switches; v1 keeps this for
// registration symmetry with existing handler names. Constraints: Register must run on fully
// typed functions; payload must be a JSON array matching the number of non-context parameters.
type Registry struct {
	codec    adapter.JSON
	handlers map[string]registryHandler
}

type registryHandler struct {
	fn  reflect.Value
	inT []reflect.Type // one per non-context input parameter, In(1)..In(n-1)
}

// NewRegistry returns an empty registry. json must be the process-wide adapter.JSON (e.g. from
// main) so payload decoding uses the same implementation as the rest of the app. Call Register
// before starting a Worker.
func NewRegistry(json adapter.JSON) *Registry {
	if json == nil {
		panic("jobs.NewRegistry: json is nil")
	}
	return &Registry{codec: json, handlers: make(map[string]registryHandler)}
}

// Register binds kind to fn. fn must be a function with first parameter assignable to
// context.Context, optional additional parameters, and a final return of type error.
// Every non-context parameter type must be decodable from JSON. Panics on invalid signatures
// to surface configuration mistakes at process startup, not in production traffic.
func (r *Registry) Register(kind string, fn any) {
	if kind == "" {
		panic("jobs.Register: kind is empty")
	}
	if r.handlers == nil {
		r.handlers = make(map[string]registryHandler)
	}
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		panic("jobs.Register: fn must be a function")
	}
	typ := v.Type()
	if typ.NumIn() < 1 {
		panic("jobs.Register: function must have at least one parameter (context.Context)")
	}
	ctxIface := reflect.TypeFor[context.Context]()
	if !typ.In(0).AssignableTo(ctxIface) {
		panic("jobs.Register: first parameter must be context.Context")
	}
	if typ.NumOut() != 1 {
		panic("jobs.Register: function must have exactly one return value (error)")
	}
	if !typ.Out(0).Implements(reflectTypeError()) {
		panic("jobs.Register: return value must be error")
	}
	var inT []reflect.Type
	for i := 1; i < typ.NumIn(); i++ {
		t := typ.In(i)
		if err := validateJSONParamType(r.codec, t); err != nil {
			panic(fmt.Sprintf("jobs.Register: parameter %d (%s) is not JSON-decodable: %v", i, t, err))
		}
		inT = append(inT, t)
	}
	if _, ok := r.handlers[kind]; ok {
		panic(fmt.Sprintf("jobs.Register: duplicate kind %q", kind))
	}
	r.handlers[kind] = registryHandler{fn: v, inT: inT}
}

// Dispatch looks up the handler for job.Kind, decodes job.Payload as a JSON array, and calls
// the function with ctx and decoded args.
//
// Reason: A single code path for invoke keeps Worker small; the wire format (JSON array) is
// defined once. Constraints: Payload must align with the handler arity from Register; mismatch
// returns a non-retrying error and the job should fail in the store layer.
func (r *Registry) Dispatch(ctx context.Context, job *schema.Job) error {
	if job == nil {
		return fmt.Errorf("jobs.Dispatch: job is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h, ok := r.handlers[job.Kind]
	if !ok {
		return fmt.Errorf("jobs.Dispatch: no handler for kind %q", job.Kind)
	}
	return h.call(ctx, job.Payload, r.codec)
}

func reflectTypeError() reflect.Type {
	return reflect.TypeFor[error]()
}

func (h *registryHandler) call(ctx context.Context, payload []byte, j adapter.JSON) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// Reason: Reflective invokes can panic on bad arity or bugs in handlers; without recovery
			// the worker goroutine dies and the job stays "running". Log with job-scoped ctx so
			// zapsentry/Sentry sees the hub from ContextWithSentryJobHandler (see worker.executeJob).
			logger.ErrorCtx(ctx, fmt.Errorf("job handler panic: %v", r), zap.Any("panic", r))
			err = fmt.Errorf("jobs: handler panic: %v", r)
		}
	}()

	p := bytesNormPayloadForArray(payload)
	if len(h.inT) == 0 {
		var raws []json.RawMessage
		if err := j.Unmarshal(p, &raws); err != nil {
			return fmt.Errorf("decode payload: %w", err)
		}
		if len(raws) > 0 {
			return fmt.Errorf("jobs: want 0 args got %d", len(raws))
		}
		ret := h.fn.Call([]reflect.Value{reflect.ValueOf(ctx)})
		err = returnAsError(ret)
		return err
	}
	var raws []json.RawMessage
	if err := j.Unmarshal(p, &raws); err != nil {
		return fmt.Errorf("decode payload array: %w", err)
	}
	if len(raws) != len(h.inT) {
		return fmt.Errorf("jobs: arg count mismatch for kind: want %d got %d", len(h.inT), len(raws))
	}
	args := make([]reflect.Value, 1+len(h.inT))
	args[0] = reflect.ValueOf(ctx)
	for i, t := range h.inT {
		arg, err := decodeArg(j, t, raws[i])
		if err != nil {
			return fmt.Errorf("arg %d: %w", i, err)
		}
		args[i+1] = arg
	}
	ret := h.fn.Call(args)
	err = returnAsError(ret)
	return err
}

// bytesNormPayloadForArray ensures a JSON top-level array for varargs. Empty or whitespace-only
// input becomes "[]" so a handler with no extra params still decodes.
func bytesNormPayloadForArray(b []byte) []byte {
	if len(strings.TrimSpace(string(b))) == 0 {
		return []byte("[]")
	}
	return b
}

func returnAsError(ret []reflect.Value) error {
	if len(ret) != 1 {
		return fmt.Errorf("jobs: expected one return, got %d", len(ret))
	}
	v := ret[0]
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Interface && v.IsNil() {
		return nil
	}
	err, _ := v.Interface().(error)
	return err
}

func decodeArg(j adapter.JSON, t reflect.Type, raw []byte) (reflect.Value, error) {
	s := strings.TrimSpace(string(raw))
	if t.Kind() == reflect.Ptr {
		if s == "null" {
			return reflect.Zero(t), nil
		}
		elem := t.Elem()
		v := reflect.New(elem)
		if err := j.Unmarshal(raw, v.Interface()); err != nil {
			return reflect.Value{}, err
		}
		return v, nil
	}
	pv := reflect.New(t)
	if err := j.Unmarshal(raw, pv.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return pv.Elem(), nil
}

func allocForUnmarshal(t reflect.Type) reflect.Value {
	if t.Kind() == reflect.Ptr {
		return reflect.New(t.Elem()) // e.g. *T points to T
	}
	return reflect.New(t) // e.g. *T holds T value
}

func validateJSONParamType(j adapter.JSON, t reflect.Type) error {
	if t == nil {
		return fmt.Errorf("nil type")
	}
	if t.Kind() == reflect.Interface {
		return fmt.Errorf("use a concrete parameter type, not a bare interface")
	}
	tt := t
	if tt.Kind() == reflect.Map && tt.Key().Kind() != reflect.String {
		return fmt.Errorf("map key must be string for JSON interop")
	}
	switch tt.Kind() {
	case reflect.Chan, reflect.Func, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
		return fmt.Errorf("kind %s not supported for JSON", tt.Kind())
	}
	v := allocForUnmarshal(t)
	// A few top-level JSON tokens cover common Go parameter shapes.
	candidates := []string{`null`, `0`, `""`, `[]`, `true`, `false`, `{}`}
	for _, s := range candidates {
		if uerr := j.Unmarshal([]byte(s), v.Interface()); uerr == nil {
			return nil
		}
	}
	// also try a minimal object for named structs if needed
	if t.Kind() == reflect.Struct {
		if uerr := j.Unmarshal([]byte("{}"), v.Interface()); uerr == nil {
			return nil
		}
	}
	return fmt.Errorf("no JSON sample decoded into %s", t)
}

//go:generate packr

package execution

import (
	"bytes"
	"github.com/gobuffalo/packr"
	"github.com/jensneuse/graphql-go-tools/pkg/ast"
	"github.com/jensneuse/graphql-go-tools/pkg/astvisitor"
	"github.com/jensneuse/graphql-go-tools/pkg/lexer/literal"
	wasm "github.com/wasmerio/go-ext-wasm/wasmer"
	"go.uber.org/zap"
	"io"
	"sync"
)

func NewQuickJSDataSourcePlanner(baseDataSourcePlanner BaseDataSourcePlanner) *QuickJSDataSourcePlanner {
	return &QuickJSDataSourcePlanner{
		BaseDataSourcePlanner: baseDataSourcePlanner,
	}
}

type QuickJSDataSourcePlanner struct {
	BaseDataSourcePlanner
}

func (w *QuickJSDataSourcePlanner) DirectiveDefinition() []byte {
	data, _ := w.graphqlDefinitions.Find("directives/wasm_datasource.graphql")
	return data
}

func (w *QuickJSDataSourcePlanner) DirectiveName() []byte {
	return []byte("QuickJSDataSource")
}

func (w *QuickJSDataSourcePlanner) Initialize(walker *astvisitor.Walker, operation, definition *ast.Document, args []Argument, resolverParameters []ResolverParameter) {
	w.walker, w.operation, w.definition, w.args = walker, operation, definition, args
}

func (w *QuickJSDataSourcePlanner) EnterInlineFragment(ref int) {

}

func (w *QuickJSDataSourcePlanner) LeaveInlineFragment(ref int) {

}

func (w *QuickJSDataSourcePlanner) EnterSelectionSet(ref int) {

}

func (w *QuickJSDataSourcePlanner) LeaveSelectionSet(ref int) {

}

func (w *QuickJSDataSourcePlanner) EnterField(ref int) {
	fieldDefinition, ok := w.walker.FieldDefinition(ref)
	if !ok {
		return
	}
	directive, ok := w.definition.FieldDefinitionDirectiveByName(fieldDefinition, w.DirectiveName())
	if !ok {
		return
	}

	value, ok := w.definition.DirectiveArgumentValueByName(directive, literal.CODE)
	if !ok {
		return
	}
	staticValue := w.definition.StringValueContentBytes(value.Ref)
	staticValue = bytes.ReplaceAll(staticValue, literal.BACKSLASH, nil)
	w.args = append(w.args, &StaticVariableArgument{
		Name:  literal.CODE,
		Value: staticValue,
	})

	// args
	if w.operation.FieldHasArguments(ref) {
		args := w.operation.FieldArguments(ref)
		for _, i := range args {
			argName := w.operation.ArgumentNameBytes(i)
			value := w.operation.ArgumentValue(i)
			if value.Kind != ast.ValueKindVariable {
				continue
			}
			variableName := w.operation.VariableValueNameBytes(value.Ref)
			name := append([]byte(".arguments."), argName...)
			arg := &ContextVariableArgument{
				VariableName: variableName,
				Name:         make([]byte, len(name)),
			}
			copy(arg.Name, name)
			w.args = append(w.args, arg)
		}
	}
}

func (w *QuickJSDataSourcePlanner) LeaveField(ref int) {

}

func (w *QuickJSDataSourcePlanner) Plan() (DataSource, []Argument) {
	return &QuickJSDataSource{
		log:w.log,
	}, w.args
}

type QuickJSDataSource struct {
	log      *zap.Logger
	instance wasm.Instance
	once     sync.Once
}

func (q *QuickJSDataSource) Resolve(ctx Context, args ResolvedArgs, out io.Writer) Instruction {

	input := args.ByKey(literal.CODE)

	q.log.Debug("QuickJSDataSource.Resolve.args",
		zap.ByteString("input",input),
	)

	q.once.Do(func() {

		box := packr.NewBox("./wasm")
		wasmBytes,_ := box.Find("quickjs.wasm")
		var err error
		q.instance, err = wasm.NewInstance(wasmBytes)
		if err != nil {
			q.log.Error("QuickJSDataSource.wasm.NewInstance(wasmBytes)",
				zap.Error(err),
			)
		}

		q.log.Debug("QuickJSDataSource.wasm.NewInstance OK")
	})

	inputLen := len(input)

	allocateInputResult, err := q.instance.Exports["allocate"](inputLen)
	if err != nil {
		q.log.Error("QuickJSDataSource.instance.Exports[\"allocate\"](inputLen)",
			zap.Error(err),
		)
		return CloseConnectionIfNotStream
	}

	inputPointer := allocateInputResult.ToI32()

	memory := q.instance.Memory.Data()[inputPointer:]

	for i := 0; i < inputLen; i++ {
		memory[i] = input[i]
	}

	memory[inputLen] = 0

	result, err := q.instance.Exports["invoke"](inputPointer)
	if err != nil {
		q.log.Error("QuickJSDataSource.instance.Exports[\"invoke\"](inputPointer)",
			zap.Error(err),
		)
		return CloseConnectionIfNotStream
	}

	start := result.ToI32()
	memory = q.instance.Memory.Data()

	var stop int32

	for i := start; i < int32(len(memory)); i++ {
		if memory[i] == 0 {
			stop = i
			break
		}
	}

	_,err = out.Write(memory[start:stop])
	if err != nil {
		q.log.Error("QuickJSDataSource.out.Write(memory[start:stop])",
			zap.Error(err),
		)
		return CloseConnectionIfNotStream
	}

	deallocate := q.instance.Exports["deallocate"]
	_, err = deallocate(inputPointer, inputLen)
	if err != nil {
		q.log.Error("QuickJSDataSource.deallocate(inputPointer, inputLen)",
			zap.Error(err),
		)
		return CloseConnectionIfNotStream
	}

	_, err = deallocate(start, stop-start)
	if err != nil {
		q.log.Error("QuickJSDataSource.deallocate(start, stop-start)",
			zap.Error(err),
		)
		return CloseConnectionIfNotStream
	}

	return CloseConnectionIfNotStream
}

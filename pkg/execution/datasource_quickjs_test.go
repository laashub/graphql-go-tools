package execution

import (
	"bytes"
	"fmt"
	"github.com/jensneuse/graphql-go-tools/pkg/lexer/literal"
	"go.uber.org/zap"
	"testing"
)

func TestQuickJSDataSource_Resolve(t *testing.T) {

	jsCode := "1 + 2"

	planner := NewQuickJSDataSourcePlanner(BaseDataSourcePlanner{
		log:zap.NewNop(),
	})

	dataSource, _ := planner.Plan()
	wasmDataSource := dataSource.(*QuickJSDataSource)

	args := ResolvedArgs{
		ResolvedArgument{
			Key:   literal.CODE,
			Value: []byte(jsCode),
		},
	}

	out := bytes.Buffer{}

	wasmDataSource.Resolve(Context{},args,&out)

	fmt.Println(out.String())
}

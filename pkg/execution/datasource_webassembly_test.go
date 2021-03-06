package execution

import (
	"bytes"
	"encoding/json"
	log "github.com/jensneuse/abstractlogger"
	"github.com/jensneuse/graphql-go-tools/pkg/execution/datasource"
	"testing"
)

type Person struct {
	Id string `json:"id"`
	Name string `json:"name"`
	Age int `json:"age"`
}

func TestWASMDataSource_Resolve(t *testing.T) {

	person := Person{
		Id: "1",
	}

	input := []byte("{\"id\":\"1\"}")

	wasmDataSource := &datasource.WasmDataSource{
		Log: log.NoopLogger,
	}

	args := ResolvedArgs{
		ResolvedArgument{
			Key: []byte("input"),
			Value:input,
		},
		ResolvedArgument{
			Key: []byte("wasmFile"),
			Value: []byte("./testdata/memory.wasm"),
		},
	}

	out := bytes.Buffer{}

	_,err := wasmDataSource.Resolve(Context{},args,&out)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(out.Bytes(),&person)
	if err != nil {
		t.Fatal(err)
	}

	if person.Id != "1" {
		t.Fatalf("want 1, got: %s\n",person.Id)
	}
	if person.Name != "Jens" {
		t.Fatalf("want Jens, got: %s\n",person.Name)
	}
	if person.Age != 31 {
		t.Fatalf("want 31, got: %d",person.Age)
	}
}

func BenchmarkWASMDataSource_Resolve(t *testing.B) {

	input := []byte("{\"id\":\"1\"}")

	wasmDataSource := &datasource.WasmDataSource{
		Log: log.NoopLogger,
	}

	args := ResolvedArgs{
		ResolvedArgument{
			Key: []byte("input"),
			Value:input,
		},
		ResolvedArgument{
			Key: []byte("wasmFile"),
			Value: []byte("./testdata/memory.wasm"),
		},
	}

	out := bytes.Buffer{}

	t.ResetTimer()
	t.ReportAllocs()

	for i := 0;i<t.N;i++ {
		out.Reset()
		_,err := wasmDataSource.Resolve(Context{}, args, &out)
		if err != nil {
			t.Fatal(err)
		}
		if out.Len() == 0 {
			t.Fatalf("must not be 0")
		}
	}
}